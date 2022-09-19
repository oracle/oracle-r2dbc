/*
  Copyright (c) 2020, 2021, Oracle and/or its affiliates.

  This software is dual-licensed to you under the Universal Permissive License
  (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License
  2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
  either license.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package oracle.r2dbc.impl;

import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Readable;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import oracle.r2dbc.impl.ReadablesMetadata.RowMetadataImpl;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLWarning;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static oracle.r2dbc.impl.OracleR2dbcExceptions.fromJdbc;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.toR2dbcException;
import static oracle.r2dbc.impl.OracleReadableImpl.createRow;
import static oracle.r2dbc.impl.ReadablesMetadata.createRowMetadata;

/**
 * <p>
 * Abstract class providing a base implementation of the R2DBC SPI
 * {@link Result} interface. Concrete subclasses implement
 * {@link #publishSegments(Function)} to return a {@link Publisher} that emits
 * the output of a {@link Segment} mapping function for each {@code Segment} of
 * the {@code Result}. Implementations of R2DBC SPI methods in the base
 * class invoke {@code publishSegments} with a mapping function that
 * filters the emitted {@code Segment}s according to the specification of the
 * SPI method.
 * </p>
 */
abstract class OracleResultImpl implements Result {

  /**
   * Object output by mapping functions provided to
   * {@link #publishSegments(Function)} for {@code Segment}s that do not
   * satisfy a filter. Downstream operators of
   * {@link #publishSegments(Function)} filter this object so that it is not
   * emitted to user code.
   */
  private static final Object FILTERED = new Object();

  /**
   * This field is set to {@code true} if this result depends on a JDBC
   * statement to remain open until this result is consumed. For instance,
   * if this result retains an JDBC {@code ResultSet}, then the statement which
   * created that {@code ResultSet} must remain open until this result is
   * consumed; Closing the JDBC statement would close any {@code ResultSet}
   * it had created. As a counter example, if this result only retains an
   * update count, then the JDBC statement does not need to remain open. The
   * update count is simply stored as a {@code long} value, and closing the
   * JDBC statement has no effect on that.
   */
  private final boolean isDependent;

  /**
   * A collection of results that depend on a JDBC statement to remain open
   * until they are consumed. This field is initialized to a placeholder value
   * that does nothing, and this placeholder is retained if this result does not
   * depend on a JDBC statement. Otherwise, if this result does depend on a
   * JDBC statement, then this field is set to the {@code DependentResults}
   * object passed to {@link #addDependent(DependentResults)}
   * {@linkplain #isDependent has such a dependency}, then it adds itself to
   * the
   */
  private DependentResults dependentResults =
    new DependentResults(Mono.empty());

  /**
   * Indicates if a method call on this {@code Result} has already returned a
   * {@code Publisher} that allows this {@code Result} to be consumed. In
   * conformance with the R2DBC SPI, multiple attempts to consume the this
   * result will yield an {@code IllegalStateException}.
   */
  private boolean isPublished = false;


  /** Private constructor invoked by inner subclasses */
  private OracleResultImpl(boolean isDependent) {
    this.isDependent = isDependent;
  }

  /**
   * Publishes the output of a {@code mappingFunction} for each {@code Segment}
   * of this {@code Result}.
   * @param mappingFunction {@code Segment} mapping function. Not null.
   * @param <T> Output type of the {@code mappingFunction}
   * @return {@code Publisher} of values output by the {@code mappingFunction}
   */
  abstract <T> Publisher<T> publishSegments(
    Function<Segment, T> mappingFunction);

  /**
   * <p>
   * Publishes the output of a {@code mappingFunction} for each {@code Segment}
   * of this {@code Result}, where the {@code Segment} is an instance of the
   * specified {@code type}.
   * </p><p>
   * This method sets {@link #isPublished} to prevent multiple consumptions
   * of this {@code Result}. In case this is a {@link FilteredResult}, this
   * method must invoke {@link #publishSegments(Function)}, before returning,
   * in order to update {@code isPublished} of the {@link FilteredResult#result}
   * as well.
   * </p><p>
   * When the returned publisher terminates with {@code onComplete},
   * {@code onError}, or {@code cancel}, the {@link #onConsumed} publisher is
   * subscribed to. The {@code onConsumed} reference is updated to {@code null}
   * so that post-consumption calls to {@link #onConsumed(Publisher)} can detect
   * that this result is already consumed.
   * </p><p>
   * This method uses a doOnNext operator to catch any Results output by a user
   * defined mapping function (this can happen with REF CURSOR out parameters).
   * Output results may be added to the collection of {@link #dependentResults}.
   *
   * </p><p>
   * The returned {@code Publisher} emits {@code onError} with an
   * {@link R2dbcException} if this {@code Result} has a {@link Message} segment
   * and the {@code type} is not a super-type of {@code Message}. This
   * corresponds to the specified behavior of R2DBC SPI methods
   * {@link #map(BiFunction)}, {@link #map(BiFunction)}, and
   * {@link #getRowsUpdated()}
   * </p>
   * @param segmentType {@code Segment} type to be mapped. Not null.
   * @param segmentMapper {@code Segment} mapping function. Not null.
   * @param <T> {@code Segment} type to be mapped
   * @param <U> Output type of the {@code mappingFunction}
   * @return {@code Publisher} of mapped {@code Segment}s
   */
  @SuppressWarnings("unchecked")
  private <T extends Segment, U> Publisher<U> publishSegments(
    Class<T> segmentType, Function<? super T, U> segmentMapper) {
    return Flux.concatDelayError(
      Flux.from(mapSegments(segmentType, segmentMapper))
        .doOnNext(next -> {
          if (next instanceof OracleResultImpl)
            ((OracleResultImpl)next).addDependent(dependentResults);
        }),
      (Publisher<U>)removeDependent())
      .doOnCancel(() -> Mono.from(removeDependent()).subscribe());
  }

  /**
   * <p>
   * Returns a publisher that emits the output of a segment mapping function for
   * each segment of this result. The mapping function accepts segments of a
   * specified type. This method is called from the public API to create
   * publishers of different value types, such as a {@code Publisher<Long>}
   * for {@link #getRowsUpdated()}, or a publisher of mapped rows for
   * {@link #map(BiFunction)}.
   * </p><p>
   * This result may not have any segments of the specified type. In this case,
   * the returned publisher emits an error for any {@code Message} segments, are
   * </p>
   * If this result has no segments of the specified type, the
   * publisher emits nothing to onNext. If
   * if this result has no segment of the given type. If the segment type is not
   * {@link Message}, and this publisher has a message segment, then publisher emits an
   * error if this result has a message segment and the type is this result has no
   * message segments
   * @param segmentType
   * @param segmentMapper
   * @return
   * @param <T>
   * @param <U>
   */
  protected abstract <T extends Segment, U> Publisher<U> mapSegments(
    Class<T> segmentType, Function<? super T, U> segmentMapper);

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method to return a {@code Publisher} emitting the
   * flat-mapped output of {@code Publisher}s output by a
   * {@code mappingFunction} for all {@code Segments} this {@code Result}.
   * {@code Publisher}s output by the {@code mappingFunction} are subscribed to
   * serially with the completion of the {@code Publisher} output for any
   * previous {@code Segment}.
   * </p><p>
   * The returned {@code Publisher} does not support multiple
   * {@code Subscriber}s
   * </p>
   */
  @Override
  public <T> Publisher<T> flatMap(
    Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
    requireNonNull(mappingFunction, "mappingFunction is null");
    setPublished();
    return singleSubscriber(Flux.concat(
      publishSegments(Segment.class, mappingFunction)));
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method to return a {@code Publisher} emitting the
   * update counts of this {@code Result} as {@link Integer} values. An
   * {@code onError} signal with {@link ArithmeticException} is emitted if a
   * update count of this {@code Result} is larger than
   * {@link Integer#MAX_VALUE}.
   * </p><p>
   * The returned {@code Publisher} supports multiple {@code Subscriber}s.
   * </p>
   */
  @Override
  public Publisher<Long> getRowsUpdated() {
    setPublished();
    return publishSegments(UpdateCount.class, UpdateCount::value);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method to return a {@code Publisher} emitting the
   * output of a {@code mappingFunction} for each {@link Row} of this
   * {@code Result}.
   * </p><p>
   * The returned {@code Publisher} does not support multiple
   * {@code Subscriber}s.
   * </p>
   */
  @Override
  public <T> Publisher<T> map(
    BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
    requireNonNull(mappingFunction, "mappingFunction is null");
    setPublished();
    return singleSubscriber(publishSegments(RowSegment.class,
      rowSegment -> {
        Row row = rowSegment.row();
        return mappingFunction.apply(row, row.getMetadata());
      }));
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method to return a {@code Publisher} emitting the
   * output of a {@code mappingFunction} for each {@link Row} of this
   * {@code Result}.
   * </p><p>
   * The returned {@code Publisher} does not support multiple
   * {@code Subscriber}s.
   * </p>
   */
  @Override
  public <T> Publisher<T> map(
    Function<? super Readable, ? extends T> mappingFunction) {
    requireNonNull(mappingFunction, "mappingFunction is null");
    setPublished();
    return singleSubscriber(publishSegments(ReadableSegment.class,
      readableSegment ->
        mappingFunction.apply(readableSegment.getReadable())));
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method to return a new instance of
   * {@code OracleResultImpl} that implements
   * {@link OracleResultImpl#publishSegments(Function)} to call
   * {@link OracleResultImpl#publishSegments(Class, Function)} on this instance
   * of {@code OracleResultImpl}. The invocation of {@code publishSegments}
   * on this instance ensures that its consumption state is updated correctly.
   * The invocation of {@code publishSegments} is provided with a mapping
   * function that outputs the {@link #FILTERED} object for {@code Segment}s
   * rejected by the {@code filter}.
   * </p>
   */
  @Override
  public OracleResultImpl filter(Predicate<Segment> filter) {
    requireNonNull(filter, "filter is null");
    return new FilteredResult(this, filter);
  }

  /**
   * <p>
   * Adds this result to a collection of results that depend on the JDBC
   * statement they came from to remain open. Depending on the type of this
   * result, this method may or may not add a result to the collection. For
   * instance, an update count result would not be added because it does not
   * depend on the JDBC statement to remain open. Conversely, a result backed by
   * a JDBC ResultSet would be added, as the ResultSet would otherwise be closed
   * when JDBC statement is closed.
   * </p><p>
   * Additional results may be added to the collection after this method
   * returns. In particular, a REF CURSOR is backed by a ResultSet, and that
   * ResultSet will be closed when the JDBC statement is closed. At the time
   * when this method is called, it is not known what the user defined mapping
   * function will do. A check to see if REF CURSOR ResultSet was created can
   * only after the user defined function has executed.
   * </p><p>
   * This method is implemented by the OracleResultImpl super class to do
   * nothing. Subclasses that depend on the JDBC statement override this method
   * and add themselves to the collection of dependent results.
   * </p>
   * @param onConsumed Publisher to subscribe to when consumed. Not null.
   * @return true if this result has not already been consumed, and the
   * publisher will be subscribed to. Returns false if the publisher will not
   * be subscribed to because this result is already consumed.
   */
  final void addDependent(DependentResults dependentResults) {
    if (isDependent)
      dependentResults.increment();

    this.dependentResults = dependentResults;
  }

  private Publisher<Void> removeDependent() {
    return isDependent
      ? dependentResults.decrement()
      : Mono.empty();
  }

  /**
   * Marks this result as having created a {@code Publisher} that allows this
   * {@code Result} to be consumed. This method enforces the {@link Result} SPI
   * contract which does not allow the same result to be consumed more than
   * once.
   * <em>
   *   This method MUST be called before returning a Publisher to user code from
   *   all public APIs of this class.
   * </em>
   * @throws IllegalStateException If this result has already been consumed.
   */
  protected void setPublished() {
    if (! isPublished)
      isPublished = true;
    else
      throw multipleConsumptionException();
  }

  /**
   * Returns an {@code IllegalStateException} to be thrown when user code
   * attempts to consume a {@code Result} more than once with invocations of
   * {@link #map(BiFunction)}, {@link #map(Function)},
   * {@link #flatMap(Function)}, or {@link #getRowsUpdated()}.
   * @return {@code IllegalStateException} indicating multiple consumptions
   */
  private static IllegalStateException multipleConsumptionException() {
    return new IllegalStateException(
      "A result can not be consumed more than once");
  }

  /**
   * Creates a {@code Result} that publishes a JDBC {@code resultSet} as
   * {@link RowSegment}s
   * @param resultSet {@code ResultSet} to publish. Not null.
   * @param adapter Adapts JDBC calls into reactive streams. Not null.
   * @return A {@code Result} for a ResultSet
   */
  public static OracleResultImpl createQueryResult(
    ResultSet resultSet, ReactiveJdbcAdapter adapter) {
    return new ResultSetResult(resultSet, adapter);
  }

  /**
   * Creates a {@code Result} that publishes {@code outParameters} as
   * {@link OutSegment}s
   * @param outParameters {@code OutParameters} to publish. Not null.
   * @param adapter Adapts JDBC calls into reactive streams. Not null.
   * @return A {@code Result} for {@code OutParameters}
   */
  static OracleResultImpl createCallResult(
    OutParameters outParameters, ReactiveJdbcAdapter adapter) {
    return new CallResult(outParameters, adapter);
  }

  /**
   * Creates a {@code Result} that publishes an {@code updateCount} as an
   * {@link UpdateCount} segment, followed by a {@code generatedKeys}
   * {@code ResultSet} as {@link RowSegment}s
   * @return A {@code Result} for values generated by DML
   * @param updateCount Update count to publish
   * @param generatedKeys Generated values to publish. Not null.
   * @param adapter Adapts JDBC calls into reactive streams. Not null.
   */
  static OracleResultImpl createGeneratedValuesResult(
    long updateCount, ResultSet generatedKeys, ReactiveJdbcAdapter adapter) {
    return new GeneratedKeysResult(updateCount, generatedKeys, adapter);
  }

  /**
   * Creates a {@code Result} that publishes an {@code updateCount} as an
   * {@link UpdateCount} segment
   * @return A {@code Result} for a DML update
   * @param updateCount Update count to publish
   */
  static OracleResultImpl createUpdateCountResult(long updateCount) {
    return new UpdateCountResult(updateCount);
  }

  /**
   * Creates a {@code Result} that publishes a batch of {@code updateCounts}
   * as {@link UpdateCount} segments
   * @return A {@code Result} for a batch DML update
   * @param updateCounts Update counts to publish. Not null.
   */
  static OracleResultImpl createBatchUpdateResult(long[] updateCounts) {
    return new BatchUpdateResult(updateCounts);
  }

  /**
   * Creates a {@code Result} that publishes update counts of a
   * {@code batchUpdateException} as {@link UpdateCount} segments, followed a
   * {@link Message} segment with the {@code batchUpdateException} mapped to
   * an {@link R2dbcException}
   * @param batchUpdateException BatchUpdateException to publish. Not null.
   * @return A {@code Result} for a failed DML batch update
   */
  static OracleResultImpl createBatchUpdateErrorResult(
    BatchUpdateException batchUpdateException) {
    return new BatchUpdateErrorResult(batchUpdateException);
  }

  /**
   * Creates a {@code Result} that publishes an {@code r2dbcException} as a
   * {@link Message} segment
   * @param r2dbcException Error to publish. Not null.
   * @return A {@code Result} for failed {@code Statement} execution
   */
  static OracleResultImpl createErrorResult(R2dbcException r2dbcException) {
    return new ErrorResult(r2dbcException);
  }

  /**
   * Creates a {@code Result} that publishes a {@code warning} as a
   * {@link Message} segment, followed by any {@code Segment}s of a
   * {@code result}.
   * @param warning Warning to publish. Not null.
   * @param result Result to publisher. Not null.
   * @return A {@code Result} for a {@code Statement} execution that
   * completed with a warning.
   */
  static OracleResultImpl createWarningResult(
    SQLWarning warning, OracleResultImpl result) {
    return new WarningResult(warning, result);
  }

  /**
   * {@link OracleResultImpl} subclass that publishes a single update count. An
   * instance of this class constructed with negative valued update count
   * will publish no {@code Segment}s
   */
  private static final class UpdateCountResult extends OracleResultImpl {

    private final long updateCount;

    private UpdateCountResult(long updateCount) {
      super(false);
      this.updateCount = updateCount;
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      return updateCount >= 0
        ? Mono.just(new UpdateCountImpl(updateCount))
            .map(mappingFunction)
        : Mono.empty();
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method uses Mono's fromSupplier factory to defer segment mapping
     * until the publisher is subscribed to. This ensures that segments are
     * consumed in the correct order when the returned publisher is concatenated
     * after another, as with
     * {@link BatchUpdateErrorResult#mapSegments(Class, Function)}, for
     * instance. Additionally, the factory handles any exception thrown by the
     * segment mapper by translating it in to an onError signal.
     * </p>
     */
    @Override
    protected <T extends Segment, U> Publisher<U> mapSegments(
      Class<T> segmentType, Function<? super T, U> segmentMapper) {

      if (!segmentType.isAssignableFrom(UpdateCountImpl.class))
        return Mono.empty();

      return Mono.fromSupplier(() ->
        segmentMapper.apply(segmentType.cast(
          new UpdateCountImpl(updateCount))));
    }
  }

  /**
   * <p>
   * {@link OracleResultImpl} subclass that publishes JDBC {@link ResultSet} as
   * {@link RowSegment}s. {@link RowMetadata} of published {@code Rows} is
   * derived from the {@link java.sql.ResultSetMetaData} of the
   * {@link ResultSet}.
   * </p><p>
   * This {@code Result} is <i>not</i> implemented to publish
   * {@link SQLWarning} chains returned by {@link ResultSet#getWarnings()} as
   * {@link Message} segments. This implementation is correct for the 21.1
   * Oracle JDBC Driver which is known to implement {@code getWarnings()} by
   * returning {@code null} for forward-only insensitive {@code ResultSets}
   * when no invocation of {@link java.sql.Statement#setMaxRows(int)}
   * or {@link java.sql.Statement#setLargeMaxRows(long)} has occurred.
   * </p><p>
   * It is a known limitation of the 21.1 Oracle JDBC Driver that
   * {@link ResultSet#getWarnings()} can not be invoked after row publishing
   * has been initiated; The {@code ResultSet} is logically closed once row
   * publishing has been initiated, and so {@code getWarnings} would throw a
   * {@link java.sql.SQLException} to indicate a closed {@code ResultSet}. If
   * a later release of Oracle JDBC removes this limitation, then this
   * {@code Result} should be implemented to invoke {@code getWarnings} to
   * ensure correctness if a later release of Oracle JDBC also returns non-null
   * values from that method.
   * </p>
   */
  private static final class ResultSetResult extends OracleResultImpl {

    private final ResultSet resultSet;
    private final RowMetadataImpl metadata;
    private final ReactiveJdbcAdapter adapter;

    private ResultSetResult(
      ResultSet resultSet, ReactiveJdbcAdapter adapter) {
      super(true);
      this.resultSet = resultSet;
      this.metadata = createRowMetadata(fromJdbc(resultSet::getMetaData));
      this.adapter = adapter;
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {

      // Avoiding object allocating by reusing the same Row object
      ReusableJdbcReadable reusableJdbcReadable = new ReusableJdbcReadable();
      Row row = createRow(reusableJdbcReadable, metadata, adapter);

      return adapter.publishRows(resultSet, jdbcReadable -> {
        reusableJdbcReadable.current = jdbcReadable;
        return mappingFunction.apply(new RowSegmentImpl(row));
      });
    }

    @Override
    protected <T extends Segment, U> Publisher<U> mapSegments(
      Class<T> segmentType, Function<? super T, U> segmentMapper) {

      if (!segmentType.isAssignableFrom(RowSegmentImpl.class))
        return Mono.empty();

      // Avoiding object allocation by reusing the same Row object
      ReusableJdbcReadable reusableJdbcReadable = new ReusableJdbcReadable();
      Row row = createRow(reusableJdbcReadable, metadata, adapter);

      return adapter.publishRows(resultSet, jdbcReadable -> {
        reusableJdbcReadable.current = jdbcReadable;
        return segmentMapper.apply(segmentType.cast(new RowSegmentImpl(row)));
      });
    }

    /**
     * Wraps an actual
     * {@link oracle.r2dbc.impl.ReactiveJdbcAdapter.JdbcReadable}. The actual
     * readable is set to {@link #current}. A single instance of
     * {@code OracleReadableImpl.RowImpl} can retain an instance of this class,
     * and the instance can read multiple rows by changing the value of
     * {@link #current} between invocations of a user defined row mapping
     * function. This is done to avoid allocating an object for each row of a
     * query result.
     */
    private static final class ReusableJdbcReadable
      implements ReactiveJdbcAdapter.JdbcReadable {

      ReactiveJdbcAdapter.JdbcReadable current = null;

      @Override
      public <T> T getObject(int index, Class<T> type) {
        return current.getObject(index, type);
      }
    }
  }

  /**
   * {@link OracleResultImpl} subclass that publishes an update count as an
   * {@link UpdateCount} segment, followed by a JDBC {@link ResultSet} as
   * {@link RowSegment}s. This class is a composite of a
   * {@link UpdateCountResult} and {@link ResultSetResult}.
   */
  private static final class GeneratedKeysResult extends OracleResultImpl {

    private final OracleResultImpl updateCountResult;
    private final OracleResultImpl generatedKeysResult;

    private GeneratedKeysResult(
      long updateCount, ResultSet generatedKeys, ReactiveJdbcAdapter adapter) {
      super(true);
      updateCountResult = createUpdateCountResult(updateCount);
      generatedKeysResult = createQueryResult(generatedKeys, adapter);
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      return Flux.concat(updateCountResult.publishSegments(mappingFunction))
        .concatWith(generatedKeysResult.publishSegments(mappingFunction));
    }

    @Override
    protected <T extends Segment, U> Publisher<U> mapSegments(
      Class<T> segmentType, Function<? super T, U> segmentMapper) {
      return Flux.concat(
        updateCountResult.mapSegments(segmentType, segmentMapper),
        generatedKeysResult.mapSegments(segmentType, segmentMapper));
    }
  }

  /**
   * {@link OracleResultImpl} subclass that publishes a single instance of
   * {@link OutParameters} as an {@link OutSegment}.
   */
  private static final class CallResult extends OracleResultImpl {

    private final OutParameters outParameters;
    private final ReactiveJdbcAdapter adapter;

    private CallResult(
      OutParameters outParameters, ReactiveJdbcAdapter adapter) {
      super(true);
      this.outParameters = outParameters;
      this.adapter = adapter;
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      // Acquire the JDBC lock asynchronously as the outParameters are backed
      // by a JDBC CallableStatement, and it may block a thread when values
      // are accessed with CallableStatement.getObject(...)
      return adapter.getLock().get(() ->
        mappingFunction.apply(new OutSegmentImpl(outParameters)));
    }

    @Override
    protected <T extends Segment, U> Publisher<U> mapSegments(
      Class<T> segmentType, Function<? super T, U> segmentMapper) {

      if (!segmentType.isAssignableFrom(OutSegmentImpl.class))
        return Mono.empty();

      // Acquire the JDBC lock asynchronously as the outParameters are backed
      // by a JDBC CallableStatement, and it may block a thread when values
      // are accessed with CallableStatement.getObject(...)
      return adapter.getLock().get(() ->
        segmentMapper.apply(segmentType.cast(
          new OutSegmentImpl(outParameters))));
    }
  }

  /**
   * {@link OracleResultImpl} subclass that publishes an array of update
   * counts as {@link UpdateCount} segments.
   */
  private static final class BatchUpdateResult extends OracleResultImpl {

    private final long[] updateCounts;

    private BatchUpdateResult(long[] updateCounts) {
      super(false);
      this.updateCounts = updateCounts;
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      return Flux.fromStream(LongStream.of(updateCounts)
        .mapToObj(UpdateCountImpl::new))
        .map(mappingFunction);
    }

    @Override
    protected <T extends Segment, U> Publisher<U> mapSegments(
      Class<T> segmentType, Function<? super T, U> segmentMapper) {

      if (!segmentType.isAssignableFrom(UpdateCountImpl.class))
        return Mono.empty();

      return Flux.fromStream(
        LongStream.of(updateCounts)
          .mapToObj(updateCount ->
            segmentMapper.apply(segmentType.cast(
              new UpdateCountImpl(updateCount)))));
    }
  }

  /**
   * {@link OracleResultImpl} subclass that publishes an array of update
   * counts from a {@link BatchUpdateException} as {@link UpdateCount} segments
   * followed by a {@link Message} segment with the {@link BatchUpdateException}
   * mapped to an {@link R2dbcException}. This class is a composite of
   * {@link BatchUpdateResult} and {@link ErrorResult}.
   */
  private static final class BatchUpdateErrorResult extends OracleResultImpl {

    private final BatchUpdateResult batchUpdateResult;
    private final ErrorResult errorResult;

    private BatchUpdateErrorResult(
      BatchUpdateException batchUpdateException) {
      super(false);
      batchUpdateResult = new BatchUpdateResult(
        batchUpdateException.getLargeUpdateCounts());
      errorResult =
        new ErrorResult(toR2dbcException(batchUpdateException));
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      return Flux.concat(
        batchUpdateResult.publishSegments(mappingFunction),
        errorResult.publishSegments(mappingFunction));
    }

    @Override
    protected <T extends Segment, U> Publisher<U> mapSegments(
      Class<T> segmentType, Function<? super T, U> segmentMapper) {
      return Flux.concat(
        batchUpdateResult.mapSegments(segmentType, segmentMapper),
        errorResult.mapSegments(segmentType, segmentMapper));
    }

  }

  /**
   * {@link OracleResultImpl} subclass that publishes an {@link R2dbcException}
   * as a {@link Message} segment.
   */
  private static final class ErrorResult extends OracleResultImpl {

    private final R2dbcException r2dbcException;

    private ErrorResult(R2dbcException r2dbcException) {
      super(false);
      this.r2dbcException = r2dbcException;
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      return Mono.just(new MessageImpl(r2dbcException))
        .map(mappingFunction);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Emits the mapping of a message segment, or emits an error if another
     * segment type is specified. Unlike other segment types, message segements
     * represent an error that must be delivered to user code. Even when user
     * code is calling for some other segment type, like rows with
     * {@link #map(BiFunction)}, or update counts with
     * {@link #getRowsUpdated()}, user code does not want these calls to ignore
     * error. If user code really does want to ignore errors, it may call
     * {@link #filter(Predicate)} to ignore message segments, or
     * {@link #flatMap(Function)} to recover from message segments.
     * </p><p>
     * This method uses Mono's fromSupplier factory to defer segment mapping
     * until the publisher is subscribed to. This ensures that segments are
     * consumed in the correct order when the returned publisher is concatenated
     * after another, as with
     * {@link BatchUpdateErrorResult#mapSegments(Class, Function)}, for
     * instance. Additionally, the factory handles any exception thrown by the
     * segment mapper by translating it in to an onError signal.
     * </p>
     */
    @Override
    protected <T extends Segment, U> Publisher<U> mapSegments(
      Class<T> segmentType, Function<? super T, U> segmentMapper) {

      if (! segmentType.isAssignableFrom(MessageImpl.class))
        return Mono.error(r2dbcException);

      return Mono.fromSupplier(() ->
        segmentMapper.apply(segmentType.cast(
          new MessageImpl(r2dbcException))));
    }
  }

  /**
   * {@link OracleResultImpl} subclass that publishes a {@link SQLWarning}
   * chain as {@link Message} segments, followed by the segments of another
   * {@link OracleResultImpl}.
   */
  private static final class WarningResult extends OracleResultImpl {

    /** The warning of this result */
    private final SQLWarning warning;

    /** The result that follows this result */
    private final OracleResultImpl result;

    /**
     * Constructs a result that publishes a {@code warning} as a
     * {@link Message}, and then publishes the segments of a {@code result}.
     * @param warning Warning to publish. Not null.
     * @param result Result of segments to publish after the warning. Not null.
     */
    private WarningResult(
      SQLWarning warning, OracleResultImpl result) {
      super(result.isDependent);
      this.warning = warning;
      this.result = result;
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      return Flux.fromStream(Stream.iterate(
        warning, Objects::nonNull, SQLWarning::getNextWarning)
        .map(OracleR2dbcExceptions::toR2dbcException)
        .map(MessageImpl::new))
        .map(mappingFunction)
        // Invoke publishSegments(Class, Function) rather than
        // publishSegments(Function) to update the state of the result; Namely,
        // the state that has the onConsumed Publisher emit a terminal signal.
        .concatWith(result != null
          ? result.publishSegments(Segment.class,mappingFunction)
          : Mono.empty());
    }

    /**
     * @implNote In the 1.0.0 release, message segments for the warning were
     * emitted prior to any segments from the {@link #result}. Unless message
     * segments were consumed by {@link #flatMap(Function)}, the publisher
     * returned to user code would emit onError before emitting values from
     * the {@link #result}.
     * Revisiting this decision before the 1.1.0 release, it really seems like a
     * bad one. It is thought that user code would typically want to consume
     * results before handling warnings and errors, and so the order is reversed
     * in later releases. Segments are now emitted from the {@link #result}
     * first, followed by the message segments. This change in behavior should
     * be safe, as the R2DBC SPI does not specify any ordering for this case.
     */
    @Override
    protected <T extends Segment, U> Publisher<U> mapSegments(
      Class<T> segmentType, Function<? super T, U> segmentMapper) {
      return Flux.concat(
        result != null
          ? result.mapSegments(segmentType, segmentMapper)
          : Mono.empty(),
        segmentType.isAssignableFrom(MessageImpl.class)
          ? Flux.fromStream(Stream.iterate(
            warning, Objects::nonNull, SQLWarning::getNextWarning)
            .map(sqlWarning ->
              segmentMapper.apply(segmentType.cast(
                new MessageImpl(toR2dbcException(sqlWarning))))))
          : Mono.error(toR2dbcException(warning)));
    }
  }

  /**
   * A result that filters out {@code Segment} of another result. Filtered
   * segments are emitted as the {@link #FILTERED} object.
   */
  private static final class FilteredResult extends OracleResultImpl {

    /** Result of segments to publish after applying the {@link #filter} */
    private final OracleResultImpl result;

    /** Returns {@code false} for segments that should be filtered */
    private final Predicate<Segment> filter;

    /**
     * Constructs a new result that applies a {@code filter} when publishing
     * segments of a {@code result}.
     */
    private FilteredResult(
      OracleResultImpl result, Predicate<Segment> filter) {
      super(result.isDependent);
      this.result = result;
      this.filter = filter;
    }

    @Override
    @SuppressWarnings("unchecked")
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      return result.publishSegments(Segment.class, segment ->
        filter.test(segment)
          ? mappingFunction.apply(segment)
          : (T)FILTERED);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Passes {@code Segment.class} to the {@code mapSegments} method of the
     * filtered {@link #result} to map all segments with the filtering
     * predicate. Mapping functions must return a non-null value, so it will
     * return a dummy object, {@link #FILTERED}, for segments that are filtered
     * by the predicate. A downstream filter operator will then filter out the
     * {@code FILTERED} objects. It is important that {@code Segment.class}
     * be passed to the {@code mapSegments} method of the filtered result, other
     * wise a {@code Message} segment will have its error be emitted with
     * {@code onError}, and bypass the filtering.
     * </p>
     * @param segmentType
     * @param segmentMapper
     * @return
     * @param <T>
     * @param <U>
     */
    @Override
    protected <T extends Segment, U> Publisher<U> mapSegments(
      Class<T> segmentType, Function<? super T, U> segmentMapper) {
      return Flux.from(result.publishSegments(
        Segment.class,
        segment -> {
          if (!filter.test(segment))
            return (U)FILTERED;

          if (segmentType.isAssignableFrom(segment.getClass()))
            return segmentMapper.apply(segmentType.cast(segment));
          else if (segment instanceof Message)
            throw ((Message)segment).exception();
          else
            return (U)FILTERED;
        }))
        .filter(next -> next != FILTERED);
    }

    /**
     * Override to check if the filtered result is already consumed. This
     * override is necessary to have {@code IllegalStateException} thrown
     * *before* a publisher is returned to user code. The check will happen
     * again when {@link #mapSegments(Class, Function)} invokes
     * {@link #publishSegments(Class, Function)} on the filtered result, but
     * this only happens *after* a publisher is returned to user code and
     * subscribed to.
     */
    @Override
    protected void setPublished() {
      result.setPublished();
      super.setPublished();
    }
  }

  /**
   * Common interface for instances of {@link Segment} with a {@link Readable}
   * value. The {@link #map(Function)} filters for this segment type, and uses
   * the common {@link #getReadable()} method to obtain a {@link Readable} from
   * the segment.
   */
  private interface ReadableSegment extends Segment {
    /** Returns the {@link Readable} value of this {@code Segment} */
    Readable getReadable();
  }

  /**
   * Implementation of {@link RowSegment}. An instance of this class
   * implements the {@link ReadableSegment} interface that satisfies the filter
   * of {@link #map(Function)}.
   */
  private static final class RowSegmentImpl
    implements RowSegment, ReadableSegment {

    private final Row row;

    private RowSegmentImpl(Row row) {
      this.row = row;
    }

    @Override
    public Row row() {
      return row;
    }

    @Override
    public Readable getReadable() {
      return row;
    }
  }

  /**
   * Implementation of {@link OutSegment}. An instance of this class
   * implements the {@link ReadableSegment} interface that satisfies the filter
   * of {@link #map(Function)}.
   */
  private static final class OutSegmentImpl
    implements OutSegment, ReadableSegment {

    private final OutParameters outParameters;

    private OutSegmentImpl(OutParameters outParameters) {
      this.outParameters = outParameters;
    }

    @Override
    public OutParameters outParameters() {
      return outParameters;
    }

    @Override
    public Readable getReadable() {
      return outParameters;
    }
  }

  /**
   * Implementation of {@link UpdateCount}.
   */
  private static final class UpdateCountImpl implements UpdateCount {

    private final long value;

    private UpdateCountImpl(long value) {
      this.value = value;
    }

    @Override
    public long value() {
      return value;
    }
  }

  /**
   * Implementation of {@link Message}.
   */
  private static final class MessageImpl implements Message {

    private final R2dbcException exception;

    private MessageImpl(R2dbcException exception) {
      this.exception = exception;
    }

    @Override
    public R2dbcException exception() {
      return exception;
    }

    @Override
    public int errorCode() {
      return exception.getErrorCode();
    }

    @Override
    public String sqlState() {
      return exception.getSqlState();
    }

    @Override
    public String message() {
      return exception.getMessage();
    }
  }

  /**
   * Returns a {@code Publisher} that emits the signals of a {@code publisher}
   * to a single {@link org.reactivestreams.Subscriber}, and rejects additional
   * {@code Subscriber}s by emitting {@code onError} with
   * {@link IllegalStateException}.
   * @param publisher Publisher that emits signals. Not null.
   * @param <T> Value type of {@code onNext} signals
   * @return A {@code Publisher} that allows a single subscriber
   */
  private static <T> Publisher<T> singleSubscriber(Publisher<T> publisher) {
    AtomicBoolean isSubscribed = new AtomicBoolean(false);
    return Flux.defer(() ->
      isSubscribed.compareAndSet(false, true)
        ? publisher
        : Mono.error(new IllegalStateException(
            "Publisher does not support multiple subscribers")));
  }
}
