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
import java.util.concurrent.CompletableFuture;
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
   * Indicates if a method call on this {@code Result} has already returned a
   * {@code Publisher} that allows this {@code Result} to be consumed. In
   * conformance with the R2DBC SPI, multiple attempts to consume the this
   * result will yield an {@code IllegalStateException}.
   */
  private boolean isPublished = false;

  /**
   * Future that is completed when this {@code Result} has been
   * <a href="OracleStatementImpl.html#fully-consumed-result">
   *   fully-consumed
   * </a>.
   */
  private final CompletableFuture<Void> consumedFuture =
    new CompletableFuture<>();

  /** Private constructor invoked by inner subclasses */
  private OracleResultImpl() { }

  /**
   * Publishes the output of a {@code mappingFunction} for each {@code Segment}
   * of this {@code Result}.
   * @param mappingFunction {@code Segment} mapping function.
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
   * This method updates the state of this {@code Result} to prevent multiple
   * consumptions and to complete the {@link #consumedFuture} after the
   * returned {@code Publisher} terminates. For this state to be updated
   * correctly, any {@code Publisher} returned to user code must one that is
   * returned by this method.
   * </p><p>
   * The returned {@code Publisher} emits {@code onError} with an
   * {@link R2dbcException} if this {@code Result} has a {@link Message} segment
   * and the {@code type} is not a super-type of {@code Message}. This
   * corresponds to the specified behavior of R2DBC SPI methods
   * {@link #map(BiFunction)}, {@link #map(BiFunction)}, and
   * {@link #getRowsUpdated()}
   * </p>
   * @param type {@code Segment} type to be mapped
   * @param mappingFunction {@code Segment} mapping function
   * @param <T> {@code Segment} type to be mapped
   * @param <U> Output type of the {@code mappingFunction}
   * @return {@code Publisher} of mapped {@code Segment}s
   */
  @SuppressWarnings("unchecked")
  private <T extends Segment, U> Publisher<U> publishSegments(
    Class<T> type, Function<? super T, U> mappingFunction) {

    setPublished();

    // In the case of Row publishing, the mapping function must be passed down
    // to the JDBC driver, as it will deallocate row data storage once the
    // mapping function returns a value. The mapping function provided to JDBC
    // must be one that invokes the user-defined mapping function before row
    // data is deallocated.
    // Instances of the desired Segment type are input to the user-defined
    // mapping function. If Message Segments are not a desired type, then
    // they are thrown and emitted as onError signals. Any other Segment type
    // is mapped to the FILTERED object, which is then filtered by a downstream
    // operator.
    return Flux.from(publishSegments(segment -> {
        if (type.isInstance(segment))
          return mappingFunction.apply(type.cast(segment));
        else if (segment instanceof Message)
          throw ((Message)segment).exception();
        else
          return (U)FILTERED;
      }))
      .filter(object -> object != FILTERED)
      .doOnTerminate(() -> consumedFuture.complete(null))
      .doOnCancel(() -> consumedFuture.complete(null));
  }

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
  public Publisher<Integer> getRowsUpdated() {
    return publishSegments(UpdateCount.class,
      updateCount -> Math.toIntExact(updateCount.value()));
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
  @SuppressWarnings("unchecked")
  public OracleResultImpl filter(Predicate<Segment> filter) {
    requireNonNull(filter, "filter is null");

    if (isPublished)
      throw multipleConsumptionException();

    return new OracleResultImpl() {
      @Override
      <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
        return OracleResultImpl.this.publishSegments(Segment.class, segment ->
          filter.test(segment)
            ? mappingFunction.apply(segment)
            : (T)FILTERED);
      }
    };
  }

  /**
   * Returns a {@code Publisher} that emits {@code onComplete} when this
   * {@code Result} has been
   * <a href="OracleStatementImpl.html#fully-consumed-result">
   *   fully-consumed
   * </a>.
   * @return {@code Publisher} of this {@code Result}'s consumption
   */
  final Publisher<Void> onConsumed() {
    return Mono.fromCompletionStage(consumedFuture);
  }

  /**
   * Marks this result as having created a {@code Publisher} that allows this
   * {@code Result} to be consumed. This method enforces the {@link Result} SPI
   * contract which does not allow the same result to be consumed more than
   * once.
   * @throws IllegalStateException If this result has already been consumed.
   */
  private void setPublished() {
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
   * @param resultSet {@code ResultSet} to publish
   * @param adapter Adapts JDBC calls into reactive streams.
   * @return A {@code Result} for a ResultSet
   */
  public static OracleResultImpl createQueryResult(
     ResultSet resultSet, ReactiveJdbcAdapter adapter) {
    return new ResultSetResult(resultSet, adapter);
  }

  /**
   * Creates a {@code Result} that publishes {@code outParameters} as
   * {@link OutSegment}s
   * @param outParameters {@code OutParameters} to publish
   * @return A {@code Result} for {@code OutParameters}
   */
  static OracleResultImpl createCallResult(OutParameters outParameters) {
    return new CallResult(outParameters);
  }

  /**
   * Creates a {@code Result} that publishes an {@code updateCount} as an
   * {@link UpdateCount} segment, followed by a {@code generatedKeys}
   * {@code ResultSet} as {@link RowSegment}s
   * @return A {@code Result} for values generated by DML
   * @param updateCount Update count to publish
   * @param generatedKeys Generated values to publish
   * @param adapter Adapts JDBC calls into reactive streams.
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
   * @param updateCounts Update counts to publish
   */
  static OracleResultImpl createBatchUpdateResult(long[] updateCounts) {
    return new BatchUpdateResult(updateCounts);
  }

  /**
   * Creates a {@code Result} that publishes update counts of a
   * {@code batchUpdateException} as {@link UpdateCount} segments, followed a
   * {@link Message} segment with the {@code batchUpdateException} mapped to
   * an {@link R2dbcException}
   * @param batchUpdateException BatchUpdateException to publish
   * @return A {@code Result} for a failed DML batch update
   */
  static OracleResultImpl createBatchUpdateErrorResult(
    BatchUpdateException batchUpdateException) {
    return new BatchUpdateErrorResult(batchUpdateException);
  }

  /**
   * Creates a {@code Result} that publishes an {@code r2dbcException} as a
   * {@link Message} segment
   * @param r2dbcException Error to publish
   * @return A {@code Result} for failed {@code Statement} execution
   */
  static OracleResultImpl createErrorResult(R2dbcException r2dbcException) {
    return new ErrorResult(r2dbcException);
  }

  /**
   * Creates a {@code Result} that publishes a {@code warning} as a
   * {@link Message} segment, followed by any {@code Segment}s of a
   * {@code result}.
   * @param warning Warning to publish
   * @param result Result to publisher
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
      this.updateCount = updateCount;
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      return updateCount >= 0
        ? Mono.just(new UpdateCountImpl(updateCount))
            .map(mappingFunction)
        : Mono.empty();
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

    private ResultSetResult(ResultSet resultSet, ReactiveJdbcAdapter adapter) {
      this.resultSet = resultSet;
      this.metadata = createRowMetadata(fromJdbc(resultSet::getMetaData));
      this.adapter = adapter;
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      return adapter.publishRows(resultSet, jdbcReadable ->
        mappingFunction.apply(
          new RowSegmentImpl(createRow(jdbcReadable, metadata, adapter))));
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
      updateCountResult = createUpdateCountResult(updateCount);
      generatedKeysResult = createQueryResult(generatedKeys, adapter);
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      return Flux.from(updateCountResult.publishSegments(mappingFunction))
        .concatWith(generatedKeysResult.publishSegments(mappingFunction));
    }
  }

  /**
   * {@link OracleResultImpl} subclass that publishes an single instance of
   * {@link OutParameters} as an {@link OutSegment}.
   */
  private static final class CallResult extends OracleResultImpl {

    private final OutParameters outParameters;

    private CallResult(OutParameters outParameters) {
      this.outParameters = outParameters;
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      return Mono.fromSupplier(() ->
        mappingFunction.apply(new OutSegmentImpl(outParameters)));
    }
  }

  /**
   * {@link OracleResultImpl} subclass that publishes an array of update
   * counts as {@link UpdateCount} segments.
   */
  private static final class BatchUpdateResult extends OracleResultImpl {

    private final long[] updateCounts;

    private BatchUpdateResult(long[] updateCounts) {
      this.updateCounts = updateCounts;
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      return Flux.fromStream(LongStream.of(updateCounts)
        .mapToObj(UpdateCountImpl::new))
        .map(mappingFunction);
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

    private BatchUpdateErrorResult(BatchUpdateException batchUpdateException) {
      batchUpdateResult =
        new BatchUpdateResult(batchUpdateException.getLargeUpdateCounts());
      errorResult = new ErrorResult(toR2dbcException(batchUpdateException));
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      return Flux.concat(
        batchUpdateResult.publishSegments(mappingFunction),
        errorResult.publishSegments(mappingFunction));
    }
  }

  /**
   * {@link OracleResultImpl} subclass that publishes an {@link R2dbcException}
   * as a {@link Message} segment.
   */
  private static final class ErrorResult extends OracleResultImpl {

    private final R2dbcException r2dbcException;

    private ErrorResult(R2dbcException r2dbcException) {
      this.r2dbcException = r2dbcException;
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      return Mono.just(new MessageImpl(r2dbcException))
        .map(mappingFunction);
    }
  }

  /**
   * {@link OracleResultImpl} subclass that publishes a {@link SQLWarning}
   * chain as {@link Message} segments, followed by the segments of another
   * {@link OracleResultImpl}.
   */
  private static final class WarningResult extends OracleResultImpl {

    private final SQLWarning warning;
    private final OracleResultImpl result;

    private WarningResult(SQLWarning warning, OracleResultImpl result) {
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
   * @param publisher Publisher that emits signals
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
