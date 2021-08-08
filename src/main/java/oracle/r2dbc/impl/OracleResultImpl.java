package oracle.r2dbc.impl;

import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.Readable;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import oracle.r2dbc.impl.ReadablesMetadata.RowMetadataImpl;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.ResultSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static oracle.r2dbc.impl.OracleR2dbcExceptions.fromJdbc;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;
import static oracle.r2dbc.impl.OracleReadableImpl.createRow;
import static oracle.r2dbc.impl.ReadablesMetadata.createRowMetadata;

/**
 * Base class with an abstract method that creates a {@link Publisher} from
 * a {@link Segment} mapping function and {@link Segment} filtering function.
 *
 * The {@code Segment} {@code Publisher} may be consumed by
 * {@link #flatMap(Function)}. The {@code Publisher} rejects multiple
 * {@code Subscribers}, as the {@code Segments} may be instances of
 * {@link RowSegment} which must deallocate memory that retains row data
 * after the {@code RowSegment} has been mapped by a mapping function.
 *
 * The {@code Segment} {@code Publisher} may be filtered by
 * {@link #filter(Predicate)}. A call to {@code filter} returns new
 * {@code Result} that retains a reference to the {@code Segment}
 * {@code Publisher} with an additional filtering operator applied to emitted
 * values.
 *
 * The {@code Segment} {@code Publisher} may be filtered
 * and then consumed in a single call by {@link #map(BiFunction)},
 * {@link #map(Function)}, or {@link #getRowsUpdated()}.
 *
 */
abstract class OracleResultImpl implements Result {

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

  private OracleResultImpl() { }

  abstract <T> Publisher<T> publishSegments(
    Function<Segment, T> mappingFunction);

  private <T> Publisher<T> publishSegments(
    Predicate<Segment> filter, Function<Segment, T> mappingFunction) {

    setPublished();

    return Flux.from(publishSegments(
      new FilteredMappingFunction<>(filter, mappingFunction)))
      .filter(output -> output != FILTERED)
      .doOnTerminate(() -> consumedFuture.complete(null))
      .doOnCancel(() -> consumedFuture.complete(null));
  }

  @Override
  public <T> Publisher<T> flatMap(
    Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
    requireNonNull(mappingFunction, "mappingFunction is null");
    return singleSubscriber(Flux.concat(
      publishSegments(segment -> true, mappingFunction)));
  }

  @Override
  public Publisher<Integer> getRowsUpdated() {
    return publishSegments(
      segment -> segment instanceof UpdateCount,
      segment -> Math.toIntExact(((UpdateCount)segment).value()));
  }

  @Override
  public <T> Publisher<T> map(
    BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
    requireNonNull(mappingFunction, "mappingFunction is null");
    return singleSubscriber(publishSegments(
      segment -> segment instanceof RowSegment,
      segment -> {
        Row row = ((RowSegment)segment).row();
        return mappingFunction.apply(row, row.getMetadata());
      }));
  }

  @Override
  public <T> Publisher<T> map(
    Function<? super Readable, ? extends T> mappingFunction) {
    requireNonNull(mappingFunction, "mappingFunction is null");
    return singleSubscriber(publishSegments(
      segment -> segment instanceof ReadableSegment,
      segment ->
        mappingFunction.apply(((ReadableSegment)segment).getReadable())));
  }

  @Override
  public OracleResultImpl filter(Predicate<Segment> filter) {
    return new FilteredResult(requireNonNull(filter, "filter is null"));
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
    if (isPublished) {
      throw new IllegalStateException(
        "A result can not be consumed more than once");
    }
    else {
      isPublished = true;
    }
  }

  private final class FilteredResult extends OracleResultImpl {
    private final Predicate<Segment> filter;

    private FilteredResult(Predicate<Segment> filter) {
      this.filter = filter;
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      return OracleResultImpl.this.publishSegments(
        new FilteredMappingFunction<>(filter, mappingFunction));
    }
  }

  public static OracleResultImpl createQueryResult(
     ResultSet resultSet, ReactiveJdbcAdapter adapter) {
    return new ResultSetResult(resultSet, adapter);
  }

  static OracleResultImpl createCallResult(OutParameters outParameters) {
    return new CallResult(outParameters);
  }

  static OracleResultImpl createGeneratedValuesResult(
    long updateCount, ResultSet generatedKeys, ReactiveJdbcAdapter adapter) {
    return new GeneratedKeysResult(updateCount, generatedKeys, adapter);
  }

  static OracleResultImpl createUpdateCountResult(Long updateCount) {
    return new UpdateCountResult(updateCount);
  }

  private static final class UpdateCountResult extends OracleResultImpl {

    private final long updateCount;

    private UpdateCountResult(long updateCount) {
      this.updateCount = updateCount;
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      return updateCount >= 0
        ? Mono.just(mappingFunction.apply(new UpdateCountImpl(updateCount)))
        : Mono.empty();
    }
  }

  private static final class ResultSetResult extends OracleResultImpl {

    private final ResultSet resultSet;
    private final ReactiveJdbcAdapter adapter;

    private ResultSetResult(ResultSet resultSet, ReactiveJdbcAdapter adapter) {
      this.resultSet = resultSet;
      this.adapter = adapter;
    }

    @Override
    <T> Publisher<T> publishSegments(Function<Segment, T> mappingFunction) {
      RowMetadataImpl metadata =
        createRowMetadata(fromJdbc(resultSet::getMetaData));

      return adapter.publishRows(resultSet, jdbcReadable ->
        mappingFunction.apply(
          new RowSegmentImpl(createRow(jdbcReadable, metadata, adapter))));
    }
  }

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

  private static final Object FILTERED = new Object();

  private static final class FilteredMappingFunction<T>
    implements Function<Segment,T> {

    private final Predicate<Segment> filter;

    private final Function<Segment, T> mappingFunction;

    private FilteredMappingFunction(
      Predicate<Segment> filter, Function<Segment, T> mappingFunction) {
      this.filter = filter;
      this.mappingFunction = mappingFunction;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T apply(Segment segment) {
      return filter.test(segment)
        ? mappingFunction.apply(segment)
        : (T)FILTERED;
    }

  }

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
   * Common interface for instances of {@link Segment} with a {@link Readable}
   * value. The {@link #map(Function)} filters for this segment type, and uses
   * the common {@link #getReadable()} method to obtain a {@link Readable} from
   * the segment.
   */
  private interface ReadableSegment {
    Readable getReadable();
  }

  private static <T> Publisher<T> singleSubscriber(Publisher<T> publisher) {
    AtomicBoolean isSubscribed = new AtomicBoolean(false);
    return Flux.defer(() ->
      isSubscribed.compareAndSet(false, true)
        ? publisher
        : Mono.error(new IllegalStateException(
            "Publisher does not support multiple subscribers")));
  }
}
