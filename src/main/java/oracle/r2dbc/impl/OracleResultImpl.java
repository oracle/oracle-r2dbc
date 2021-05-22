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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static oracle.r2dbc.impl.OracleR2dbcExceptions.getOrHandleSQLException;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;

/**
 * <p>
 * Implementation of the {@link Result} SPI for Oracle Database.
 * </p><p>
 * This abstract class implements behavior that is common to all {@code Result}
 * objects. Subclasses implement abstract methods to publish an update count or
 * row data. Factory methods return instances implementing these abstract
 * methods for results of various types: Update counts, row data, and
 * generated column values.
 * </p>
 *
 * @author  harayuanwang, michael-a-mcmahon
 * @since   0.1.0
 */
abstract class OracleResultImpl implements Result {

  /**
   * Indicates if a method call on this result has already returned a
   * {@code Publisher} of row data or an update count. In conformance with the
   * R2DBC SPI, multiple attempts to consume the this result will yield an
   * {@code IllegalStateException}.
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

  /**
   * Constructs a new instance of this class. This private constructor is
   * invoked by the factory methods of this class.
   */
  private OracleResultImpl() { }

  /**
   * Creates a {@code Result} that publishes either an empty stream of row
   * data, or publishes an {@code updateCount} if it is greater than or equal
   * to zero. An {@code updateCount} less than zero is published as an empty
   * stream.
   * @param updateCount Update count to publish
   * @return An update count {@code Result}. Not null.
   */
  public static OracleResultImpl createUpdateCountResult(int updateCount) {
    return new OracleResultImpl() {

      final Publisher<Integer> updateCountPublisher =
        updateCount < 0 ? Mono.empty() : Mono.just(updateCount);

      @Override
      Publisher<Integer> publishUpdateCount() {
        return updateCountPublisher;
      }

      @Override
      <T> Publisher<T> publishRows(
        BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
        return Mono.empty();
      }
    };
  }

  /**
   * <p>
   * Creates a {@code Result} that either publishes a {@code ResultSet} of
   * row data from a query, or publishes an update count as an empty stream.
   * </p>
   * @param adapter Adapts {@code ResultSet} API calls into reactive streams.
   *   Not null.
   * @param resultSet Row data to publish. Not null. Retained.
   * @return A row data {@code Result}. Not null.
   */
  public static OracleResultImpl createQueryResult(
    ReactiveJdbcAdapter adapter, ResultSet resultSet) {

    return new OracleResultImpl() {

      /**
       * R2DBC {@code RowMetadata} adapted from JDBC
       * {@link java.sql.ResultSetMetaData}
       */
      final OracleRowMetadataImpl metadata =
        OracleRowMetadataImpl.createRowMetadata(
          getOrHandleSQLException(resultSet::getMetaData));

      @Override
      Publisher<Integer> publishUpdateCount() {
        return Mono.empty();
      }

      @Override
      <T> Publisher<T> publishRows(
        BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {

        return Flux.from(adapter.publishRows(resultSet, jdbcRow ->
          mappingFunction.apply(
            new OracleRowImpl(jdbcRow, metadata, adapter), metadata)));
      }
    };
  }

  /**
   * <p>
   * Publishes a {@code Result} that either publishes generated values of a
   * {@link PreparedStatement#getGeneratedKeys()} {@code ResultSet}, or
   * publishes an {@code updateCount}.
   * </p>
   *
   * @implNote TODO: It is not necessary to cache rows. It is not the intent of
   * the TCK to verify that rows are valid after the connection is closed.
   * Until the TCK is updated, OracleTestKit can override the default behavior.
   * @param adapter Adapts {@code ResultSet} API calls into reactive streams.
   *   Not null.
   * @param updateCount Update count to publish
   * @param values A {@code ResultSet} of generated keys. Not null. Retained.
   * @return A result that publishes generated values, or an update count.
   * Not null.
   */
  public static Publisher<OracleResultImpl> createGeneratedValuesResult(
    ReactiveJdbcAdapter adapter, int updateCount, ResultSet values) {

    // Avoid invoking ResultSet.getMetaData() on an empty ResultSet, it may
    // throw a SQLException
    if (! getOrHandleSQLException(values::isBeforeFirst))
      return Mono.just(createUpdateCountResult(updateCount));

    // Obtain metadata before the ResultSet is closed by publishRows(...)
    OracleRowMetadataImpl metadata =
      OracleRowMetadataImpl.createRowMetadata(
        getOrHandleSQLException(values::getMetaData));

    return Flux.from(adapter.publishRows(
      values, ReactiveJdbcAdapter.JdbcRow::copy))
      .collectList()
      .map(cachedRows -> new OracleResultImpl() {

        @Override
        Publisher<Integer> publishUpdateCount() {
          return Mono.just(updateCount);
        }

        @Override
        <T> Publisher<T> publishRows(
          BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
          return Flux.fromIterable(cachedRows)
            .map(jdbcRow -> mappingFunction.apply(
              new OracleRowImpl(jdbcRow, metadata, adapter), metadata));
        }
      });
  }

  /**
   * Creates a {@code Result} having no update count and a single {@code Row}
   * of out parameter values.
   *
   * @param outParameterRow {@code Row} of out parameter values. Not null.
   * Retained.
   * @return {@code Result} of {@code outParameterRow}. Not null.
   */
  static OracleResultImpl createCallResult(OracleRowImpl outParameterRow) {
    return new OracleResultImpl() {

      @Override
      <T> Publisher<T> publishRows(
        BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
        return Mono.fromSupplier(() ->
          mappingFunction.apply(
            outParameterRow, outParameterRow.metadata()));
      }

      @Override
      Publisher<Integer> publishUpdateCount() {
        return Mono.empty();
      }
    };
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning a publisher that emits a
   * positive count of updated rows, or only emits {@code onComplete} if this
   * {@code Result} has no update count or has an update count of zero.
   * </p><p>
   * The returned publisher supports multiple subscribers.
   * </p>
   */
  @Override
  public final Publisher<Integer> getRowsUpdated() {
    setPublished();

    return Flux.from(publishUpdateCount())
      .doFinally(signalType -> consumedFuture.complete(null));
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning a publisher that emits a
   * {@code mappingFunction's} output for each row of data in this
   * {@code Result}. If this result has no row data, then the returned
   * publisher only emits {@code onComplete}.
   * </p><p>
   * Rows that are input to the {@code mappingFunction} should not be retained.
   * An {@link IllegalStateException} will be thrown if any method of a
   * {@code Row} is invoked outside of the mappingFunction's scope.
   * </p><p>
   * The {@code mappingFunction} must output non-null values or throw an
   * exception. The returned publisher emits {@code onError} with
   * {@link NullPointerException} if the {@code mappingFunction} outputs
   * {@code null}.
   * </p><p>
   * The returned publisher emits {@code onError} with any exception that is
   * thrown by the {@code mappingFunction}.
   * </p><p>
   * The returned publisher does not support multiple subscribers. After
   * one subscriber has subscribed, the publisher signals {@code onError}
   * with {@code IllegalStateException} to all subsequent subscribers.
   * </p>
   */
  @Override
  public final <T> Publisher<T> map(
    BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {

    requireNonNull(mappingFunction, " Mapping function is null");
    setPublished();

    Publisher<T> rowPublisher =
      Flux.<T>from(publishRows(mappingFunction))
        .doOnTerminate(() -> consumedFuture.complete(null))
        .doOnCancel(() -> consumedFuture.complete(null));

    AtomicBoolean isSubscribed = new AtomicBoolean(false);
    return Flux.defer(() ->
      isSubscribed.compareAndSet(false, true)
        ? rowPublisher
        : Mono.error(new IllegalStateException(
            "Multiple subscribers are not supported by the Oracle R2DBC " +
              " Result.map(BiFunction) publisher")));
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
   * Marks this result as having created a {@code Publisher} of row data or
   * an update count. This method enforces the {@link Result} SPI contract which
   * does not allow the same result to be consumed more than once.
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

  /**
   * Returns a publisher that emits the result of processing row data with a
   * {@code mappingFunction}. If this {@code Result} has no row data to publish,
   * then the returned publisher only emits {@code onComplete}.
   * @param mappingFunction Maps row data to a target type
   * @param <T> The type of the mapped value
   * @return A row data publisher
   */
  abstract <T> Publisher<T> publishRows(
    BiFunction<Row, RowMetadata, ? extends T> mappingFunction);

  /**
   * Returns a publisher that emits an update count. If this {@code Result}
   * has no update count, or has an update count of zero, then the returned
   * publisher only emits {@code onComplete}.
   * @return An update count publisher
   */
  abstract Publisher<Integer> publishUpdateCount();

}
