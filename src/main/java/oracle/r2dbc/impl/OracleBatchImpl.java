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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import static oracle.r2dbc.impl.OracleR2dbcExceptions.getOrHandleSQLException;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireOpenConnection;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.runOrHandleSQLException;

/**
 * <p>
 * Implementation of the {@link Batch} SPI for Oracle Database. This SPI
 * implementation executes an ordered sequence of arbitrary SQL statements
 * using a JDBC connection. JDBC API calls are adapted into Reactive Streams
 * APIs using a {@link ReactiveJdbcAdapter}.
 * </p><p>
 * Oracle Database supports batch execution of parameterized DML statements,
 * but does not support batch execution of arbitrary SQL statements. This
 * implementation reflects the capabilities of Oracle Database; It does not
 * offer any performance benefit compared to individually executing each
 * statement in the batch.
 * </p>
 *
 * @author  harayuanwang, michael-a-mcmahon
 * @since   0.1.0
 */
final class OracleBatchImpl implements Batch {

  /** Adapts Oracle JDBC Driver APIs into Reactive Streams APIs */
  private final ReactiveJdbcAdapter adapter;

  /**
   * JDBC connection to an Oracle Database which executes this batch.
   */
  private final Connection jdbcConnection;

  /**
   * Ordered sequence of SQL commands that have been added to this batch. May
   * be empty.
   */
  private Queue<OracleStatementImpl> statements = new LinkedList<>();

  /**
   * Constructs a new batch that uses the specified {@code adapter} to execute
   * SQL statements with a {@code jdbcConnection}.
   * @param adapter Adapts JDBC calls into reactive streams.
   * @param jdbcConnection JDBC connection to an Oracle Database.
   */
  OracleBatchImpl(
    ReactiveJdbcAdapter adapter, java.sql.Connection jdbcConnection) {
    this.adapter = requireNonNull(adapter, "adapter is null");
    this.jdbcConnection =
      requireNonNull(jdbcConnection, "jdbcConnection is null");
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by adding a {@code sql} command to the
   * end of the command sequence of the current batch.
   * </p>
   */
  @Override
  public Batch add(String sql) {
    requireOpenConnection(jdbcConnection);
    requireNonNull(sql, "sql is null");
    statements.add(new OracleStatementImpl(adapter, jdbcConnection, sql));
    return this;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by executing the SQL statements that have
   * been added to the current batch since the previous execution. Statements
   * are executed in the order they were added. Calling this method clears all
   * statements that have been added to the current batch.
   * </p><p>
   * A {@code Result} emitted by the returned {@code Publisher} must be
   * <a href="OracleStatementImpl.html#fully-consumed-result">
   *   fully-consumed
   * </a>
   * before the next {@code Result} is emitted. This ensures that a command in
   * the batch can not be executed while the {@code Result} of a previous
   * command is consumed concurrently. It is a known limitation of the Oracle
   * R2DBC Driver that concurrent operations on a single {@code Connection}
   * will result in blocked threads. Deferring {@code Statement} execution
   * until full consumption of the previous {@code Statement}'s {@code Result}
   * is necessary in order to avoid blocked threads.
   * </p><p>
   * If the execution of any statement in the sequence results in a failure,
   * then the returned publisher emits {@code onError} with an
   * {@link R2dbcException} that describes the failure, and all subsequent
   * statements in the sequence are not executed.
   * </p><p>
   * The returned publisher begins executing the batch <i>after</i> a
   * subscriber subscribes, <i>before</i> the subscriber emits a {@code
   * request} signal. The returned publisher does not support multiple
   * subscribers. After one subscriber has subscribed, the returned publisher
   * signals {@code onError} with {@code IllegalStateException} to any
   * subsequent subscribers.
   * </p>
   * @implNote Oracle Database does not offer native support for batched
   * execution of arbitrary SQL statements. This SPI method is implemented by
   * individually executing each statement in this batch.
   */
  @Override
  public Publisher<OracleResultImpl> execute() {
    requireOpenConnection(jdbcConnection);
    Queue<OracleStatementImpl> currentStatements = statements;
    statements = new LinkedList<>();
    return publishBatch(currentStatements);
  }

  /**
   * <p>
   * Publish a batch of {@code Result}s from {@code statements}. Each
   * {@code Result} is published serially with the consumption of the
   * previous {@code Result}.
   * </p><p>
   * This method returns an empty {@code Publisher} if {@code statements} is
   * empty. Otherwise, this method dequeues the next {@code Statement} and
   * executes it for a {@code Result}. After the {@code Result} has been
   * fully consumed, this method is invoked recursively to publish the {@code
   * Result}s of remaining {@code statements}.
   * </p>
   * @param statements A batch to executed.
   * @return
   */
  private static Publisher<OracleResultImpl> publishBatch(
    Queue<OracleStatementImpl> statements) {

    OracleStatementImpl next = statements.poll();

    if (next != null) {
      AtomicReference<OracleResultImpl> lastResult =
        new AtomicReference<>(null);

      return Flux.from(next.execute())
        .doOnNext(lastResult::set)
        .concatWith(Mono.defer(() ->
          Mono.from(lastResult.get().onConsumed())
            .cast(OracleResultImpl.class)))
        .concatWith(Flux.defer(() -> publishBatch(statements)));
    }
    else {
      return Mono.empty();
    }
  }

  /*
    return Flux.create(new Consumer<>() {

      final AtomicBoolean isSubscribed = new AtomicBoolean(false);

      @Override
      public void accept(FluxSink<OracleResultImpl> fluxSink) {
        if (isSubscribed.compareAndSet(false, true))
          publishNext(fluxSink);
        else {
          fluxSink.error(new IllegalStateException(
            "Multiple subscribers are not supported by the Oracle R2DBC" +
              " Batch.execute() publisher"));
        }
      }

      void publishNext(FluxSink<OracleResultImpl> fluxSink) {
        OracleStatementImpl nextStatement = statements.poll();
        if (nextStatement != null) {

          AtomicReference<OracleResultImpl> lastResult =
            new AtomicReference<>();

          Flux.from(nextStatement.execute())
            .subscribe(nextResult -> { // onNext
              fluxSink.next(nextResult);
              lastResult.set(nextResult);
            },
            fluxSink::error, // onError
            () -> // onComplete
              lastResult.get().runOnConsumed(() ->
                publishNext(fluxSink)),
            subscription -> // request
              fluxSink.onRequest(subscription::request));
        }
        else {
          fluxSink.complete();
        }
      }
    });
  }

  /**
   * Executes each {@code Statement} in a {@code Queue} of {@code statements}.
   * A {@code Statement} is not executed until the {@code Result} of any
   * previous {@code Statement} is fully-consumed.
   * @param statements {@code Statement}s to execute. Not null.
   * @return A {@code Publisher} of each {@code Statement}'s {@code Result}.
   * Not null.
  private static Publisher<? extends Result> executeBatch(
    Queue<Statement> statements) {
    return publishSequential(statements, Statement::execute);
  }
   */

  /**
   * Returns a {@code Publisher} that emits 0 or 1 results to a subscriber. When
   * the subscriber subscribes, {@link java.sql.Statement#getMoreResults()} is
   * invoked on a JDBC {@code statement}, causing any previously opened
   * {@code ResultSet} to be closed. The {@code Subscriber} then receives a
   * {@code Result} if one more is available, then receives {@code onComplete}.
   * @param adapter
   * @param statement
   * @return
   */

  /**
   * Sequentially generates {@code Result} {@code Publishers}, guaranteeing
   * that the generation of each {@code Publisher} happens after the previously
   * generated {@code Publisher}'s  {@code Result} is fully consumed.
   */
  static Publisher<OracleResultImpl> publishNextResult(
    ReactiveJdbcAdapter adapter, java.sql.Statement statement) {

    OracleResultImpl next =
      getOrHandleSQLException(() -> getNextResult(adapter, statement));

    if (next == null) {
      return Mono.empty();
    }
    else {
      return Mono.just(next)
        .then(Mono.from(next.onConsumed())
          .cast(OracleResultImpl.class))
        .concatWith(Mono.defer(() ->
          Mono.from(publishNextResult(adapter, statement))));
    }
  }

    /*
  static Publisher<OracleResultImpl> publishNextResult(
    ReactiveJdbcAdapter adapter, java.sql.Statement statement) {
    return Flux.generate(synchronousSink -> runOrHandleSQLException(() -> {
      if (statement.getMoreResults()) {
        synchronousSink.next(
          OracleResultImpl.createQueryResult(
            adapter, statement.getResultSet()));
      }
      else {
        int updateCount = statement.getUpdateCount();
        if (updateCount != -1) {
          synchronousSink.next(
            OracleResultImpl.createUpdateCountResult(updateCount));
        }
      }
      synchronousSink.complete();
    }));
  }
    // concat : subscribe after previous onComplete
    // defer : invoke gmr after previous onConsumed
    return Flux.concat(() -> new Iterator<>() {

      Publisher<Void> previous = Mono.empty();

      @Override
      public boolean hasNext() {
        // called only after previous onComplete?
        return false;
      }

      @Override
      public Publisher<? extends OracleResultImpl> next() {
        return null;
      }
    });
    return Flux.create(new Consumer<>() {

      @Override
      public void accept(FluxSink<OracleResultImpl> fluxSink) {
        OracleResultImpl next = getOrHandleSQLException(this::next);

        if (next != null) {
          fluxSink.next(next);
          next.runOnConsumed(() -> accept(fluxSink));
        }
        else {
          fluxSink.complete();
        }
      }

      OracleResultImpl next() throws SQLException {
        if (statement.getMoreResults()) {
          return OracleResultImpl.createQueryResult(
            adapter, statement.getResultSet());
        }
        else {
          int updateCount = statement.getUpdateCount();
          if (updateCount != -1) {
            return OracleResultImpl.createUpdateCountResult(updateCount);
          }
          else {
            return null;
          }
        }
      }
    });
    */

  private static OracleResultImpl getNextResult(
    ReactiveJdbcAdapter adapter, java.sql.Statement statement)
    throws SQLException {
      if (statement.getMoreResults()) {
        return OracleResultImpl.createQueryResult(
          adapter, statement.getResultSet());
      }
      else {
        int updateCount = statement.getUpdateCount();
        if (updateCount != -1) {
          return OracleResultImpl.createUpdateCountResult(updateCount);
        }
        else {
          return null;
        }
      }
    }

  /**
   * <p>
   * A {@code Result} that completes a {@link CompletableFuture} when it has
   * been fully consumed. Instances of {@code BatchResult} are used by Oracle
   * R2DBC to ensure that statement execution and row data processing do
   * not occur concurrently; The completion of the future signals that the row
   * data of a result has been fully consumed, and that no more database
   * calls will be initiated to fetch additional rows.
   * </p><p>
   * Instances of {@code BatchResult} delegate invocations of
   * {@link #getRowsUpdated()} and {@link #map(BiFunction)} to a
   * {@code Result} provided on construction; The behavior of {@code Publisher}s
   * returned by these methods is identical to those returned by the delegate
   * {@code Result}.
   * </p>
   */
  private static final class BatchResult implements Result {

    /** Completed when this {@code BatchResult} is fully consumed */
     final CompletableFuture<Void> consumeFuture;

    /** Delegate {@code Result} that provides row data or an update count */
    final Result delegateResult;

    /**
     * Constructs a new result that completes a {@code consumeFuture} when the
     * row data or update count of a {@code delegateResult} has been fully
     * consumed.
     * @param consumeFuture Future completed upon consumption
     * @param delegateResult Result of row data or an update count
     */
    BatchResult(CompletableFuture<Void> consumeFuture, Result delegateResult) {
      this.consumeFuture = consumeFuture;
      this.delegateResult = delegateResult;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Immediately completes the {@link #consumeFuture} and then returns the
     * update count {@code Publisher} of the {@link #delegateResult}. After
     * returning an update count {@code Publisher}, the {@link #delegateResult}
     * can not initiate any more database calls (based on the assumption
     * noted below).
     * </p>
     * @implNote It is assumed that the {@link #delegateResult} will throw
     * {@link IllegalStateException} upon multiple attempts to consume it, and
     * this method does not check for multiple consumptions.
     */
    @Override
    public Publisher<Integer> getRowsUpdated() {
      consumeFuture.complete(null);
      return Flux.from(delegateResult.getRowsUpdated());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Completes the {@link #consumeFuture} after the row data {@code
     * Publisher} of the {@link #delegateResult} emits a terminal signal or
     * has it's {@code Subscription} cancelled. After emitting a terminal
     * signal or having it's {@code Subscription} cancelled, the
     * {@link #delegateResult} can not initiate any more database calls.
     * </p>
     * @implNote It is assumed that the {@link #delegateResult} will throw
     * {@link IllegalStateException} upon multiple attempts to consume it, and
     * this method does not check for multiple consumptions.
     */
    @Override
    public <T> Publisher<T> map(
      BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
      return Flux.<T>from(delegateResult.map(mappingFunction))
        .doFinally(signalType -> consumeFuture.complete(null));
    }
  }
}

