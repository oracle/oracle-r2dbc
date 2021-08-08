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
import java.time.Duration;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.R2dbcException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireOpenConnection;

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
   * Timeout applied to each statement this {@code Batch} executes;
   */
  private final Duration timeout;

  /**
   * Ordered sequence of SQL commands that have been added to this batch. May
   * be empty.
   */
  private Queue<OracleStatementImpl> statements = new LinkedList<>();

  /**
   * Constructs a new batch that uses the specified {@code adapter} to execute
   * SQL statements with a {@code jdbcConnection}.
   * @param timeout Timeout applied to each statement this batch executes.
   * Not null. Not negative.
   * @param jdbcConnection JDBC connection to an Oracle Database. Not null.
   * @param adapter Adapts JDBC calls into reactive streams. Not null.
   */
  OracleBatchImpl(
    Duration timeout, Connection jdbcConnection, ReactiveJdbcAdapter adapter) {
    this.timeout = timeout;
    this.jdbcConnection =
      requireNonNull(jdbcConnection, "jdbcConnection is null");
    this.adapter = requireNonNull(adapter, "adapter is null");
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
    statements.add(
      new OracleStatementImpl(sql, timeout, jdbcConnection, adapter));
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
   * @return {@code Publisher} of {@code statements} {@code Result}s
   */
  private static Publisher<OracleResultImpl> publishBatch(
    Queue<OracleStatementImpl> statements) {

    OracleStatementImpl next = statements.poll();

    if (next != null) {
      AtomicReference<OracleResultImpl> lastResult =
        new AtomicReference<>(null);

      return Flux.from(next.execute())
        .doOnNext(lastResult::set)
        .concatWith(Flux.defer(() ->
          Mono.from(lastResult.get().onConsumed())
            .thenMany(publishBatch(statements))));
    }
    else {
      return Mono.empty();
    }
  }

}

