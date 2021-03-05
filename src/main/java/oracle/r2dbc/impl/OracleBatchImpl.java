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
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;

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
  private Queue<Statement> statements = new LinkedList<>();

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
  public Publisher<? extends Result> execute() {
    Queue<Statement> currentStatements = statements;
    statements = new LinkedList<>();

    AtomicBoolean isSubscribed = new AtomicBoolean(false);
    return Flux.defer(() -> {
      if (isSubscribed.compareAndSet(false, true)) {
        return Flux.fromIterable(currentStatements)
          .concatMap(Statement::execute);
      }
      else {
        return Mono.error(new IllegalStateException(
          "Multiple subscribers are not supported by the Oracle R2DBC" +
            " Batch.execute() publisher"));
      }
    });
  }
}

