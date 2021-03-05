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

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.getOrHandleSQLException;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.runOrHandleSQLException;

/**
 * <p>
 * Implementation of the {@link Connection} SPI for Oracle Database.
 * </p><p>
 * Instances of this class represent a session in which a user performs
 * operations on an Oracle Database. Sessions typically begin by establishing
 * a network connection to the database and then authenticating as a particular
 * user. Operations are typically specified as Structured Query Language (SQL)
 * {@linkplain #createStatement(String) statements} that store and retrieve
 * information from relational data structures. Operations occur within the
 * scope of a transaction. A transaction must either be
 * {@linkplain #commitTransaction() committed} or
 * {@linkplain #rollbackTransaction() rolled back}. If committed, then changes
 * made within the transaction become visible to other sessions. If rolled back,
 * then the changes are discarded.
 * </p><p>
 * Instances of this class operate on a {@link java.sql.Connection} from a
 * JDBC Driver. JDBC API calls are adapted into Reactive Streams APIs
 * using a {@link ReactiveJdbcAdapter}.
 * </p>
 *
 * @author  harayuanwang, michael-a-mcmahon
 * @since   0.1.0
 */
final class OracleConnectionImpl implements Connection {

  /** Adapts JDBC Driver APIs into Reactive Streams APIs */
  private final ReactiveJdbcAdapter adapter;

  /**
   * JDBC connection to an Oracle Database that this connection uses to
   * perform database operations.
   */
  private final java.sql.Connection jdbcConnection;

  /**
   * Constructs a new connection that uses the specified {@code adapter} to
   * perform database operations with the specified {@code jdbcConnection}.
   * @param adapter Adapts JDBC calls into reactive streams. Not null. Retained.
   * @param jdbcConnection JDBC connection to an Oracle Database. Not null.
   *                       Retained.
   */
  OracleConnectionImpl(
    ReactiveJdbcAdapter adapter, java.sql.Connection jdbcConnection) {
    this.adapter = adapter;
    this.jdbcConnection = jdbcConnection;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by executing a {@code SET TRANSACTION}
   * command to explicitly begin a transaction on the Oracle Database to which
   * JDBC is connected.
   * </p><p>
   * Oracle Database supports transactions that begin <i>implicitly</i>
   * when executing SQL statements that modify data, or when a executing a
   * {@code SELECT ... FOR UPDATE} command. This functionality is accessible
   * with the Oracle R2DBC Driver, but R2DBC applications should not rely on
   * it. For maximum portability between R2DBC drivers, applications should
   * explicitly begin transactions by invoking this method.
   * </p><p>
   * The returned publisher begins a transaction <i>after</i> a subscriber
   * subscribes, <i>before</i> the subscriber emits a {@code request}
   * signal. Multiple subscribers are supported, but the returned publisher
   * does not repeat the action of beginning a transaction for each
   * subscription. Any signals emitted to the first subscription are
   * propagated to subsequent subscriptions.
   * </p>
   */
  @Override
  public Publisher<Void> beginTransaction() {
    // The SET TRANSACTION command must specify an isolation level, and it
    // the level must be the same as what the JDBC connection has been set to.
    final String isolationLevel;

    int jdbcIsolationLevel =
      getOrHandleSQLException(jdbcConnection::getTransactionIsolation);
    switch (jdbcIsolationLevel) {
      case TRANSACTION_READ_COMMITTED:
        isolationLevel = "READ COMMITTED";
        break;
      case TRANSACTION_SERIALIZABLE:
        isolationLevel = "SERIALIZABLE";
        break;
      default:
        // In 21c, Oracle only supports READ COMMITTED or SERIALIZABLE, so the
        // only code that can be verified is in the cases above.
        throw OracleR2dbcExceptions.newNonTransientException(
          "Unrecognized JDBC transaction isolation level: "
            + jdbcIsolationLevel, null);
    }

    return Mono.from(setAutoCommit(false))
      .then(Mono.from(createStatement(
        "SET TRANSACTION ISOLATION LEVEL " + isolationLevel)
        .execute())
        .flatMap(result -> Mono.from(result.getRowsUpdated()))
        .then())
      .cache();
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by closing the JDBC connection.
   * </p><p>
   * Publishers emit {@code onError} with an {@link IllegalStateException} when
   * interacting with a closed connection or with any objects created by a
   * closed connection .
   * </p><p>
   * The returned publisher closes the connection <i>after</i> a subscriber
   * subscribes, <i>before</i> the subscriber emits a {@code request}
   * signal. Multiple subscribers are supported, but the returned publisher
   * does not repeat the action of closing the connection for each
   * subscription. Signals emitted to the first subscription are propagated
   * to all subsequent subscriptions.
   * </p><p>
   * Calling this method on a Connection that is already closed is a no-op.
   * The returned publisher emits {@code onComplete} if the connection is
   * already closed.
   * </p>
   */
  @Override
  public Publisher<Void> close() {
    return adapter.publishClose(jdbcConnection);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by committing a transaction on the
   * Oracle Database to which JDBC is connected.
   * </p><p>
   * The returned publisher commits the transaction <i>after</i> a
   * subscriber subscribes, <i>before</i> the subscriber emits a {@code
   * request} signal. Multiple subscribers are supported, but the returned
   * publisher does not repeat the action of committing the transaction for
   * each subscription. Signals emitted to the first subscription are
   * propagated to all subsequent subscriptions.
   * </p><p>
   * Calling this method is a no-op if auto-commit is enabled. The returned
   * publisher emits {@code onComplete} if auto-commit is enabled.
   * </p>
   */
  @Override
  public Publisher<Void> commitTransaction() {
    return adapter.publishCommit(jdbcConnection);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning a {@code Batch} that executes
   * a sequence of arbitrary SQL statements on the Oracle Database to which JDBC
   * is connected.
   * </p><p>
   * Parallel execution of {@code Batch} objects created by a single {@code
   * Connection} is <i>not</i> supported by the Oracle R2DBC Driver. The
   * Oracle R2DBC Driver reflects the capabilities of Oracle Database, which
   * does <i>not</i> support parallel execution of SQL within a single
   * database session. Attempting parallel execution of {@code Batch} objects
   * from the same {@code Connection} will cause threads to become blocked as
   * each SQL command executes serially.
   * </p>
   *
   * @throws IllegalStateException if this connection is closed
   */
  @Override
  public Batch createBatch() {
    requireOpenConnection();
    return new OracleBatchImpl(adapter, jdbcConnection);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning a new statement that is
   * executed by the Oracle Database to which JDBC is connected.
   * </p><p>
   * Parallel execution of {@code Statement} objects created by a single
   * {@code Connection} is <i>not</i> supported by the Oracle R2DBC Driver.
   * The Oracle R2DBC Driver reflects the capabilities of Oracle Database, which
   * does <i>not</i> support parallel execution of SQL within a single
   * database session. Attempting parallel execution of {@code Statement}
   * objects from the same {@code Connection} will cause threads to become
   * blocked as each statement executes serially.
   * </p>
   *
   * @throws IllegalStateException if this connection is closed
   */
  @Override
  public Statement createStatement(String sql) {
    requireNonNull(sql, "sql is null");
    requireOpenConnection();
    return new OracleStatementImpl(adapter, jdbcConnection, sql);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning the current auto-commit mode
   * of the JDBC connection.
   * </p>
   * @throws IllegalStateException if this connection is closed
   */
  @Override
  public boolean isAutoCommit() {
    requireOpenConnection();
    return getOrHandleSQLException(jdbcConnection::getAutoCommit);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning metadata about the
   * Oracle Database to which JDBC is connected.
   * </p>
   * @throws IllegalStateException if this connection is closed
   */
  @Override
  public ConnectionMetadata getMetadata() {
    requireOpenConnection();
    return new OracleConnectionMetadataImpl(
      getOrHandleSQLException(jdbcConnection::getMetaData));
  }

  /**
   * {@inheritDoc}
   * <p>
   * This SPI method is not yet implemented.
   * </p>
   * @throws UnsupportedOperationException Always
   */
  @Override
  public Publisher<Void> createSavepoint(String name) {
    requireNonNull(name, "name is null");
    // TODO: Execute SQL to create a savepoint. Examine and understand the
    // Oracle JDBC driver's implementation of
    // OracleConnection.oracleSetSavepoint(), and replicate it without
    // blocking a thread. Consider adding a ReactiveJDBCAdapter API to do this.
    throw new UnsupportedOperationException("createSavepoint not supported");
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method as a no-op. Oracle Database does not
   * support explicit releasing of savepoints.
   * </p>
   */
  @Override
  public Publisher<Void> releaseSavepoint(String name) {
    requireNonNull(name, "name is null");
    return Mono.empty();
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by rolling back a transaction on the
   * Oracle Database to which JDBC is connected.
   * </p><p>
   * The returned publisher rolls back the current transaction <i>after</i>
   * a subscriber subscribes, <i>before</i> the subscriber emits a {@code
   * request} signal. Multiple subscribers are supported, but the returned
   * publisher does not repeat the action of rolling back the transaction for
   * each subscription. Signals emitted to the first subscription are
   * propagated to all subsequent subscriptions.
   * </p><p>
   * Calling this method is a no-op if auto-commit is enabled. The returned
   * publisher emits {@code onComplete} if auto-commit is enabled.
   * </p>
   */
  @Override
  public Publisher<Void> rollbackTransaction() {
    return adapter.publishRollback(jdbcConnection);
  }

  /**
   * {@inheritDoc}
   * <p>
   * This SPI method is not yet implemented.
   * </p>
   * @throws UnsupportedOperationException In version 0.1.0
   */
  @Override
  public Publisher<Void> rollbackTransactionToSavepoint(String name) {
    requireNonNull(name, "name is null");
    // TODO: Use the JDBC connection to rollback to a savepoint without blocking
    // a thread.
    throw new UnsupportedOperationException(
      "rollbackTransactionToSavepoint not supported");
  }

  /**
   * {@inheritDoc}
   * <p>
   * This SPI method implementation sets the auto-commit mode of the JDBC
   * connection.
   * </p><p>
   * The returned publisher sets the JDBC connection's auto-commit mode
   * <i>after</i> a subscriber subscribes, <i>before</i> the subscriber
   * emits a {@code request} signal. Multiple subscribers are supported, but
   * the returned publisher does not repeat the action of setting the
   * auto-commit mode for each subscription. Signals emitted to the first
   * subscription are propagated to all subsequent subscriptions.
   * </p>
   */
  @Override
  public Publisher<Void> setAutoCommit(boolean autoCommit) {
    return Mono.defer(() -> getOrHandleSQLException(() -> {
      if (autoCommit == jdbcConnection.getAutoCommit()) {
        return Mono.empty(); // No change
      }
      else if (! autoCommit) {
        // Changing auto-commit from enabled to disabled. When enabled,
        // there is no active transaction.
        jdbcConnection.setAutoCommit(false);
        return Mono.empty();
      }
      else {
        // Changing auto-commit from disabled to enabled. Commit in case
        // there is an active transaction.
        return Mono.from(commitTransaction())
          .doOnSuccess(nil -> runOrHandleSQLException(() ->
            jdbcConnection.setAutoCommit(true)));
      }
    }))
    .cache();
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning the JDBC connection's
   * transaction isolation level.
   * </p>
   * @implNote Currently, Oracle R2DBC only supports the READ COMMITTED
   * isolation level.
   * @throws IllegalStateException if this connection is closed
   */
  @Override
  public IsolationLevel getTransactionIsolationLevel() {
    requireOpenConnection();
    return IsolationLevel.READ_COMMITTED;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by setting the transaction isolation
   * level of the JDBC connection.
   * </p><p>
   * Oracle Database only supports {@link IsolationLevel#READ_COMMITTED} and
   * {@link IsolationLevel#SERIALIZABLE} isolation levels. If an unsupported
   * {@code isolationLevel} is specified to this method, then the returned
   * publisher emits {@code onError} with an {@link R2dbcException}
   * indicating that the specified {@code isolationLevel} is not supported.
   * </p><p>
   * Oracle Database does not support changing an isolation level during
   * an active transaction. If the isolation level is changed during an
   * active transaction, then the returned publisher emits {@code onError}
   * with an {@link R2dbcException} indicating that changing the isolation level
   * during an active transaction is not supported.
   * </p><p>
   * The returned publisher sets the transaction isolation level
   * <i>after</i> a subscriber subscribes, <i>before</i> the subscriber
   * emits a {@code request} signal. Multiple subscribers are supported, but
   * the returned publisher does not repeat the action of setting the
   * transaction isolation level for each subscription. Signals emitted to
   * the first subscription are propagated to all subsequent subscriptions.
   * </p>
   * @implNote Currently, Oracle R2DBC only supports the READ COMMITTED
   * isolation level.
   */
  @Override
  public Publisher<Void> setTransactionIsolationLevel(
    IsolationLevel isolationLevel) {
    requireNonNull(isolationLevel, "isolationLevel is null");

    // TODO: Need to add a connection factory option that disables Oracle
    //  JDBC's Result Set caching function before SERIALIZABLE can be supported.
    // For now, the isolation level can never be changed from the default READ
    // COMMITTED.
    if (isolationLevel.equals(IsolationLevel.READ_COMMITTED)) {
      return Mono.empty();
    }
    else {
      return Mono.error(OracleR2dbcExceptions.newNonTransientException(
        "Oracle R2DBC does not support isolation level: " + isolationLevel,
        null));
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by validating the JDBC connection in one
   * of two ways, either locally or remotely, as specified by the {@code
   * depth} parameter. Local validation tests if the JDBC connection has
   * become closed, and remote validation tests if the JDBC connection can
   * execute a SQL statement.
   * </p><p>
   * The returned publisher validates the connection <i>after</i> a
   * subscriber subscribes, <i>before</i> the subscriber emits a {@code
   * request} signal. Multiple subscribers are supported, but the returned
   * publisher does not repeat the action of validating the connection for each
   * subscription. Signals emitted to the first subscription are
   * propagated to all subsequent subscriptions.
   * </p>
   * @implNote Remote validation executes a SQL query against the {@code sys
   * .dual} table. It is assumed that all Oracle Databases have the {@code
   * sys.dual} table.
   */
  @Override
  public Publisher<Boolean> validate(ValidationDepth depth) {
    requireNonNull(depth, "depth is null");
    return Mono.defer(() -> getOrHandleSQLException(() -> {
      if (jdbcConnection.isClosed()) {
        return Mono.just(false);
      }
      else if (depth == ValidationDepth.LOCAL) {
        return Mono.just(true);
      }
      else {
        return Mono.from(createStatement("SELECT 1 FROM sys.dual")
          .execute())
          .flatMap(result ->
            Mono.from(result.map((row, metadata) ->
              row.get(0, Integer.class))))
          .map(value -> Integer.valueOf(1).equals(value))
          .defaultIfEmpty(false)
          .onErrorReturn(false);
      }
    }))
    .cache();
  }

  /**
   * Checks if the JDBC connection is open, and throws an exception if the
   * check fails.
   * @throws IllegalStateException If the JDBC connection is closed
   */
  private void requireOpenConnection() {
    if (getOrHandleSQLException(jdbcConnection::isClosed))
      throw new IllegalStateException("Connection is closed");
  }

}