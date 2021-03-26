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

import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Reader;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static oracle.r2dbc.impl.OracleR2dbcExceptions.getOrHandleSQLException;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.runOrHandleSQLException;

/**
 * <p>
 * Implementation of the {@link Statement} SPI for the Oracle Database.
 * </p><p>
 * This implementation executes SQL using a {@link java.sql.PreparedStatement}
 * from the Oracle JDBC Driver. JDBC API calls are adapted into Reactive
 * Streams APIs using a {@link ReactiveJdbcAdapter}.
 * </p>
 *
 * <h3>Database Cursor Management</h3>
 * <p>
 * A cursor is opened each time a new SQL statement is executed on an Oracle
 * Database session. If a session never closes it's cursors, it will
 * eventually exceed the maximum number of open cursors allowed by the Oracle
 * Database and an ORA-01000 error will be raised. The Oracle R2DBC Driver
 * will close cursors after each {@link Result} emitted by the
 * {@link #execute()} publisher has been fully consumed.
 * </p><p>
 * To ensure that cursors are eventually closed, application code MUST
 * fully consume the {@link Result} objects emitted by the {@link #execute()}
 * publisher. A {@code Result} is fully consumed by subscribing to it's
 * {@linkplain Result#getRowsUpdated() update count} or
 * {@linkplain Result#map(BiFunction) row data} {@code Publisher} and then
 * requesting items until the publisher emits {@code onComplete/onError} or
 * cancelling the subscription.
 * </p><p>
 * To improve performance when the same SQL statement is executed multiple
 * times, implementations of {@link ReactiveJdbcAdapter} are expected to
 * configure statement caching using any non-standard APIs that the adapted
 * JDBC driver may implement.
 * </p>
 *
 * <h3 id="named_parameters">Named Parameter Markers</h3>
 * <p>
 * The Oracle R2DBC Driver implements the {@code Statement} SPI to support
 * named parameter markers. A expression of the form {@code :name} designates
 * a parameterized value within the SQL statement. The following example shows a
 * SQL statement with two named parameter markers in the WHERE clause:
 * </p><pre>
 *   SELECT name FROM pets WHERE species=:species AND age=:age
 * </pre><p>
 * Parameter values can be bound to alpha-numeric names that appear
 * after the colon character. Given a {@link Statement} created with the SQL
 * above, the following code would set parameter values to select the names
 * of all 10 year old dogs:
 * </p><pre>
 *   statement
 *     .bind("species", "Dog")
 *     .bind("age", 10);
 * </pre>
 *
 * <h3>JDBC Style Parameter Markers</h3>
 * <p>
 * The Oracle R2DBC Driver implements the {@code Statement} SPI to support
 * JDBC style parameter markers. A {@code ?} character designates a
 * parameterized value within the SQL statement. When this style of parameter
 * is used, the Oracle R2DBC Driver does not support SPI methods for setting
 * {@linkplain #bind(String, Object) named binds}. The following example
 * shows a SQL statement with two {@code ?} parameter markers in the WHERE
 * clause:
 * </p><pre>
 *   SELECT name FROM pets WHERE species=? AND age=?
 * </pre><p>
 * Parameter values can be bound to the numeric zero-based index at that a
 * {@code ?} marker appears when the statement is read from left to right. In
 * the example above, the {@code species=?} marker appears first, so the bind
 * index for this parameter is {@code 0}. The {@code age=?} marker appears
 * next, so the bind index for that  parameter is {@code 1}. Given a
 * {@link Statement} created with the SQL above, the following code would set
 * parameter values to select the names of all 9 year old cats:
 * </p><pre>
 *   statement
 *     .bind(0, "Cat")
 *     .bind(1, 9);
 * </pre>
 *
 * @author  harayuanwang, michael-a-mcmahon
 * @since   0.1.0
 */
final class OracleStatementImpl implements Statement {

  /**
   * Placeholder in {@link #bindValues} for bind positions that have not been
   * set with a value. When a bind position is set with the {@code null} value,
   * {@code bindValues} stores {@code null} at that position.
   */
  private static final Object BIND_NOT_SET = new Object();

  /** A JDBC connection that executes this statement's {@link #sql}. */
  private final java.sql.Connection jdbcConnection;

  /** Adapts Oracle JDBC Driver APIs into Reactive Streams APIs */
  private final ReactiveJdbcAdapter adapter;

  /**
   * SQL Language command that this statement executes. The command is
   * provided by user code and may include parameter markers.
   */
  private final String sql;

  /**
   * Parameter names recognized in this statement's SQL. This list contains
   * {@code null} entries at the indexes of unnamed parameters.
   */
  private final List<String> parameterNames;

  /**
   * Collection of bind values that require asynchronous materialization, such
   * as a {@link io.r2dbc.spi.Blob}. Entries in this list are instances of
   * {@link DeferredBind} objects that initiate asynchronous materialization
   * when this statement is executed, and that release any resources
   * allocated for their materialization when the execution completes. This
   * collection is cleared when {@link #execute()} is invoked on this statement.
   */
  private Collection<DeferredBind> deferredBinds = new ArrayList<>();

  /**
   * The current set of bind values. This array stores {@link #BIND_NOT_SET} at
   * positions that have not been set with a value. Otherwise, it stores the
   * value that has been set at each position; The stored value may be
   * {@code null}.
   */
  private final Object[] bindValues;

  /**
   * The current batch of bind values. A copy of {@link #bindValues} is added
   * to this queue when {@link #add()} is invoked.
   */
  private Queue<Object[]> batch = new LinkedList<>();

  /**
   * Fetch size that has been provided to {@link #fetchSize(int)}.
   */
  private int fetchSize = 0;

  /**
   * A hint from user code providing the names of columns that the database
   * might generate a value for when this statement is executed. This array
   * is a copy of one provided to {@link #returnGeneratedValues(String...)},
   * or is {@code null} if user code has not specified generated values. If
   * this array has been specified, then it does not contain {@code null}
   * values. This array may be specified as a zero-length array to indicate
   * that the R2DBC driver should determine which column values are returned.
   */
  private String[] generatedColumns = null;

  /**
   * <p>
   * Constructs a new statement that executes {@code sql} using the specified
   * {@code adapter} and {@code jdbcConnection}.
   * </p><p>
   * The SQL string may be parameterized as described in the javadoc of
   * {@link SqlParameterParser}.
   * </p>
   * @param adapter Adapts JDBC calls into reactive streams.
   * @param jdbcConnection JDBC connection to an Oracle Database.
   * @param sql SQL Language statement that may include parameter markers.
   */
  OracleStatementImpl(ReactiveJdbcAdapter adapter,
                      java.sql.Connection jdbcConnection,
                      String sql) {
    this.adapter = adapter;
    this.jdbcConnection = jdbcConnection;
    this.sql = sql;

    this.parameterNames = SqlParameterParser.parse(sql);
    this.bindValues = new Object[parameterNames.size()];
    Arrays.fill(bindValues, BIND_NOT_SET);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by storing the bind {@code value} at the
   * specified {@code index} in {@link #bindValues}. A reference to the
   * {@code value} is retained until this statement is executed.
   * </p>
   */
  @Override
  public Statement bind(int index, Object value) {
    requireNonNull(value, "value is null");
    requireValidIndex(index);
    bindValues[index] = convertToJdbcBindValue(value);
    return this;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by setting the specified {@code value} as a
   * parameter on the JDBC PreparedStatement that this statement executes.
   * The JDBC PreparedStatement retains a reference to the {@code value}
   * until this statement is executed.
   * </p><p>
   * The Oracle R2DBC Driver only supports this method if the SQL used to
   * create this statement contains
   * <a href="#named_parameters">colon prefixed parameter names</a>.
   * </p><p>
   * Note that parameter names are <i>case sensitive</i>. See
   * {@link SqlParameterParser} for a full specification of the parameter name
   * syntax.
   * </p><p>
   * If the specified {@code identifier} matches more than one parameter name,
   * then this method binds the {@code value} to all parameters having a
   * matching name. For instance, when {@code 9} is bound to the parameter
   * named "x", the following SQL would return all names having a birthday on
   * the 9th day of the 9th month:
   * <pre>
   * SELECT name FROM birthday WHERE month=:x AND day=:x
   * </pre>
   * </p>
   * @throws IllegalArgumentException {@inheritDoc}
   * @throws IllegalArgumentException If the {@code identifier} does match a
   * case sensitive parameter name that appears in this {@code Statement's}
   * SQL command.
   * @throws IllegalArgumentException If the JDBC PreparedStatement does not
   * support conversions of the bind value's Java type into a SQL type.
   */
  @Override
  public Statement bind(String identifier, Object value) {
    requireNonNull(identifier, "identifier is null");
    requireNonNull(value, "value is null");
    bindNamedParameter(identifier, value);
    return this;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by setting the {@code null} value as a
   * parameter on the JDBC PreparedStatement that this statement executes. The
   * {@code null} value is specified to JDBC as the SQL
   * {@link java.sql.Types#NULL} type.
   * </p>
   */
  @Override
  public Statement bindNull(int index, Class<?> type) {
    requireNonNull(type, "type is null");
    requireValidIndex(index);
    bindValues[index] = null;
    return this;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by setting the {@code null} value as a
   * parameter on the JDBC PreparedStatement that this statement executes. The
   * {@code null} value is specified to JDBC as the SQL
   * {@link java.sql.Types#NULL} type.
   * </p><p>
   * The Oracle R2DBC Driver only supports this method if the SQL used to
   * create this statement contains
   * <a href="#named_parameters">colon prefixed parameter names</a>.
   * </p><p>
   * Note that parameter names are <i>case sensitive</i>. See
   * {@link SqlParameterParser} for a full specification of the parameter name
   * syntax.
   * </p><p>
   * If the specified {@code identifier} matches more than one parameter name
   * in this {@code Statement's} SQL command, this method binds the SQL
   * {@code NULL} value to the first matching parameter that appears when the
   * SQL command is read from left to right. (Note: It is not recommended to use
   * duplicate parameter names. Use {@link #bindNull(int, Class)} to set the
   * SQL {@code NULL} value for a duplicate parameter name at a given index).
   * </p>
   * </p><p>
   * If the specified {@code identifier} matches more than one parameter name,
   * then this method binds the SQL {@code NULL} value to all parameters
   * having a matching name. For instance, when {@code NULL} is bound to the
   * parameter named "x", the following SQL would create a birthday with
   * {@code NULL} values for month and day:
   * <pre>
   * INSERT INTO birthday (name, month, day) VALUES ('Plato', :x, :x)
   * </pre>
   * </p>
   * @throws IllegalArgumentException {@inheritDoc}
   * @throws IllegalArgumentException If the {@code identifier} does match a
   * case sensitive parameter name that appears in this {@code Statement's}
   * SQL command.
   */
  @Override
  public Statement bindNull(String identifier, Class<?> type) {
    requireNonNull(identifier, "identifier is null");
    requireNonNull(type, "type is null");
    bindNamedParameter(identifier, null);
    return this;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by adding the current set of bind values
   * to the batch of the JDBC PreparedStatement that this statement executes.
   * </p><p>
   * The Oracle R2DBC Driver only supports this method if this
   * {@code Statement} was created with a DML type SQL command. If this
   * method is invoked on a non-DML {@code Statement}, the publisher returned
   * by {@link #execute()} emits {@code onError} with an
   * {@code R2dbcException} indicating that the SQL is not a DML command.
   * </p>
   */
  @Override
  public Statement add() {
    requireAllParametersSet(bindValues);
    batch.add(bindValues.clone());
    Arrays.fill(bindValues, BIND_NOT_SET);
    return this;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by setting the {@link #generatedColumns}
   * that a JDBC {@link PreparedStatement} will be configured to return when
   * this statement is executed.
   * </p><p>
   * No reference to the {@code columns} array is retained after this method
   * returns.
   * </p>
   *
   * @implNote
   * <p><b>
   * Oracle R2DBC implements {@link Statement#execute()} to perform a blocking
   * database call if this method is invoked with a non-empty {@code String[]}.
   * </b></p><p>
   * This is a known limitation stemming from the implementation of
   * {@link Connection#prepareStatement(String, String[])} in the 21.1 Oracle
   * JDBC Driver. This limitation will be resolved in a later release;
   * The Oracle JDBC Team is aware of the issue and is working on a fix.
   * </p><p>
   * The blocking database call can be avoided by calling this method with an
   * empty {@code String[]}. In this case, the Oracle JDBC Driver will not
   * execute a blocking database call, however it will only return the
   * generated value for the ROWID pseudo column.
   * </p>
   */
  @Override
  public Statement returnGeneratedValues(String... columns) {
    requireNonNull(columns, "Column names are null");

    for (int i = 0; i < columns.length; i++) {
      if (columns[i] == null)
        throw new IllegalArgumentException("Null column name at index: " + i);
    }

    generatedColumns = columns.clone();
    return this;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by storing a number of rows to be set as a
   * JDBC statement's fetch size when this R2DBC statement is executed.
   * </p>
   */
  @Override
  public Statement fetchSize(int rows) {
    if (rows < 0) {
      throw new IllegalArgumentException(
        "Fetch size is less than zero: " + rows);
    }
    fetchSize = rows;
    return this;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning a publisher that publishes the
   * result of executing a JDBC PreparedStatement. If a batch of bind values
   * have been {@linkplain #add() added} to this statement, the returned
   * publisher emits a {@code Result} for each parameterized update in the
   * batch.
   * </p><p>
   * When this method returns, any bind values previously set on this
   * statement are cleared, and any sets of bind values saved with
   * {@link #add()} are also cleared.
   * </p><p>
   * If at least one set of bind values has been saved with {@link #add()},
   * and a new set of bind values has been set without calling {@link #add()},
   * then the new set of bind values are implicitly added when this method is
   * called. For example, the following code executes a statement that inserts
   * two rows even though {@code add()} is only called once:
   * </p><pre>
   * connection.createStatement(
   *   "INSERT INTO pets (species, age) VALUES (:species, :age)")
   *   .bind("species", "Cat").bind("age", 9).add()
   *   .bind("species", "Dog").bind("age", 10)
   *   .execute();
   * </pre><p>
   * The returned publisher initiates SQL execution <i>the first time</i> a
   * subscriber subscribes, before the subscriber emits a {@code request}
   * signal. The returned publisher does not support multiple subscribers. After
   * one subscriber has subscribed, the publisher signals {@code onError}
   * with {@code IllegalStateException} to all subsequent subscribers.
   * </p>
   *
   * @implNote
   * <p><b>
   * This method executes a blocking database call when generated column
   * names have been specified by invoking
   * {@link #returnGeneratedValues(String...)} with a non-empty
   * {@code String[]}.
   * </b></p><p>
   * This is a known limitation stemming from the implementation of
   * {@link Connection#prepareStatement(String, String[])} in the 21.1 Oracle
   * JDBC Driver. This limitation will be resolved in a later release;
   * The Oracle JDBC Team is aware of the issue and is working on a fix.
   * </p><p>
   * The blocking database call can be avoided by specifying an empty {@code
   * String[]} to {@link #returnGeneratedValues(String...)}. In this case, the
   * Oracle JDBC Driver will not execute a blocking database call, however it
   * will only return the generated value for the ROWID pseudo column.
   * </p>
   */
  @Override
  public Publisher<? extends Result> execute() {

    if (batch.isEmpty())
      add(); // Add a single set of binds to the batch, if all are set
    else
      addImplicit(); // Add the current set of binds, if any are set

    Publisher<Result> resultPublisher =
      createResultPublisher(batch, fetchSize, generatedColumns);
    batch = new LinkedList<>();

    Collection<DeferredBind> currentDeferredBinds = deferredBinds;
    deferredBinds = new ArrayList<>();

    AtomicBoolean isSubscribed = new AtomicBoolean(false);
    return Flux.defer(() -> {
      if (isSubscribed.compareAndSet(false, true)) {
        return materializeDeferredBinds(currentDeferredBinds)
          .thenMany(resultPublisher)
          .concatWith(
            discardDeferredBinds(currentDeferredBinds).cast(Result.class));
      }
      else {
        return Mono.error(new IllegalStateException(
          "Multiple subscribers are not supported by the Oracle R2DBC" +
            " Statement.execute() publisher"));
      }
    });
  }

  /**
   * <p>
   * Creates a {@code Publisher} that emits the {@code Result} of executing
   * this statement with one or more sets of bind parameters. If there is
   * more than one set of bind parameters, then the statement is executed as
   * a batch DML operation. The DML operation is executed for each set of
   * parameters, in the iteration order of {@code batch}.
   * </p><p>
   * The {@code Result} provides any generated values for columns specified
   * as {@code generatedColumns}, which may be null if no generated values are
   * requested, or may be a zero-length array if the JDBC driver determines
   * the set of returned columns.
   * </p><p>
   * If the {@code Result} fetches rows from a database cursor, then each
   * fetch requests an amount of rows specified as {@code fetchSize}. If
   * {@code fetchSize} is zero, then the JDBC driver determines the amount of
   * rows to request.
   * </p>
   *
   * @param batch One or more sets of bind values. Not null. Retained.
   * @param fetchSize Number of rows to request when fetching, or {@code 0}
   * to have the JDBC driver determine the fetch size.
   * @param generatedColumns Column names to return the generated values of,
   * or an empty array to have the JDBC driver determine the generated values
   * to return. May be null. Retained.
   * @return A {@code Publisher} of this {@code Statement's} result.
   *
   * @implNote
   * A JDBC driver may determine a fetch size based on demand
   * signalled with {@link org.reactivestreams.Subscription#request(long)}.
   * However, it is known that the 21c Oracle JDBC Driver does not implement
   * this. It determines a fixed fetch size based on
   * {@link PreparedStatement#setFetchSize(int)}.
   */
  private Publisher<Result> createResultPublisher(
    Queue<Object[]> batch, int fetchSize, String[] generatedColumns) {

    return Mono.from(
      adapter.publishPreparedStatement(sql, generatedColumns, jdbcConnection))
      .flatMapMany(preparedStatement -> {
        runOrHandleSQLException(() ->
          preparedStatement.setFetchSize(fetchSize));
        return batch.size() == 1
          ? generatedColumns == null
            ? executeSingle(preparedStatement, batch.remove())
            : executeGeneratingValues(preparedStatement, batch.remove())
          : executeBatch(preparedStatement, batch);
      });
  }

  /**
   * <p>
   * Executes a JDBC statement with a single, non-batched, set of parameters.
   * If the execution returns a {@link java.sql.ResultSet} then the
   * {@code jdbcStatement} is closed after the {@code ResultSet} is fully
   * consumed by {@link Result#map(BiFunction)}. Otherwise, if the execution
   * only produces an update count, then the {@code jdbcStatement} is closed
   * immediately.
   * </p><p>
   * The returned publisher initiates SQL execution <i>the first time</i> a
   * subscriber subscribes, before the subscriber emits a {@code request}
   * signal.
   * </p>
   * @param jdbcStatement A JDBC statement
   * @param bindValues A set of bind values
   * @return A publisher that emits the {@code Result} of executing the JDBC
   * statement.
   */
  private Publisher<Result> executeSingle(
    PreparedStatement jdbcStatement, Object[] bindValues) {
    setJdbcBindValues(bindValues, jdbcStatement);

    return Mono.from(adapter.publishSQLExecution(jdbcStatement))
      .map(isResultSet -> {
        if (isResultSet) {
          runOrHandleSQLException(jdbcStatement::closeOnCompletion);
          return OracleResultImpl.createQueryResult(
              adapter, getOrHandleSQLException(jdbcStatement::getResultSet));
        }
        else {
          int updateCount =
            getOrHandleSQLException(jdbcStatement::getUpdateCount);
          runOrHandleSQLException(jdbcStatement::close);
          return OracleResultImpl.createUpdateCountResult(updateCount);
        }
      });
  }

  /**
   * <p>
   * Executes a JDBC statement with a {@code batch} of bind values. The
   * {@code jdbcStatement} is closed immediately after the execution completes.
   * </p><p>
   * The returned publisher initiates SQL execution <i>the first time</i> a
   * subscriber subscribes, before the subscriber emits a {@code request}
   * signal.
   * </p>
   * @param jdbcStatement A JDBC statement
   * @param batch A batch of bind values
   * @return A publisher that emits the {@code Results} of executing the
   * batched JDBC statement.
   *
   * @implNote This method is implemented with the assumption that the JDBC
   * Driver does not support {@link PreparedStatement#getGeneratedKeys()} when
   * executing a batch DML operation. This is known to be true for the 21c
   * Oracle JDBC Driver. This method is not implemented to publish
   * {@code Results} with generated keys.
   */
  private Publisher<Result> executeBatch(
    PreparedStatement jdbcStatement, Queue<Object[]> batch) {

    // Add the batch of bind values to the JDBC statement
    while (! batch.isEmpty()) {
      Object[] batchValues = batch.remove();
      setJdbcBindValues(batchValues, jdbcStatement);
      runOrHandleSQLException(jdbcStatement::addBatch);
    }

    // Execute the batch. The execution won't return a ResultSet, so the JDBC
    // statement is closed immediately after the execution completes.
    runOrHandleSQLException(jdbcStatement::closeOnCompletion);
    return Flux.from(adapter.publishBatchUpdate(jdbcStatement))
      .map(updateCount ->
        OracleResultImpl.createUpdateCountResult(Math.toIntExact(updateCount)));
  }

  /**
   * <p>
   * Executes a key generating {@code jdbcStatement} for each set
   * of bind values in a {@code batch}. The {@code jdbcStatement} is closed
   * after all executions have completed. If any execution results in an
   * error, subsequent executions are skipped.
   * </p><p>
   * The returned {@code Publisher} emits a {@code Result} with an update
   * count and generated values for each execution of the {@code
   * jdbcStatement}.
   * </p>
   *
   * @implNote The 21c Oracle JDBC Driver does not support batch DML when
   * generated keys are requested, so this method can not invoke
   * {@link PreparedStatement#addBatch()},
   *
   * @param jdbcStatement A JDBC statement
   * @param bindValues A set of bind values
   * @return A publisher that emits the {@code Results} of executing the
   * JDBC statement for each set of bind values in the {@code batch}
   */
  private Publisher<Result> executeGeneratingValues(
    PreparedStatement jdbcStatement, Object[] bindValues) {

    setJdbcBindValues(bindValues, jdbcStatement);
    return Mono.from(adapter.publishSQLExecution(jdbcStatement))
      .flatMap(isResultSet -> {
        runOrHandleSQLException(jdbcStatement::closeOnCompletion);
        return isResultSet
          ? Mono.just(OracleResultImpl.createQueryResult(
              adapter,
              getOrHandleSQLException(jdbcStatement::getResultSet)))
          : Mono.from(OracleResultImpl.createGeneratedValuesResult(
              adapter,
              getOrHandleSQLException(jdbcStatement::getUpdateCount),
              getOrHandleSQLException(jdbcStatement::getGeneratedKeys)));
      });
  }

  /**
   * Adds the current set of {@link #bindValues} to the {@link #batch} if all
   * positions are set with a value. A full set of bind values are implicitly
   * {@link #add() added} when an R2DBC {@code Statement} is executed. This
   * behavior is specified in "Section 7.2.2. Batching" of the R2DBC 0.8.2
   * Specification. If this statement has no bind position, then this method
   * does nothing. If at least one bind position is set with a value, but not
   * all bind positions are set with a value, then this method throws an
   * {@code IllegalStateException}.
   *
   * @throws IllegalStateException If user code has provided a value for one
   * or more parameters, but not for all parameters.
   */
  private void addImplicit() {
    if (bindValues.length != 0 && bindValues[0] != BIND_NOT_SET) {
      add();
    }
    else {
      for (int i = 1; i < bindValues.length; i++) {
        if (bindValues[i] != BIND_NOT_SET) {
          throw new IllegalStateException(
            "One or more parameters are not set");
        }
      }
    }
  }

  /**
   * Checks that the specified 0-based {@code index} is within the range of
   * valid parameter indexes for this statement.
   * @param index A 0-based parameter index
   * @throws IndexOutOfBoundsException If the {@code index} is outside of the
   *   valid range.
   */
  private void requireValidIndex(int index) {
    if (index < 0) {
      throw new IndexOutOfBoundsException(
        "Parameter index is non-positive: " + index);
    }
    else if (index >= parameterNames.size()) {
      throw new IndexOutOfBoundsException(
        "Parameter index is out of range: " + index
          + ". Largest index is: " + (parameterNames.size() - 1));
    }
  }

  /**
   * Binds a {@code value} to all named parameters matching the specified
   * {@code name}. The match is case-sensitive.
   * @param name A parameter name. Not null.
   * @param value A value to bind. May be null.
   * @throws IllegalArgumentException if no named parameter matches the
   *   {@code identifier}
   */
  private void bindNamedParameter(String name, Object value) {
    boolean isMatched = false;

    for (int i = 0; i < parameterNames.size(); i++) {
      if (name.equals(parameterNames.get(i))) {
        isMatched = true;
        bindValues[i] = convertToJdbcBindValue(value);
      }
    }

    if (! isMatched) {
      throw new IllegalArgumentException(
        "Unrecognized parameter identifier: " + name);
    }
  }

  /**
   * <p>
   * Converts a {@code bindValue} of a type that is supported by R2DBC into an
   * equivalent type that is supported by JDBC. For instance, if this method
   * is called with an {@code io.r2dbc.spi.Blob} type {@code bindValue}, then
   * it will return an {@code java.sql.Blob} type object. The object returned
   * by this method will express the same information as the original {@code
   * bindValue}. If no conversion is necessary, this method simply returns the
   * original {@code bindValue}.
   * </p>
   *
   * @param bindValue Bind value to convert. May be null.
   * @return Value to set as a bind on the JDBC statement. May be null.
   * @throws IllegalArgumentException If the JDBC driver can not convert a
   *   bind value into a SQL value.
   */
  private Object convertToJdbcBindValue(Object bindValue) {
    if (bindValue == null) {
      return null;
    }
    else if (bindValue instanceof io.r2dbc.spi.Blob) {
      return convertBlobBind((io.r2dbc.spi.Blob) bindValue);
    }
    else if (bindValue instanceof io.r2dbc.spi.Clob) {
      return convertClobBind((io.r2dbc.spi.Clob) bindValue);
    }
    else if (bindValue instanceof ByteBuffer) {
      return convertByteBufferBind((ByteBuffer) bindValue);
    }
    else if (adapter.isSupportedBindType(bindValue.getClass())) {
      return bindValue;
    }
    else {
      throw new IllegalArgumentException(
        "Unsupported Java type: " + bindValue.getClass());
    }
  }

  /**
   * Sets bind {@code values} at parameter positions of a {@code jdbcStatement}.
   * The index of a value in the {@code values} values array determines the
   * parameter position it is set at. Bind values that store character
   * information, such as {@code String} or {@code Reader}, are set as NCHAR
   * SQL types so that the JDBC driver will represent the values with the
   * National (Unicode) character set of the database.
   *
   * @param values Values to bind. Not null.
   * @param jdbcStatement JDBC statement. Not null.
   */
  private static void setJdbcBindValues(
    Object[] values, PreparedStatement jdbcStatement) {
    runOrHandleSQLException(() -> {
      for (int i = 0; i < values.length; i++) {
        Object bindValue = values[i];

        if (bindValue == null)
          jdbcStatement.setNull(i + 1, Types.NULL);
        else if (bindValue instanceof String)
          jdbcStatement.setNString(i + 1, (String)bindValue);
        else if (bindValue instanceof Reader)
          jdbcStatement.setNCharacterStream(i + 1, (Reader)bindValue);
        else
          jdbcStatement.setObject(i + 1, bindValue);
      }
    });
  }

  /**
   * Checks that a bind value has been set for all parameters in the current
   * parameter set.
   * @throws R2dbcException if one or more parameters are not set.
   */
  private static void requireAllParametersSet(Object[] bindValues) {
    for (Object value : bindValues) {
      if (value == BIND_NOT_SET)
        throw new IllegalStateException("One or more parameters are not set");
    }
  }

  /**
   * <p>
   * Materializes a collection of {@code deferredBind} values. This is method
   * is used to create a processing stage in that bind values such as LOBs are
   * materialized asynchronously prior to a statement's execution.
   * </p><p>
   * The returned publisher materializes binds <i>after</i> a subscriber
   * subscribes, <i>before</i> the subscriber emits a {@code request}
   * signal. Multiple subscribers are supported, but the returned publisher
   * does not repeat the action of materializing binds for each subscription.
   * Any signals emitted to the first subscription are propagated to any
   * subsequent subscriptions.
   * </p>
   * @param deferredBinds Collection of binds to materialize. Not null.
   *                      Retained, but not modified.
   * @return A mono that terminates when all deferred binds are materialized.
   */
  private static Mono<Void> materializeDeferredBinds(
    Collection<DeferredBind> deferredBinds) {
    if (deferredBinds.isEmpty()) {
      return Mono.empty();
    }
    else {
      return Mono.defer(() -> {
          Mono<Void> allMaterialized = Mono.empty();
          for (DeferredBind deferredBind : deferredBinds)
            allMaterialized = allMaterialized.then(deferredBind.materialize());

          return allMaterialized;
        })
        .cache();
    }
  }

  /**
   * <p>
   * Releases any resources allocated for a collection of
   * {@code deferredBinds}. This is method is used to create a processing stage
   * in that database resources allocated for bind values, such as LOBs, are
   * asynchronously released after the execution of a statement.
   * </p><p>
   * The returned publisher releases resources <i>after</i> a subscriber
   * subscribes, <i>before</i> the subscriber emits a {@code request}
   * signal. Multiple subscribers are supported, but the returned publisher
   * does not repeat the action of releasing resources for each subscription.
   * Any signals emitted to the first subscription are propagated to any
   * subsequent subscriptions.
   * </p>
   * @param deferredBinds Collection of binds to discard. Not null. Retained,
   *                     but not modified.
   * @return A mono that terminates when all resources of deferred binds are
   *   released.
   */
  private static Mono<Void> discardDeferredBinds(
    Collection<DeferredBind> deferredBinds) {
    if (deferredBinds.isEmpty()) {
      return Mono.empty();
    }
    else {
      return Mono.defer(() -> {
          Mono<Void> allDiscarded = Mono.empty();
          for (DeferredBind deferredBind : deferredBinds)
            allDiscarded = allDiscarded.then(deferredBind.discard());

          return allDiscarded;
        })
        .cache();
    }
  }

  /**
   * Converts an R2DBC Blob to a JDBC Blob. This method adds a
   * {@link DeferredBind} to this statement's {@link #deferredBinds} collection.
   * The {@code DeferredBind} will materialize by asynchronously writing the
   * {@code r2dbcBlob's} content to the JDBC Blob returned by this method.
   * The JDBC Blob allocates a temporary database BLOB that is freed when the
   * {@code DeferredBind} is discarded with {@linkplain DeferredBind#discard()}.
   * @param r2dbcBlob An R2DBC Blob. Not null. Retained.
   * @return A JDBC Blob. Not null.
   */
  private java.sql.Blob convertBlobBind(io.r2dbc.spi.Blob r2dbcBlob) {
    java.sql.Blob jdbcBlob =
      getOrHandleSQLException(jdbcConnection::createBlob);

    deferredBinds.add(new DeferredBind() {
      @Override
      public Mono<Void> materialize() {
        return Mono.from(
          adapter.publishBlobWrite(r2dbcBlob.stream(), jdbcBlob));
      }

      @Override
      public Mono<Void> discard() {
        return Mono.from(adapter.publishBlobFree(jdbcBlob));
      }
    });

    return jdbcBlob;
  }

  /**
   * Converts an R2DBC Clob to a JDBC Clob. This method adds a
   * {@link DeferredBind} to this statement's {@link #deferredBinds} collection.
   * The {@code DeferredBind} will materialize by asynchronously writing the
   * {@code r2dbcClob's} content to the JDBC Clob returned by this method. The
   * JDBC Clob allocates a temporary database CLOB that is freed when the
   * {@code DeferredBind} is discarded with {@linkplain DeferredBind#discard()}.
   *
   * @param r2dbcClob An R2DBC Clob. Not null. Retained.
   * @return A JDBC Clob. Not null.
   *
   * @implNote The returned JDBC bind value is an instance of
   * {@link java.sql.NClob}. When character data is written to the NClob, the
   * Oracle JDBC Driver will encode it using the National Character Set of
   * the Oracle Database it is connected to. In 21c, the National Character Set
   * must be either UTF-8 or UTF-16, so the use of NClob will ensure that
   * unicode data is properly encoded by Oracle JDBC. Otherwise, if java.sql.
   * Clob were used, the Oracle JDBC Driver use the database's Default
   * Character set, which might not support unicode characters.
   */
  private java.sql.Clob convertClobBind(io.r2dbc.spi.Clob r2dbcClob) {
    // Always use NClob to support unicode characters
    java.sql.Clob jdbcClob =
      getOrHandleSQLException(jdbcConnection::createNClob);

    deferredBinds.add(new DeferredBind() {
      @Override
      public Mono<Void> materialize() {
        return Mono.from(
          adapter.publishClobWrite(r2dbcClob.stream(), jdbcClob));
      }

      @Override
      public Mono<Void> discard() {
        return Mono.from(adapter.publishClobFree(jdbcClob));
      }
    });

    return jdbcClob;
  }

  /**
   * Converts a ByteBuffer to a byte array. The {@code byteBuffer} contents,
   * delimited by it's position and limit, are copied into the returned byte
   * array. No state of the {@code byteBuffer} is mutated, including it's
   * position, limit, or mark.
   * @param byteBuffer A ByteBuffer. Not null. Not retained.
   * @return A byte array storing the {@code byteBuffer's} content. Not null.
   */
  private byte[] convertByteBufferBind(ByteBuffer byteBuffer) {
    ByteBuffer slice = byteBuffer.slice(); // Don't mutate position/limit/mark
    byte[] byteArray = new byte[slice.remaining()];
    slice.get(byteArray);
    return byteArray;
  }

  /**
   * <p>
   * Bind value that is not materialized until one or more database calls
   * have completed. The Oracle R2DBC Driver uses instances of this type to
   * defer execution of database calls. Database calls that materialize the
   * value are deferred until {@link Statement#execute()} is invoked.
   * </p><p>
   * Materialization of a deferred bind value is initiated by calling
   * {@link #materialize()}. A {@code DeferredBind} implements this method by
   * returning a Mono that completes when the bind value is materialized.
   * </p><p>
   * Any resources allocated during materialization are released by calling
   * {@link #discard()}. A {@code DeferredBind} implements this method by
   * returning a Mono that completes when allocated resources are released.
   * </p>
   */
  private interface DeferredBind {

    /**
     * Materializes this bind value.
     * @return A mono that terminates when the bind value is materialized.
     */
    Mono<Void> materialize();

    /**
     * Releases any resources allocated for this bind value.
     * @return A mono that terminates when resources are released.
     */
    Mono<Void> discard();
  }

}