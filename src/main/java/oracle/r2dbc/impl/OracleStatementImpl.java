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

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.Type;
import oracle.r2dbc.impl.OracleR2dbcExceptions.ThrowingSupplier;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;
import java.sql.Types;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static oracle.r2dbc.impl.OracleColumnMetadataImpl.r2dbcToSQLType;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.getOrHandleSQLException;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireOpenConnection;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.runOrHandleSQLException;
import static oracle.r2dbc.impl.OracleResultImpl.createCallResult;
import static oracle.r2dbc.impl.OracleResultImpl.createGeneratedValuesResult;
import static oracle.r2dbc.impl.OracleResultImpl.createQueryResult;
import static oracle.r2dbc.impl.OracleResultImpl.createUpdateCountResult;

/**
 * <p>
 * Implementation of the {@link Statement} SPI for the Oracle Database.
 * </p><p>
 * This implementation executes SQL using a {@link PreparedStatement}
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
 * </p><p id="fully-consumed-result">
 * To ensure that cursors are eventually closed, application code MUST
 * fully consume {@link Result} objects emitted by the {@link #execute()}
 * {@code Publisher}. A {@code Result} is fully consumed by subscribing to its
 * {@linkplain Result#getRowsUpdated() update count} or
 * {@linkplain Result#map(BiFunction) row data} {@code Publisher} and then
 * requesting items until the {@code Publisher} emits {@code onComplete/onError}
 * or its {@code Subscription} is cancelled.
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
 * Parameter values can be bound to the numeric zero-based index of a
 * {@code ?} marker, where the index corresponds to the position of the
 * marker within the sequence of all markers that appear when the
 * statement is read from left to right (ie: the ordinal index). In the example
 * above, the {@code species=?} marker appears first, so the bind index for
 * this parameter is {@code 0}. The {@code age=?} marker appears next, so the
 * bind index for that parameter is {@code 1}. Given a {@link Statement}
 * created with the SQL above, the following code would set parameter values
 * to select the names of all 9 year old cats:
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

  /** A JDBC connection that executes this statement's {@link #sql}. */
  private final Connection jdbcConnection;

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
   * The current set of bind values. This array stores {@code null} at
   * positions that have not been set with a value. All {@code Objects} input
   * to a {@code bind} method of this {@code Statement} are stored in this
   * array.
   */
  private final Parameter[] parameters;

  /**
   * The current batch of bind values. A copy of {@link #parameters} is added
   * to this queue when {@link #add()} is invoked.
   */
  private Queue<Parameter[]> batch = new LinkedList<>();

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
                      Connection jdbcConnection,
                      String sql) {
    this.adapter = adapter;
    this.jdbcConnection = jdbcConnection;
    this.sql = sql;
    this.parameterNames = SqlParameterParser.parse(sql);
    this.parameters = new Parameter[parameterNames.size()];
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by storing the bind {@code value} at the
   * specified {@code index} in {@link #parameters}. A reference to the
   * {@code value} is retained until this statement is executed.
   * </p>
   */
  @Override
  public Statement bind(int index, Object value) {
    requireOpenConnection(jdbcConnection);
    requireNonNull(value, "value is null");
    bindObject(index, value);
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
    requireOpenConnection(jdbcConnection);
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
    requireOpenConnection(jdbcConnection);
    requireNonNull(type, "type is null");
    bindObject(index, null);
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
    requireOpenConnection(jdbcConnection);
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
   * @throws IllegalStateException If one or more binds are out parameters
   */
  @Override
  public Statement add() {
    requireOpenConnection(jdbcConnection);
    addBatchParameters();
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
    requireOpenConnection(jdbcConnection);
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
    requireOpenConnection(jdbcConnection);
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
   * </p><p><b>
   * The 21.1 Oracle JDBC Driver does not determine a fetch size based on demand
   * signalled with {@link org.reactivestreams.Subscription#request(long)}.</b>
   * Oracle JDBC will use a fixed fetch size specified to
   * {@link #fetchSize(int)}. If no fetch size is specified, Oracle JDBC will
   * use a default fixed fetch size. A later release of Oracle JDBC may
   * implement dynamic fetch sizes that are adjusted to based on
   * {@code request} signals from the subscriber. When executing queries that
   * return a large number of rows, programmers are advised to set
   * {@link #fetchSize(int)} to configure the amount of rows that Oracle
   * JDBC should fetch and buffer.
   * </p>
   */
  @Override
  public Publisher<OracleResultImpl> execute() {
    requireOpenConnection(jdbcConnection);


    // Allow just one subscriber to the result publisher.
    AtomicBoolean isSubscribed = new AtomicBoolean(false);
    Publisher<OracleResultImpl> resultPublisher = createResultPublisher();
    return Flux.defer(() -> {
      if (isSubscribed.compareAndSet(false, true)) {
        return resultPublisher;
      }
      else {
        return Mono.error(new IllegalStateException(
          "Multiple subscribers are not supported by the Oracle R2DBC" +
            " Statement.execute() publisher"));
      }
    });
  }

  /**
   * Creates a {@code Publisher} that emits the {@code Result}(s) of
   * executing this {@code Statement}. The returned {@code Publisher}
   * retains a copy of mutable state needed to execute of this {@code
   * Statement} is copied and retained when this method is invoked.
   * (parameters,
   * fetch size, generated values,
   * etc) is cap
   * @return
   */
  private Publisher<OracleResultImpl> createResultPublisher() {
    if (! batch.isEmpty()) {
      return executeBatch(copyAndValidateBatch());
    }
    else {
      Parameter[] currentParameters = new Parameter[parameters.length];
      boolean isCall = copyAndValidateParameters(currentParameters);

      if (isCall) {
        return executeCall(currentParameters, fetchSize);
      }
      else if (generatedColumns != null) {
        return executeGeneratingValues(
          generatedColumns.clone(), currentParameters, fetchSize);
      }
      else {
        return executeSql(currentParameters, fetchSize);
      }
    }
  }

  private Queue<Parameter[]> copyAndValidateBatch() {
    if (generatedColumns != null) {
      throw new IllegalStateException(
        "Batch execution with generated values is not supported");
    }

    addImplicit();
    Queue<Parameter[]> currentBatch = batch;
    batch = new LinkedList<>();
    return currentBatch;
  }

  private boolean copyAndValidateParameters(Parameter[] copy) {
    boolean isCall = false;
    for (int i = 0; i < parameters.length; i++) {
      if (parameters[i] instanceof Parameter.Out) {
        if (generatedColumns != null) {
          throw new IllegalStateException(
            "Returning generated values is not supported with out parameters");
        }
        isCall = true;
      }
      else if (parameters[i] == null) {
        throw parameterNotSet();
      }

      copy[i] = parameters[i];
      parameters[i] = null;
    }

    return isCall;
  }

  /**
   * <p>
   * Executes this {@code Statement} with a single, non-batched, set of
   * {@code bindValues}. The returned {@code Publisher} emits a single {@code
   * Result} that may provide either row data, an update count, or neither.
   * </p><p>
   * This method copies any mutable state of this {@code Statement} that
   * effects the execution; The copied state is unaffected by any mutations
   * that occur after this method returns.
   * </p><p>
   * If the execution results in row data, the database cursor used to fetch that
   * data remains open until the {@code Result} is fully consumed. If the
   * result is not row data, or is an error, then the cursor is closed
   * immediately.
   * </p><p>
   * The returned publisher initiates SQL execution <i>the first time</i> a
   * subscriber subscribes, before the subscriber emits a {@code request}
   * signal.
   * </p>
   * @return A publisher that emits the {@code Result} of executing this
   * {@code Statement}
   */
  private Publisher<OracleResultImpl> executeSql(
    Parameter[] parameters, int fetchSize) {
    return execute(() -> jdbcConnection.prepareStatement(sql),
      (preparedStatement, discardQueue) ->
        bindInParameters(preparedStatement, parameters, discardQueue),
      preparedStatement -> publishSqlResult(preparedStatement, fetchSize));
  }

  /**
   * <p>
   * Executes this {@code Statement} with a single, non-batched, set of
   * {@code bindValues}. The returned {@code Publisher} emits a single {@code
   * Result} that may provide either row data, or an update count with
   * generated values, or neither.
   * </p><p>
   * This method copies any mutable state of this {@code Statement} that
   * effects the execution; The copied state is unaffected by any mutations
   * that occur after this method returns.
   * </p><p>
   * If the execution results in row data or generated values, the database
   * cursor used to fetch that data remains open until the {@code Result} is
   * fully consumed. If the result is not row data nor generated values, or is
   * an error, then the cursor is closed immediately.
   * </p><p>
   * The returned publisher initiates SQL execution <i>the first time</i> a
   * subscriber subscribes, before the subscriber emits a {@code request}
   * signal.
   * </p>
   * @return A publisher that emits the {@code Result} of executing this
   * {@code Statement}
   */
  private Publisher<OracleResultImpl> executeGeneratingValues(
    String[] generatedColumns, Parameter[] parameters, int fetchSize) {
    return execute(
      () -> generatedColumns.length == 0
        ? jdbcConnection.prepareStatement(sql, RETURN_GENERATED_KEYS)
        : jdbcConnection.prepareStatement(sql, generatedColumns),
      (preparedStatement, discardQueue) ->
        bindInParameters(preparedStatement, parameters, discardQueue),
      preparedStatement ->
        publishGeneratingValues(preparedStatement, fetchSize));
  }

  /**
   * <p>
   * Executes a procedural call using {@link CallableStatement}. The
   * returned {@code Publisher} emits one or more {@code Result} objects.
   * Out parameter values are emitted as the last {@code Result}, following any
   * cursors returned by the procedure with {@code DBMS_SQL.RETURN_RESULT} (ie:
   * Implicit Results). Implicit results are emitted in the same order as the
   * procedure returns them.
   * <p></p>
   *
   * first emits
   * any {@code
   * Result} at least one {@code Result} of
   * out
   * parameter bind value. that
   * implements {@link Result#map(BiFunction)} to emit a single
   * {@link Row} of the out parameters values. The returned {@code Publisher}
   * may emit additional {@code Result} objects, one for each cursor returned
   * using {@code DBMS_SQL.RETURN_RESULT}.
   *
   * The {@code CallableStatement} is closed after the returned
   * {@code Publisher} emits the last {@code Result}, and the last
   * {@code Result} is
   * <a href="OracleStatementImpl.html#fully-consumed-result">
   * fully consumed
   * </a>.
   *
   * Out parameter values may be
   * accessed by an index provided {@link #bind(int, Object)}
   * {@link Parameter.Out} object but
   * not both
   * (this is a
   * limitation
   * of JDBC). {@link Parameter.Out} objects
   * be
   * accessed by index or by name, but not both{@code
   * CallableStatement}
   * @return
   * Does
   * Oracle
   * JDBC or Oracle Database specify an order?
   * @throws IllegalStateException If {@link #returnGeneratedValues(String...)}
   *   has configured this {@code Statement} to returned generated values.
   *   Oracle JDBC does not support this.
   */
  Publisher<OracleResultImpl> executeCall(
    Parameter[] parameters, int fetchSize) {
    return execute(
      () -> jdbcConnection.prepareCall(sql),
      (callableStatement, discardQueue) ->
        Mono.from(bindInParameters(callableStatement, parameters, discardQueue))
          .doOnSuccess(nil ->
            bindOutParameters(callableStatement, parameters)),
      callableStatement ->
        publishCallResult(callableStatement, parameters, fetchSize));
  }

  Publisher<OracleResultImpl> executeBatch(Queue<Parameter[]> currentBatch) {
    return execute(
      () -> jdbcConnection.prepareStatement(sql),
      (preparedStatement, discardQueue) ->
        bindBatch(preparedStatement, currentBatch, discardQueue),
      preparedStatement -> publishBatchResult(preparedStatement));
  }

  private Publisher<OracleResultImpl> publishBatchResult(
    PreparedStatement preparedStatement) {
    return Flux.from(adapter.publishBatchUpdate(preparedStatement))
      .map(Math::toIntExact)
      .map(OracleResultImpl::createUpdateCountResult);
  }

  /**
   * <p>
   * Executes a JDBC statement using functional abstractions that allow for
   * various types of SQL execution, such as: queries, DML, DDL, batch DML,
   * DML returning generated values, or procedure calls. The functional
   * abstractions are:
   * <dl>
   *   <dt>{@code statementSupplier}</dt>
   *   <dd>
   *     Outputs a JDBC statement, which may be an instance of
   *     {@code PreparedStatement} or {@code CallableStatement}
   *   </dd>
   *   <dt>{@code bindFunction}</dt>
   *   <dd>
   *     Outputs a {@code Publisher} that emits {@code onComplete} after
   *     setting bind values on an input JDBC statement, and pushing
   *     {@code Publisher}s that deallocate bind values to an input {@code
   *     Queue}. Bind values do not typically require resource allocation and
   *     deallocation using {@code Publisher}s, so the function may output an
   *     empty {@code Publisher}, and push nothing to the {@code Queue}. But
   *     certain bind values, like {@code Blob} and {@code Clob}, will require
   *     asynchronous database calls for allocation and deallocation.
   *   </dd>
   *   <dt>
   *     {@code resultFunction}
   *   </dt>
   *   <dd>
   *     Outputs a {@code Publisher} that emits the {@code Result} of
   *     executing an input JDBC statement. The {@code Publisher} may emit 0,
   *     1, or many {@code Results}. If the {@code Publisher} emits 0 {@code
   *     Results}, the {@code Publisher} returned by this method emits a
   *     single {@code Result} having no update count or row data.
   *   </dd>
   * </dl>
   * </p><p>
   * This method ensures that the JDBC statement is eventually closed. If the
   * returned {@code Publisher} emits 1 or more {@code Result}s before
   * terminating, then the JDBC statement is closed when the last {@code Result}
   * emitted has been fully consumed. If the {@code Publisher} terminates before
   * emitting a single {@code Result}, then the JDBC statement is closed
   * immediately upon termination.
   * </p><p>
   * This method ensures that resources allocated for bind values are
   * deallocated after the {@code Publisher} output by the
   * {@code resultFunction} has terminated.
   * </p>
   * @param statementSupplier Prepares a JDBC statement
   * @param bindFunction Sets bind values on a JDBC statement
   * @param resultFunction Executes a JDBC statement
   * @param <T> The type of JDBC statement
   * @return A {@code Publisher} that executes a JDBC statement and emits the
   * {@code Result}s.
   */
  private static <T extends PreparedStatement> Publisher<OracleResultImpl> execute(
    ThrowingSupplier<T> statementSupplier,
    BiFunction<T, Queue<Publisher<Void>>, Publisher<Void>> bindFunction,
    Function<T, Publisher<OracleResultImpl>> resultFunction) {

    AtomicReference<OracleResultImpl> lastResultRef =
      new AtomicReference<>(null);

    return Flux.using(statementSupplier::get,

      preparedStatement ->
        Flux.usingWhen(
          Mono.just(new LinkedList<Publisher<Void>>()),
          discardQueue ->
            Flux.from(bindFunction.apply(preparedStatement, discardQueue))
              .thenMany(resultFunction.apply(preparedStatement))
              .doOnNext(lastResultRef::set),
          discardQueue ->
            Flux.fromIterable(discardQueue)
              .concatMapDelayError(Function.identity())),

      preparedStatement -> {
        OracleResultImpl lastResult = lastResultRef.get();
        if (lastResult != null) {
          Mono.from(lastResultRef.get().onConsumed())
            .doFinally(signalType ->
              runOrHandleSQLException(preparedStatement::close))
            .subscribe();
        }
        else {
          runOrHandleSQLException(preparedStatement::close);
        }
      })
      .defaultIfEmpty(createUpdateCountResult(-1));
  }

  /**
   * <p>
   * Allocates resources needed to execute this {@code Statement}. The
   * returned {@code Publisher} emits a single {@code Allocation} object with
   * {@link SqlResources#preparedStatement} generated by {@code
   * statementSupplier} and initialed using the current set of
   * {@link #parameters}.
   * </p><p>
   * The {@code PreparedStatement} does not return generated values or out
   * binds.
   * </p>
   * @return A {@code Publisher} that emits an {@code Allocation} of a
   * {@code PreparedStatement} with bind values allocated.
   */

  /**
   * <p>
   * Publishes one or more {@code Result}s returned by executing a
   * {@code preparedStatement}. This method handles results for queries, DML
   * (not batched, and not returning generated values), DDL, and procedure
   * calls having no out binds.
   * </p><p>
   * The returned {@code Publisher} emits 1 {@code Result} for each implicit
   * result identified by {@link PreparedStatement#getMoreResults()}. The
   * returned {@code Publisher} emits 0 {@code Result}s if the {@code
   * preparedStatement} returns no update count, row data, or implicit results.
   * </p>
   * @return A {@code Publisher} that emits the {@code Result}s of executing a
   * {@code preparedStatement}.
   */
  private Publisher<OracleResultImpl> publishSqlResult(
    PreparedStatement preparedStatement, int fetchSize) {

    runOrHandleSQLException(() -> preparedStatement.setFetchSize(fetchSize));

    return Mono.from(adapter.publishSQLExecution(preparedStatement))
      .flatMapMany(isResultSet -> {

        OracleResultImpl firstResult =
          getSqlResult(adapter, preparedStatement, isResultSet);

        if (firstResult != null) {
          return Mono.just(firstResult)
            .concatWith(Mono.from(firstResult.onConsumed())
              .thenMany(publishMoreResults(adapter, preparedStatement)));
        }
        else {
          return publishMoreResults(adapter, preparedStatement);
        }
      });
  }

  /**
   * <p>
   * Publishes implicit {@code Result}s of update counts or row data
   * indicated by {@link PreparedStatement#getMoreResults()}.
   * </p><p>
   * The returned {@code Publisher} terminates with {@code onComplete} after
   * {@code getMoreResults} and {@link PreparedStatement#getUpdateCount()}
   * indicate that all results have been published. The returned
   * {@code Publisher} may emit 0 {@code Results}.
   * </p><p>
   * The returned {@code Publisher} does not emit the next {@code Result}
   * until a previous {@code Result} has been fully consumed. The
   * {@link java.sql.ResultSet} of a previous {@code Result} is closed when
   * it has been fully consumed.
   * </p>
   * @param adapter Adapts JDBC calls into reactive streams.
   * @param preparedStatement JDBC statement
   * @return {@code Publisher} of implicit results.
   */
  static Publisher<OracleResultImpl> publishMoreResults(
    ReactiveJdbcAdapter adapter, PreparedStatement preparedStatement) {

    return Flux.defer(() -> {
      OracleResultImpl next =
        getSqlResult(adapter, preparedStatement,
          getOrHandleSQLException(preparedStatement::getMoreResults));

      if (next == null) {
        return Mono.empty();
      }
      else {
        return Mono.just(next)
          .concatWith(Mono.from(next.onConsumed())
            .thenMany(publishMoreResults(adapter,preparedStatement)));
      }
    });
  }

  /**
   * Returns the current {@code Result} of a {@code preparedStatement}, which
   * is row data if {@code isResultSet} is {@code true}, or an update count if
   * {@link PreparedStatement#getUpdateCount()} returns a value of 0 or greater.
   * This method returns {@code null} if current result is neither row data
   * nor a update count.
   * @param adapter Adapts JDBC calls into reactive streams.
   * @param preparedStatement JDBC statement
   * @param isResultSet {@code true} if the current result is row data,
   * otherwise false.
   * @return The current {@code Result} of the {@code preparedStatement}
   */
  private static OracleResultImpl getSqlResult(
    ReactiveJdbcAdapter adapter, PreparedStatement preparedStatement,
    boolean isResultSet) {
    return getOrHandleSQLException(() -> {
      if (isResultSet) {
        return OracleResultImpl.createQueryResult(
          adapter, preparedStatement.getResultSet());
      }
      else {
        int updateCount = preparedStatement.getUpdateCount();
        if (updateCount >= 0) {
          return OracleResultImpl.createUpdateCountResult(updateCount);
        }
        else {
          return null;
        }
      }
    });
  }

  private Publisher<OracleResultImpl> publishGeneratingValues(
    PreparedStatement preparedStatement, int fetchSize) {

    runOrHandleSQLException(() -> preparedStatement.setFetchSize(fetchSize));

    return Mono.from(adapter.publishSQLExecution(preparedStatement))
      .flatMap(isResultSet -> isResultSet
        ? Mono.just(createQueryResult(adapter,
            getOrHandleSQLException(preparedStatement::getResultSet)))
        : Mono.from(createGeneratedValuesResult(adapter,
            getOrHandleSQLException(preparedStatement::getUpdateCount),
            getOrHandleSQLException(preparedStatement::getGeneratedKeys))));
  }

  private Publisher<Void> bindBatch(
    PreparedStatement preparedStatement, Queue<Parameter[]> batch,
    Queue<Publisher<Void>> discardQueue) {
    return Flux.fromStream(Stream.generate(batch::poll)
      .takeWhile(Objects::nonNull))
      .concatMap(parameters ->
        Mono.from(bindInParameters(preparedStatement, parameters, discardQueue))
          .doOnSuccess(nil ->
            runOrHandleSQLException(preparedStatement::addBatch)));
  }

  /**
   * <p>
   * Binds in {@code parameters} to a {@code preparedStatement}, ignoring any
   * instances of {@code Parameter.Out}. The returned {@code Publisher}
   * completes after all values are materialized and bound to the {@code
   * preparedStatement}.
   * </p><p>
   * Resources allocated for bind values are deallocated by {@code Publisher}s
   * this method adds to the {@code discardQueue}.
   * </p>
   *
   * @param preparedStatement
   * @param parameters
   * @param discardQueue Resource deallocation queue
   * @return
   */
  private Publisher<Void> bindInParameters(
    PreparedStatement preparedStatement, Parameter[] parameters,
    Queue<Publisher<Void>> discardQueue) {

    Mono<Void> allocations = Mono.empty();

    for (int i = 0; i < parameters.length; i++) {

      Object value = parameters[i].getValue();
      Type r2dbcType = parameters[i].getType();

      int jdbcIndex = i + 1;
      SQLType jdbcType = r2dbcType != null
        ? r2dbcToSQLType(r2dbcType)
        : value == null
        ? JDBCType.NULL
        : OracleColumnMetadataImpl.javaToSQLType(value.getClass());

      if (parameters[i] instanceof Parameter.Out)
        continue;

      Object jdbcValue = convertToJdbcBindValue(value, discardQueue);
      if (jdbcValue instanceof Publisher<?>) {
        allocations = allocations.then(Mono.from((Publisher<?>)jdbcValue))
          .doOnSuccess(allocatedValue -> runOrHandleSQLException(() ->
            preparedStatement.setObject(jdbcIndex, allocatedValue, jdbcType)))
          .then();
      }
      else {
        runOrHandleSQLException(() ->
          preparedStatement.setObject(jdbcIndex, jdbcValue, jdbcType));
      }
    }

    return allocations;
  }

  /**
   * <p>
   * Converts a {@code bindValue} of a type that is supported by R2DBC into an
   * equivalent type that is supported by JDBC. For instance, if this method
   * is called with an {@code io.r2dbc.spi.Blob} type {@code bindValue}, it
   * will convert it into an {@code java.sql.Blob} type object. The object
   * returned by this method will express the same information as the
   * original {@code bindValue}. If no conversion is necessary, this method
   * simply returns the original {@code bindValue}. If the conversion
   * requires a database call, this method returns a {@code Publisher} that
   * emits the converted value. If the conversion requires resource
   * allocation, a {@code Publisher} that deallocates resources is added to
   * the {@code discardQueue}
   * </p>
   *
   * @param bindValue Bind value to convert. May be null.
   * @param discardQueue Resource deallocation queue
   * @return Value to set as a bind on the JDBC statement. May be null.
   * @throws IllegalArgumentException If the JDBC driver can not convert a
   *   bind value into a SQL value.
   */
  private Object convertToJdbcBindValue(
    Object bindValue, Queue<Publisher<Void>> discardQueue) {
    if (bindValue == null) {
      return null;
    }
    else if (bindValue instanceof io.r2dbc.spi.Blob) {
      return convertBlobBind((io.r2dbc.spi.Blob) bindValue, discardQueue);
    }
    else if (bindValue instanceof io.r2dbc.spi.Clob) {
      return convertClobBind((io.r2dbc.spi.Clob) bindValue, discardQueue);
    }
    else if (bindValue instanceof ByteBuffer) {
      return convertByteBufferBind((ByteBuffer) bindValue);
    }
    else {
      return bindValue;
    }
  }

  /**
   * Converts an R2DBC Blob to a JDBC Blob. The returned {@code Publisher}
   * asynchronously writes the {@code r2dbcBlob's} content to a JDBC Blob and
   * then emits the JDBC Blob after all content has been written. The JDBC 
   * Blob allocates a temporary database BLOB that is freed by a {@code
   * Publisher} added to the {@code discardQueue}.
   * @param r2dbcBlob An R2DBC Blob. Not null. Retained.
   * @return A JDBC Blob. Not null.
   */
  private Publisher<java.sql.Blob> convertBlobBind(
    io.r2dbc.spi.Blob r2dbcBlob, Queue<Publisher<Void>> discardQueue) {

    return Mono.using(() ->
        getOrHandleSQLException(jdbcConnection::createBlob),
      jdbcBlob -> {
        discardQueue.add(adapter.publishBlobFree(jdbcBlob));
        return Mono.from(adapter.publishBlobWrite(r2dbcBlob.stream(), jdbcBlob))
          .thenReturn(jdbcBlob);
      },
      jdbcBlob -> r2dbcBlob.discard());
  }
  
  /**
   * Converts an R2DBC Clob to a JDBC Clob. The returned {@code Publisher}
   * asynchronously writes the {@code r2dbcClob's} content to a JDBC Clob and
   * then emits the JDBC Clob after all content has been written. The JDBC 
   * Clob allocates a temporary database Clob that is freed by a {@code
   * Publisher} added to the {@code discardQueue}.
   * @param r2dbcClob An R2DBC Clob. Not null. Retained.
   * @return A JDBC Clob. Not null.
   */
  private Publisher<java.sql.Clob> convertClobBind(
    io.r2dbc.spi.Clob r2dbcClob, Queue<Publisher<Void>> discardQueue) {

    return Mono.using(() ->
        // Always use NClob to support unicode characters
        getOrHandleSQLException(jdbcConnection::createNClob),
      jdbcClob -> {
        discardQueue.add(adapter.publishClobFree(jdbcClob));
        return Mono.from(adapter.publishClobWrite(r2dbcClob.stream(), jdbcClob))
          .thenReturn(jdbcClob);
      },
      jdbcClob -> r2dbcClob.discard());
  }

  /**
   * Converts a ByteBuffer to a byte array. The {@code byteBuffer} contents,
   * delimited by it's position and limit, are copied into the returned byte
   * array. No state of the {@code byteBuffer} is mutated, including it's
   * position, limit, or mark.
   * @param byteBuffer A ByteBuffer. Not null. Not retained.
   * @return A byte array storing the {@code byteBuffer's} content. Not null.
   */
  private static byte[] convertByteBufferBind(ByteBuffer byteBuffer) {
    ByteBuffer slice = byteBuffer.slice(); // Don't mutate position/limit/mark
    byte[] byteArray = new byte[slice.remaining()];
    slice.get(byteArray);
    return byteArray;
  }

  /**
   * Binds in {@code parameters} to a {@code preparedStatement}, ignoring any
   * {@code Parameter} that is not an instance of {@code Parameter.Out}.  The
   * {@link SQLType} associated to the R2DBC {@code Type} of each out
   * parameter is registered with the {@code callableStatement}.
   * @param callableStatement JDBC statement having out parameters
   * @param parameters Parameters of this {@code Statement}
   */
  private static void bindOutParameters(
    CallableStatement callableStatement, Parameter[] parameters) {
    for (int i = 0; i < parameters.length; i++) {
      if (parameters[i] instanceof Parameter.Out) {
        int jdbcIndex = i + 1;
        SQLType jdbcType = r2dbcToSQLType(parameters[i].getType());
        runOrHandleSQLException(() ->
          callableStatement.registerOutParameter(jdbcIndex, jdbcType));
      }
    }
  }

  /**
   * Publishes any implicit results generated by {@code DBMS_SQL.RETURN_RESULT}
   * followed by a single {@code Result} of out parameters.
   * @param callableStatement JDBC statement having out parameters
   * @param parameters Parameters of this {@code Statement}
   * @return Publisher emitting implicit results and out parameters.
   */
  private Publisher<OracleResultImpl> publishCallResult(
    CallableStatement callableStatement, Parameter[] parameters,
    int fetchSize) {
    return Flux.from(publishSqlResult(callableStatement, fetchSize))
      .concatWith(Mono.just(createCallResult(createOutParameterRow(
          callableStatement, parameters))));
  }

  /**
   * <p>
   * Creates a {@code Row} accessing out parameter values of a
   * {@code callableStatement}.
   * </p><p>
   * Values may be accessed by their ordinal index relative to other out
   * parameters, such that the index of the first out parameter is 0, and the
   * indexes of the next out parameters are 1 greater than the index of the
   * previous out parameter. Where {@code i} is an in {@code Parameter} in
   * {@code parameters}, and {@code o} is an out or in-out {@code Parameter} in
   * {@code parameters}, and {...} -> {...} expresses a mapping from
   * {@code parameters} to the {@code callableStatement} index that is accessed
   * for each ordinal index of the {@code Row}:
   * </p><pre>
   * {i,i,i} -> {}
   * {o,i,i} -> {1}
   * {i,o,i} -> {2}
   * {o,o,i} -> {1,2}
   * {i,i,o} -> {3}
   * {o,i,o} -> {1,3}
   * {i,o,o} -> {2,3}
   * {o,o,o} -> {1,2,3}
   * </pre><p>
   * Values may be accessed by their parameter name, where the name is declared
   * using a colon-prefixed named parameter marker in this {@code Statement}'s
   * SQL. Values of unnamed parameter markers can not be accessed by name,
   * only by index.
   * </p><p>
   * {@code RowMetadata} provides the {@link Type} of each {@code Parameter},
   * and the name of each {@code Parameter} having named parameter marker.
   * For {@code Parameter}s having an unnamed parameter marker, the name
   * provided by {@code RowMetadata} is the ordinal index of that {@code
   * Parameter}.
   * </p>
   *
   *
   * @param callableStatement JDBC statement having out parameters
   * @param parameters Parameters of this {@code Statement}
   * @return A {@code Row} accessing the out parameters of {@code
   * callableStatement}.
   *
   * @implNote Oracle JDBC does not implement
   * {@link java.sql.ParameterMetaData}, so it can not be used to implement
   * {@code RowMetaData}.
   */
  private OracleRowImpl createOutParameterRow(
    CallableStatement callableStatement, Parameter[] parameters) {

    int[] outBindIndexes = IntStream.range(0, parameters.length)
      .filter(i -> parameters[i] instanceof Parameter.Out)
      .toArray();

    return new OracleRowImpl(
      new ReactiveJdbcAdapter.JdbcRow() {

        @Override
        public <T> T getObject(int index, Class<T> type) {
          // TODO Check supported type? or does OracleRowImpl do it still?
          return getOrHandleSQLException(() ->
            callableStatement.getObject(1 + outBindIndexes[index], type));
        }

        @Override
        public ReactiveJdbcAdapter.JdbcRow copy() {
          throw new UnsupportedOperationException();
        }
      },
      new OracleRowMetadataImpl(IntStream.range(0, outBindIndexes.length)
        .mapToObj(i -> OracleColumnMetadataImpl.create(
          Objects.requireNonNullElse(
            parameterNames.get(outBindIndexes[i]), String.valueOf(i)),
          parameters[outBindIndexes[i]].getType()))
        .toArray(OracleColumnMetadataImpl[]::new)),
      adapter);
  }

  /**
   * Returns a copy of the {@link #parameters} array, and sets all positions
   * of the original array to {@code null}.
   * @return A copy of the {@code parameters} array
   */
  private Parameter[] getAndResetParameters() {
    requireAllParametersSet(parameters);
    Parameter[] copy = parameters.clone();
    Arrays.fill(parameters, null);
    return copy;
  }

  private static boolean isOutParameterPresent(Parameter[] binds) {
    for (Parameter bind : binds) {
      if (bind instanceof Parameter.Out)
        return true;
    }
    return false;
  }

  /**
   * Adds the current set of {@link #parameters} to the {@link #batch} if all
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
    if (parameters.length != 0 && parameters[0] != null) {
      add();
    }
    else {
      for (int i = 1; i < parameters.length; i++) {
        if (parameters[i] != null) {
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
    if (parameterNames.isEmpty()) {
      throw new IndexOutOfBoundsException(
        "Statement has no parameter markers");
    }
    else if (index < 0) {
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
        bindObject(i, value);
      }
    }

    if (! isMatched) {
      throw new IllegalArgumentException(
        "Unrecognized parameter identifier: " + name);
    }
  }

  /**
   * Checks that a bind value has been set for all parameters in the current
   * parameter set.
   * @throws R2dbcException if one or more parameters are not set.
   */
  private static void requireAllParametersSet(Parameter[] binds) {
    for (Parameter bind : binds) {
      if (bind == null)
        throw parameterNotSet();
    }
  }

  /**
   * Adds the current set of {@link #parameters} to the {@link #batch}, and
   * then resets the {@code parameters} array to store {@code null} at all
   * positions.
   * @throws IllegalStateException If a parameter has not been set
   * @throws IllegalStateException If an out parameter has been set
   */
  private void addBatchParameters() {
    for (Parameter parameter : parameters) {
      if (parameter == null) {
        throw parameterNotSet();
      }
      else if (parameter instanceof Parameter.Out) {
        throw new IllegalStateException(
          "Batch execution with out binds is not supported");
      }
    }
    batch.add(parameters.clone());
    Arrays.fill(parameters, null);
  }

  /**
   * Returns an exception indicating that a parameter has not been set.
   * @return Unset parameter exception
   */
  private static IllegalStateException parameterNotSet() {
    return new IllegalStateException("One or more parameters are not set");
  }

  /**
   * Binds an {@code object}, retaining it as a {@link Parameter} in
   * {@link #parameters} at the specified {@code index}. If the {@code object}
   * is not an instance of {@code Parameter}, a {@code Parameter} having the
   * {@code object} as it's value is retained in {@link #parameters}.
   *
   * @param object Bind value to retain. May be null.
   * @throws IllegalArgumentException If {@code index} is not valid
   * @throws IllegalArgumentException If the class of {@code object} is not
   * supported as a bind value.
   * @throws IllegalArgumentException If {@code object} is a {@code Parameter},
   * and the class of the value is not supported as a bind value.
   * @throws IllegalArgumentException If {@code object} is a {@code Parameter},
   * and the SQL type is not supported as a bind value.
   */
  private void bindObject(int index, Object object) {

    requireValidIndex(index);

    if (object == null) {
      // TODO: OracleR2dbcType
      parameters[index] = Parameters.in(R2dbcType.VARCHAR);
    }
    else if (object instanceof Parameter) {
      // TODO: OracleR2dbcType {SQLType sqlType; Type r2dbcType;}
      Type type = ((Parameter)object).getType();
      if (type != null && null == r2dbcToSQLType(type))
        throw new IllegalArgumentException("TEMPORARY");
      else {
        Object value = ((Parameter) object).getValue();
        if (value != null && null == OracleColumnMetadataImpl.javaToSQLType(value.getClass()))
          throw new IllegalArgumentException("TEMPORARY");
      }

      parameters[index] =(Parameter)object;
    }
    else {
      if (null == OracleColumnMetadataImpl.javaToSQLType(object.getClass()))
        throw new IllegalArgumentException("TEMPORARY");

      parameters[index] = Parameters.in(object);
    }
  }

  /**
   * Materializes an R2DBC {@code Blob} by creating a JDBC {@code Blob} and
   * writing the content of the R2DBC {@code Blob} to the JDBC {@code Blob}.
   * The returned {@code Publisher} emits a single JDBC {@code Blob} after
   * the R2DBC {@code Blob} has been written to it. The R2DBC and JDBC
   * {@code Blob}s are discarded and freed if the returned {@code Publisher}
   * terminates with {@code onError} or is cancelled.
   * @param r2dbcBlob Blob to materialize
   * @return {@code Publisher} emitting a materialized JDBC {@code Blob}
   */
  private Publisher<java.sql.Blob> materializeBlob(
    io.r2dbc.spi.Blob r2dbcBlob) {
    return Mono.usingWhen(Mono.fromSupplier(() ->
        getOrHandleSQLException(jdbcConnection::createBlob)),
      jdbcBlob ->
        Mono.from(adapter.publishBlobWrite(r2dbcBlob.stream(), jdbcBlob))
          .thenReturn(jdbcBlob),
      jdbcBlob -> r2dbcBlob.discard(),
      (jdbcBlob, error) ->
        Mono.from(r2dbcBlob.discard())
          .then(Mono.from(adapter.publishBlobFree(jdbcBlob))),
      jdbcBlob ->
        Mono.from(r2dbcBlob.discard())
          .then(Mono.from(adapter.publishBlobFree(jdbcBlob))));
  }

  /**
   * Materializes an R2DBC {@code Clob} by creating a JDBC {@code Clob} and
   * writing the content of the R2DBC {@code Clob} to the JDBC {@code Clob}.
   * The returned {@code Publisher} emits a single JDBC {@code Clob} after
   * the R2DBC {@code Clob} has been written to it. The R2DBC and JDBC
   * {@code Clob}s are discarded and freed if the returned {@code Publisher}
   * terminates with {@code onError} or is cancelled.
   * @param r2dbcClob Clob to materialize
   * @return {@code Publisher} emitting a materialized JDBC {@code Clob}
   */
  private Publisher<java.sql.Clob> materializeClob(
    io.r2dbc.spi.Clob r2dbcClob) {
    return Mono.usingWhen(Mono.fromSupplier(() ->
      // TODO: Use NCLOB if an N* type has been specified, otherwise use default
      // Use NClob to support unicode characters
      getOrHandleSQLException(jdbcConnection::createNClob)),
      jdbcClob ->
        Mono.from(adapter.publishClobWrite(r2dbcClob.stream(), jdbcClob))
          .thenReturn(jdbcClob),
      jdbcClob -> r2dbcClob.discard(),
      (jdbcClob, error) ->
          Mono.from(r2dbcClob.discard())
            .then(Mono.from(adapter.publishClobFree(jdbcClob))),
      jdbcClob ->
        Mono.from(r2dbcClob.discard())
          .then(Mono.from(adapter.publishClobFree(jdbcClob))));
  }


}
