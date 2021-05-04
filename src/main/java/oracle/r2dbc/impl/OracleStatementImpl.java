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

import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static oracle.r2dbc.impl.OracleColumnMetadataImpl.javaToSQLType;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.getOrHandleSQLException;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireOpenConnection;
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

  // TODO: Document these default mappings
  // TODO: Add entry for array -> JDBCType.ARRAY

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
   * The current set of bind values. This array stores {@code null} at
   * positions that have not been set with a value.
   */
  private final Bind[] binds;

  /**
   * The current batch of bind values. A copy of {@link #binds} is added
   * to this queue when {@link #add()} is invoked.
   */
  private Queue<Bind[]> batch = new LinkedList<>();

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
    this.binds = new Bind[parameterNames.size()];
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by storing the bind {@code value} at the
   * specified {@code index} in {@link #binds}. A reference to the
   * {@code value} is retained until this statement is executed.
   * </p>
   */
  @Override
  public Statement bind(int index, Object value) {
    requireOpenConnection(jdbcConnection);
    requireNonNull(value, "value is null");
    requireValidIndex(index);
    binds[index] = bindObject(value);
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
    requireValidIndex(index);
    binds[index] = bindObject(null);
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
   */
  @Override
  public Statement add() {
    requireOpenConnection(jdbcConnection);
    validateBatchBinds(binds);
    batch.add(binds.clone());
    Arrays.fill(binds, null);
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
   * </p>
   */
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
  @Override
  public Publisher<? extends Result> execute() {
    requireOpenConnection(jdbcConnection);

    final Publisher<Result> resultPublisher;
    if (!batch.isEmpty())
      resultPublisher = executeBatch();
    else if (isOutBindPresent(binds))
      resultPublisher = executeCall();
    else if (generatedColumns != null)
      resultPublisher = executeGeneratingValues();
    else
      resultPublisher = executeSingle();

    AtomicBoolean isSubscribed = new AtomicBoolean(false);
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

  Publisher<Result> executeBatch() {

    if (generatedColumns != null) {
      throw new IllegalStateException(
        "Batch execution with generated values is not supported");
    }

    addImplicit();
    Queue<Bind[]> currentBatch = batch;
    batch = new LinkedList<>();

    return Flux.usingWhen(
      prepareBatch(jdbcConnection, sql, currentBatch),
      adapter::publishBatchUpdate,
      preparedStatement -> {
        runOrHandleSQLException(preparedStatement::close);
        return discardBatchBinds(currentBatch);
      })
      .map(Math::toIntExact)
      .map(OracleResultImpl::createUpdateCountResult);
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
  Publisher<Result> executeCall() {

    if (generatedColumns != null) {
      throw new IllegalStateException(
        "Call execution with generated values is not supported");
    }

    requireAllParametersSet(binds);
    Bind[] currentBinds = binds.clone();
    Arrays.fill(binds, null);

    return Flux.usingWhen(
      prepareCall(jdbcConnection, sql, currentBinds),
      callableStatement ->
        Mono.from(adapter.publishSQLExecution(callableStatement))
          .map(isRowDataResult ->
            // TODO: Implicit ResultSets
            OracleResultImpl.createCallResult(adapter, callableStatement,
              createOutParameterRow(callableStatement, currentBinds))),
      // onComplete: The CallableStatement is closed when the Result is consumed
      callableStatement -> discardBinds(currentBinds),
      // onError: The CallableStatement is closed since there is no Result to
      // consume
      (callableStatement, error) -> {
        runOrHandleSQLException(callableStatement::close);
        return discardBinds(currentBinds);
      },
      // cancel: The CallableStatement is closed in case the Subscription is
      // cancelled before the Result is emitted
      callableStatement -> {
        runOrHandleSQLException(callableStatement::close);
        return discardBinds(currentBinds);
      });
  }

  private static Publisher<CallableStatement> prepareCall(
    java.sql.Connection jdbcConnection, String sql, Bind[] binds) {
    return Mono.defer(() -> getOrHandleSQLException(() -> {
      CallableStatement callableStatement = jdbcConnection.prepareCall(sql);
      return Mono.from(setBinds(callableStatement, binds))
        .thenReturn(callableStatement);
    }));
  }

  /**
   * <p>
   * Generates {@code ColumnMetadata} for all out parameters. The length of
   * the returned array is equal to the number {@link Parameter.Out} objects
   * passed to a {@code bind} method of this {@code Statement}. The array is
   * ordered by the ordinal index of each out bind.
   * </p><p>
   * The returned metadata is generated locally based solely on user defined
   * inputs, and is not obtained from the database or JDBC.
   * </p><p>
   * The metadata supplies the SQL type specified by {@link Parameter#getType()}
   * for each {@link Parameter.Out} passed to a {@code bind} method of this
   * {@code Statement}. The metadata supplies the name only if the parameter
   * marker for
   * for
   * each out
   * specified by
   * </p>
   * {@link Parameter#getType()}
   * provides the SQL type of all out parameters
   *
   * @return
   */
  private OracleRowMetadataImpl createOutParameterMetadata() {
    if (binds.length == 0) {
      return new OracleRowMetadataImpl(new OracleColumnMetadataImpl[0]);
    }
    else {
      return new OracleRowMetadataImpl(IntStream.range(0, binds.length)
        .filter(i -> binds[i].isOutBind())
        .mapToObj(this::createParameterMetadata)
        .toArray(OracleColumnMetadataImpl[]::new));
    }
  }

  /**
   * Creates a {@code Row} of out parameter values. Parameter values may be
   * accessed by their ordinal index relative to other out parameters.
   * {i, i, i} = {}
   * {o, i, i} = {(0,1)}
   * {i, o, i} = {(0,2}}
   * {i, i, o} = {(0,3)}
   * {o, o, i} = {(0,1},(1,2)}
   * {i, o, o} = {(0,2),(1,3)}
   * {i, o, i} = {(0,1},(1,2)}
   * {o, o, o} = {(0,1},(1,2),(2,3)}
   * {o, o, o} = {0, 1, 2}
   * {i, i, o} = {(0,3)}
   * @return
   */
  private OracleRowImpl createOutParameterRow(
    CallableStatement callableStatement, Bind[] binds) {
    int[] outBindIndexes = IntStream.range(0, binds.length)
      .filter(i -> binds[i].isOutBind())
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
      new OracleRowMetadataImpl(Arrays.stream(outBindIndexes)
        .mapToObj(i ->  OracleColumnMetadataImpl.create(
          Objects.requireNonNullElse(parameterNames.get(i), ""),
          binds[i].sqlType()))
        .toArray(OracleColumnMetadataImpl[]::new)),
      adapter);
  }

  private OracleColumnMetadataImpl createParameterMetadata(int bindIndex) {
    String name = parameterNames.get(bindIndex);
    return OracleColumnMetadataImpl.create(
      name == null ? String.valueOf(bindIndex) : name,
      binds[bindIndex].sqlType());
  }

  private static Publisher<Void> discardBinds(Bind[] binds) {
    return Flux.fromArray(binds)
      .concatMapDelayError(Bind::discard);
  }

  /**
   * Discards all {@code BindValue}s enqueued in a {@code batch}. The
   * returned {@code Publisher} dequeues {@code BindValue}s from the
   * {@code batch} until the {@code Queue} is empty. If an error results from
   * discarding a {@code BindValue}, the returned {@code Publisher} continues
   * dequeing and discarding any remaining {@code BindValues}, and then
   * terminates with {@code onError}.
   * @param batch Batch of enqueued {@code bindValues}
   * @return {@code Publisher} that terminates when all {@code BindValue}s are
   * dequeued and discarded.
   */
  private static Publisher<Void> discardBatchBinds(Queue<Bind[]> batch) {
    return Flux.fromIterable(() ->
      new Iterator<Bind[]>() {
        @Override
        public boolean hasNext() {
          return ! batch.isEmpty();
        }

        @Override
        public Bind[] next() {
          return batch.remove();
        }
      })
      .concatMapDelayError(OracleStatementImpl::discardBinds);
  }

  private static boolean isOutBindPresent(Bind[] binds) {
    for (Bind bind : binds) {
      if (bind != null && bind.isOutBind())
        return true;
    }
    return false;
  }


  /**
   * Has Generated Values ->
   *   Has OUT Bind -> Error
   *   Has Batch -> Error
   *   Has Empty Generated Values -> PreparedStatement(RETURN_GENERATED_KEYS)
   *   Has Non-Empty Generated Values -> PreparedStatement(Sting[] keys)
   * Has OUT Bind ->
   *   Has Batch -> Error
   *   No Batch -> CallableStatement
   * No OUT Bind ->
   *   Batch -> PreparedStatement(addBatch)
   *   No Batch -> PreparedStatement
   *
   * @return
   */
  PreparedStatement prepareJdbcStatement() {
    return null;
  }

  /**
   * <p>
   * Executes a JDBC statement with a single, non-batched, set of {@code
   * bindValues}. If the execution results in a {@link java.sql.ResultSet} then the
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
   * @param binds A set of bind values
   * @return A publisher that emits the {@code Result} of executing the JDBC
   * statement.
   */
  private Publisher<Result> executeSingle() {

    requireAllParametersSet(binds);
    Bind[] currentBinds = binds.clone();
    Arrays.fill(binds, null);

    return Mono.usingWhen(
      prepareStatement(jdbcConnection, sql, currentBinds),
      preparedStatement ->
        Mono.from(adapter.publishSQLExecution(preparedStatement))
          .map(isResultSet -> {
            if (isResultSet) {
              return OracleResultImpl.createQueryResult(
                adapter, getOrHandleSQLException(preparedStatement::getResultSet));
            }
            else {
              int updateCount =
                getOrHandleSQLException(preparedStatement::getUpdateCount);
              runOrHandleSQLException(preparedStatement::close);
              return OracleResultImpl.createUpdateCountResult(updateCount);
            }
          }),
      preparedStatement -> // onComplete:
        // The PreparedStatement is closed when the Result is consumed
        discardBinds(currentBinds),
      (preparedStatement, error) -> { // onError:
        // The PreparedStatement is closed since there is no Result to consume
        runOrHandleSQLException(preparedStatement::close);
        return discardBinds(currentBinds);
      },
      preparedStatement -> { // cancel:
        // The PreparedStatement is closed in case the Result is not emitted
        runOrHandleSQLException(preparedStatement::close);
        return discardBinds(currentBinds);
      });
  }

  interface BindValue {
    SQLType type();
    Object value();
    Publisher<Void> discard();
  }

  private static Publisher<BindValue[]> materializeBinds(Bind[] binds) {
    return null;
  }

  private static Publisher<PreparedStatement> prepareStatement(
    java.sql.Connection connection, String sql, Bind[] binds) {
    return Mono.defer(() -> {
      PreparedStatement preparedStatement =
        getOrHandleSQLException(() -> connection.prepareStatement(sql));
      return Mono.from(setBinds(preparedStatement, binds))
        .thenReturn(preparedStatement);
    });
  }

  private static Publisher<PreparedStatement> prepareBatch(
    java.sql.Connection connection, String sql, Queue<Bind[]> bindsBatch) {

    return Mono.defer(() -> {
      PreparedStatement preparedStatement =
        getOrHandleSQLException(() -> connection.prepareStatement(sql));

      return Flux.fromIterable(bindsBatch)
        .concatMap(bindValues ->
          Mono.from(setBinds(preparedStatement, bindValues))
            .doOnSuccess(nil ->
              runOrHandleSQLException(preparedStatement::addBatch)))
        .then(Mono.just(preparedStatement));
    });
  }

  private static Publisher<PreparedStatement> prepareGeneratingValues(
    java.sql.Connection connection, String sql, Bind[] binds,
    String[] generatedColumns) {
    return Mono.defer(() -> {
      PreparedStatement preparedStatement = getOrHandleSQLException(() ->
        generatedColumns.length == 0
          ? connection.prepareStatement(sql, RETURN_GENERATED_KEYS)
          : connection.prepareStatement(sql, generatedColumns));
      return Mono.from(setBinds(preparedStatement, binds))
        .thenReturn(preparedStatement);
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
    CallableStatement jdbcStatement, Queue<Bind[]> batch) {

    // Add the batch of bind values to the JDBC statement
    Mono<Void> last = Mono.empty();
    while (! batch.isEmpty()) {
      Bind[] batchValues = batch.remove();
      last = last.then(Mono.from(setBinds(jdbcStatement, batchValues)))
        .doOnSuccess(nil ->
          runOrHandleSQLException(jdbcStatement::addBatch));
    }

    // Execute the batch. The execution won't return a ResultSet, so the JDBC
    // statement is closed immediately after the execution completes.
    return last.thenMany(adapter.publishBatchUpdate(jdbcStatement))
      .map(updateCount ->
        OracleResultImpl.createUpdateCountResult(Math.toIntExact(updateCount)))
      .doFinally(signalType -> runOrHandleSQLException(jdbcStatement::close));
  }

  /**
   * <p>
   * Executes a JDBC statement with a single, non-batched, set of {@code
   * bindValues}. If the execution results in a {@link java.sql.ResultSet} then
   * the {@code jdbcStatement} is closed after the {@code ResultSet} is fully
   * consumed by {@link Result#map(BiFunction)}. If the execution results in
   * an update count and/or values generated by DML, then the {@code
   * jdbcStatement} is closed after the returned {@code Publisher} emits a
   * {@code Result} with all generated values cached, such that
   * {@link Result#map(BiFunction)} may be called after the database connection
   * has been closed.
   * </p><p>
   * The returned publisher initiates SQL execution <i>the first time</i> a
   * subscriber subscribes, before the subscriber emits a {@code request}
   * signal.
   * </p>
   *
   * @implNote The 21c Oracle JDBC Driver does not support batch DML when
   * generated keys are requested, so this method can not invoke
   * {@link PreparedStatement#addBatch()},
   *
   * @param jdbcStatement A JDBC statement
   * @param binds A set of bind values
   * @return A publisher that emits the {@code Result} of executing the
   * JDBC statement.
   */
  private Publisher<Result> executeGeneratingValues() {

    requireAllParametersSet(binds);
    Bind[] currentBinds = binds.clone();
    Arrays.fill(binds, null);

    return Flux.usingWhen(
      prepareGeneratingValues(
        jdbcConnection, sql, currentBinds, generatedColumns.clone()),
      preparedStatement ->
        Mono.from(adapter.publishSQLExecution(preparedStatement))
          .flatMap(isResultSet -> isResultSet
            ? Mono.just(OracleResultImpl.createQueryResult(
                adapter,
                getOrHandleSQLException(preparedStatement::getResultSet)))
            : Mono.from(OracleResultImpl.createGeneratedValuesResult(
                adapter,
                getOrHandleSQLException(preparedStatement::getUpdateCount),
                getOrHandleSQLException(preparedStatement::getGeneratedKeys)))),
      // onComplete
      preparedStatement -> discardBinds(currentBinds),
      // onError
      (preparedStatement, error) -> {
        runOrHandleSQLException(preparedStatement::close);
        return discardBinds(currentBinds);
      },
      // cancel
      preparedStatement -> {
        runOrHandleSQLException(preparedStatement::close);
        return discardBinds(currentBinds);
      });
  }

  /**
   * Adds the current set of {@link #binds} to the {@link #batch} if all
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
    if (binds.length != 0 && binds[0] != null) {
      add();
    }
    else {
      for (int i = 1; i < binds.length; i++) {
        if (binds[i] != null) {
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
        binds[i] = bindObject(value);
      }
    }

    if (! isMatched) {
      throw new IllegalArgumentException(
        "Unrecognized parameter identifier: " + name);
    }
  }


  /**
   * Sets bind {@code values} at parameter positions of a {@code jdbcStatement}.
   * The index of a value in the {@code values} values array determines the
   * parameter position it is set at. Bind values that store character
   * information, such as {@code String} or {@code Reader}, are set as NCHAR
   * SQL types so that the JDBC driver will represent the values with the
   * National (Unicode) character set of the database.
   *  @param values Values to bind. Not null.
   * @param jdbcStatement JDBC statement. Not null.
   */
  private static Publisher<Void> setBinds(
    PreparedStatement jdbcStatement, Bind[] binds) {

    Mono<Void> last = Mono.empty();
    for (int i = 0; i < binds.length; i++)
      last = last.then(Mono.from(binds[i].setValue(jdbcStatement, i + 1)));

    return last;
  }

  /**
   * Checks that a bind value has been set for all parameters in the current
   * parameter set.
   * @throws R2dbcException if one or more parameters are not set.
   */
  private static void requireAllParametersSet(Bind[] binds) {
    for (Bind bind : binds) {
      if (bind == null)
        throw parameterNotSet();
    }
  }

  private static void validateBatchBinds(Bind[] binds) {
    for (Bind bind : binds) {
      if (bind == null) {
        throw parameterNotSet();
      }
      else if (bind.isOutBind()) {
        throw new IllegalStateException(
          "Batch execution with out binds is not supported");
      }
    }
  }

  private static IllegalStateException parameterNotSet() {
    return new IllegalStateException("One or more parameters are not set");
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
  /**
   * Returns a {@code BindValue} that binds a
   * @param object
   * @return
   */
  private Bind bindObject(Object object) {

    if (object == null) {
      return new NullBind(JDBCType.NULL);
    }
    else if (object instanceof Parameter) {
      return bindParameter((Parameter) object);
    }
    else {
      return createInBindValue(javaToSQLType(object.getClass()), object);
    }
  }

  private Bind bindParameter(Parameter parameter) {
    SQLType sqlType = OracleColumnMetadataImpl.r2dbcToSQLType(parameter.getType());
    if (sqlType == null) {
      throw new IllegalArgumentException(
        "Unsupported Parameter type:" + parameter.getType());
    }

    if (parameter instanceof Parameter.Out) {
      OutBind outBind = new OutBind(sqlType);
      return parameter instanceof Parameter.In
        ? new InOutBind(
            createInBindValue(sqlType, parameter.getValue()), outBind)
        : outBind;
    }
    else {
      return createInBindValue(sqlType, parameter.getValue());
    }
  }

  private Bind createInBindValue(SQLType sqlType, Object value) {
    if (value == null)
      return new NullBind(sqlType);
    else if (value instanceof io.r2dbc.spi.Blob)
      return bindBlob(sqlType, (io.r2dbc.spi.Blob)value);
    else if (value instanceof io.r2dbc.spi.Clob)
      return bindClob(sqlType, (io.r2dbc.spi.Clob)value);
    else if (value instanceof ByteBuffer)
      return bindByteBuffer(sqlType, (ByteBuffer)value);
    else
      return new InBind(
        sqlType, Mono.just(value), ignored -> Mono.empty());
  }

  private Bind bindBlob(
    SQLType sqlType, io.r2dbc.spi.Blob r2dbcBlob) {
    return new InBind<>(
      sqlType, materializeBlob(r2dbcBlob), adapter::publishBlobFree);
  }

  private Publisher<java.sql.Blob> materializeBlob(
    io.r2dbc.spi.Blob r2dbcBlob) {
    return Mono.fromSupplier(() ->
      getOrHandleSQLException(jdbcConnection::createBlob))
      .flatMap(jdbcBlob ->
        Mono.from(adapter.publishBlobWrite(r2dbcBlob.stream(), jdbcBlob))
          .onErrorResume(error ->
            Mono.from(r2dbcBlob.discard())
              .then(Mono.from(adapter.publishBlobFree(jdbcBlob)))
              .then(Mono.error(error)))
          .thenReturn(jdbcBlob));
  }

  private Bind bindClob(
    SQLType sqlType, io.r2dbc.spi.Clob r2dbcClob) {
    return new InBind<>(
      sqlType, materializeClob(r2dbcClob), adapter::publishClobFree);
  }

  private Publisher<java.sql.Clob> materializeClob(
    io.r2dbc.spi.Clob r2dbcClob) {
    return Mono.fromSupplier(() ->
      // Use NClob to support unicode characters
      getOrHandleSQLException(jdbcConnection::createNClob))
      .flatMap(jdbcClob ->
        Mono.from(adapter.publishClobWrite(r2dbcClob.stream(), jdbcClob))
          .onErrorResume(error ->
            Mono.from(r2dbcClob.discard())
              .then(Mono.from(adapter.publishClobFree(jdbcClob)))
              .then(Mono.error(error)))
          .thenReturn(jdbcClob));
  }

  /**
   * Converts a ByteBuffer to a byte array. The {@code byteBuffer} contents,
   * delimited by it's position and limit, are copied into the returned byte
   * array. No state of the {@code byteBuffer} is mutated, including it's
   * position, limit, or mark.
   * @param byteBuffer A ByteBuffer. Not null. Not retained.
   * @return A byte array storing the {@code byteBuffer's} content. Not null.
   */
  private Bind bindByteBuffer(SQLType sqlType, ByteBuffer byteBuffer) {

    // Copy the ByteBuffer into a byte array
    ByteBuffer slice = byteBuffer.slice(); // Don't mutate position/limit/mark
    byte[] byteArray = new byte[slice.remaining()];
    slice.get(byteArray);

    return new InBind<>(
      sqlType, Mono.just(byteArray), ignored -> Mono.empty());
  }

  private static final class NullBind implements Bind {

    final SQLType sqlType;

    private NullBind(SQLType sqlType) {
      this.sqlType = sqlType;
    }

    @Override
    public SQLType sqlType() {
      return sqlType;
    }

    @Override
    public Publisher<Void> setValue(PreparedStatement jdbcStatement, int jdbcIndex) {
      return Mono.defer(() -> {
        runOrHandleSQLException(() ->
          jdbcStatement.setNull(jdbcIndex, sqlType.getVendorTypeNumber()));
        return Mono.empty();
      });
    }

    @Override
    public Publisher<Void> discard() {
      return Mono.empty();
    }

    @Override
    public boolean isOutBind() {
      return false;
    }
  }

  private static final class InBind<T> implements Bind {
    final SQLType sqlType;
    final Publisher<T> valuePublisher;
    final Function<T, Publisher<Void>> discardFunction;
    final AtomicReference<T> valueReference = new AtomicReference<>(null);

    private InBind(
      SQLType sqlType, Publisher<T> valuePublisher,
      Function<T, Publisher<Void>> discardFunction) {
      this.sqlType = sqlType;
      this.valuePublisher = valuePublisher;
      this.discardFunction = discardFunction;
    }

    @Override
    public SQLType sqlType() {
      return sqlType;
    }

    @Override
    public Publisher<Void> setValue(
      PreparedStatement jdbcStatement, int jdbcIndex) {
      return Mono.from(valuePublisher)
        .doOnSuccess(value -> setValue(jdbcStatement, jdbcIndex, value))
        .then();
    }

    @Override
    public Publisher<Void> discard() {
      T value = valueReference.getAndSet(null);
      return value == null
        ? Mono.empty()
        : discardFunction.apply(value);
    }

    @Override
    public boolean isOutBind() {
      return false;
    }

    private void setValue(
      PreparedStatement jdbcStatement, int jdbcIndex, T value) {
      valueReference.set(value);
      runOrHandleSQLException(() ->
        jdbcStatement.setObject(jdbcIndex, value, sqlType));
    }
  }

  private static final class OutBind implements Bind {

    private final SQLType sqlType;

    private OutBind(SQLType sqlType) {
      this.sqlType = sqlType;
    }

    @Override
    public SQLType sqlType() {
      return sqlType;
    }

    @Override
    public Publisher<Void> setValue(
      PreparedStatement jdbcStatement, int jdbcIndex) {
      if (jdbcStatement instanceof CallableStatement) {
        CallableStatement callableStatement = (CallableStatement)jdbcStatement;
        return Mono.fromSupplier((ThrowingSupplier<Void>)() -> {
          callableStatement.registerOutParameter(jdbcIndex, sqlType);
          return null;
        });
      }
      else {
        throw new IllegalStateException(
          "CallableStatement is required for out parameter binds. Can not " +
            "invoke registerOutParameter on: " + jdbcStatement.getClass());
      }
    }

    @Override
    public Publisher<Void> discard() {
      return Mono.empty();
    }

    @Override
    public boolean isOutBind() {
      return true;
    }
  }

  private static final class InOutBind implements Bind {
    private final Bind inBind;
    private final OutBind outBind;

    private InOutBind(Bind inBind, OutBind outBind) {
      this.inBind = inBind;
      this.outBind = outBind;
    }

    @Override
    public SQLType sqlType() {
      return inBind.sqlType();
    }

    @Override
    public Publisher<Void> setValue(PreparedStatement jdbcStatement, int jdbcIndex) {
      return Mono.from(inBind.setValue(jdbcStatement, jdbcIndex))
        .then(Mono.from(outBind.setValue(jdbcStatement, jdbcIndex)));
    }

    @Override
    public Publisher<Void> discard() {
      return Mono.from(inBind.discard())
        .then(Mono.from(outBind.discard()));
    }

    @Override
    public boolean isOutBind() {
      return true;
    }
  }

  /**
   * Bind value that is set on a JDBC statement. Instance of {@code
   * JdbcBind} implement different strategies for specifying a bind
   * value to JDBC, depending on how the bind value is specified to R2DBC:
   * <dl>
   *   <dd>{@code bindNull(int/String, Class)}</dd>
   *   <dt>
   *     A NULL bind for an IN parameter is specified to JDBC.
   *   </dt>
   *   <dt>
   *     {@code bind(int/String, Object)}, where the Object is not an instance
   *     of {@code Parameter}
   *   </dt>
   *   <dd>
   *     The object is specified to JDBC as a bind for an IN parameter, without
   *     specifying a SQL type. The JDBC driver represents the bind value as
   *     the default SQL type mapping  for the Java object's type. Default
   *     mappings are listed in Table B.4 of the JDBC 4.3 Specification
   *   </dd>
   *   <dd>
   *     {@code bind(int/String, Object)}, where the Object is an instance of
   *     of {@link Parameter} and is not an instance of {@link Parameter.Out}.
   *   </dd>
   *   <dt>
   *     {@link Parameter#getValue()} is specified to JDBC as a bind for an IN
   *     parameter, with {@link Parameter#getType()} specifying a SQL type.
   *     Table B.5 of the JDBC 4.3 Specification lists the combinations of
   *     Java and SQL types that a JDBC driver will support.
   *   </dt>
   *   <dd>
   *     {@code bind(int/String, Object)}, where the Object is an instance of
   *     of {@link Parameter} and also an instance of {@link Parameter.Out}
   *     and is not an instance of {@link Parameter.In}.
   *   </dd>
   *   <dt>
   *     {@link Parameter#getType()} is specified to JDBC as a SQL type for
   *     an OUT parameter. {@link Parameter#getValue()} is ignored.
   *   </dt>
   *   <dd>
   *     {@code bind(int/String, Object)}, where the Object is an instance of
   *     of {@link Parameter} and also an instance of {@link Parameter.Out},
   *     and also an instance of {@link Parameter.In}.
   *   </dd>
   *   <dt>
   *     {@link Parameter#getType()} is specified to JDBC as a SQL type for
   *     an OUT parameter. {@link Parameter#getValue()} is specified to JDBC as
   *     a bind for an IN parameter, with {@link Parameter#getType()}
   *     specifying a SQL type. Table B.5 of the JDBC 4.3 Specification lists
   *     the combinations of Java and SQL types that a JDBC driver will
   *     support.
   *   </dt>
   * </dl>
   */
  private interface Bind {

    SQLType sqlType();

    /**
     * <p>
     * Sets this bind value as a {@code jdbcStatement}'s parameter at a 1-based
     * {@code jdbcIndex}. The returned {@code Publisher} emits {@code
     * onComplete} immediately if the bind value can be materialized
     * synchronously, without blocking the calling thread.
     * </p><p>
     * Certain bind values types, such as {@code Blob} or {@code Clob}, require
     * asynchronous processing to occur before a bind value materializes. For
     * these cases, the returned {@code Publisher} emits {@code onComplete}
     * after the value is materialized and set as a parameter on the
     * {@code jdbcStatement}. The {@code Publisher} may emit {@code onError}
     * if a failure occurs during materialization.
     * </p><p>
     * An invocation of this method may allocate resources for the bind
     * value, and these resources may need to be held until the {@code
     * jdbcStatement} is executed. In order to deallocate these
     * resources, an invocation of this method should always be paired with an
     * invocation of {@link #discard()} following the statement's execution.
     * </p>
     * @param jdbcStatement JDBC statement set with a parameter's bind
     * value.
     * @param jdbcIndex 1-based index of a parameter
     * @return {@code Publisher} that terminates after {@code jdbcStatement}
     * is set with a bind value, or after materializing the bind value fails.
     */
    Publisher<Void> setValue(PreparedStatement jdbcStatement, int jdbcIndex);

    /**
     * Discards any resources allocated by a previous invocation of
     * {@link #setValue(PreparedStatement, int)}. The returned {@code Publisher}
     * emits {@code onComplete} immediately if no resources are
     * currently allocated.
     * @return A {@code Publisher} that terminates after resources are
     * deallocated, or after deallocating resources fails.
     */
    Publisher<Void> discard();

    boolean isOutBind();
  }

}
