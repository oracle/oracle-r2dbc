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

import io.r2dbc.spi.OutParameterMetadata;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.Type;
import oracle.r2dbc.impl.ReactiveJdbcAdapter.JdbcReadable;
import oracle.r2dbc.impl.ReadablesMetadata.OutParametersMetadataImpl;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import static java.sql.Statement.KEEP_CURRENT_RESULT;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.fromJdbc;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.newNonTransientException;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireOpenConnection;
import static oracle.r2dbc.impl.OracleReadableImpl.createOutParameters;
import static oracle.r2dbc.impl.OracleReadableMetadataImpl.createParameterMetadata;
import static oracle.r2dbc.impl.OracleResultImpl.createBatchUpdateErrorResult;
import static oracle.r2dbc.impl.OracleResultImpl.createCallResult;
import static oracle.r2dbc.impl.OracleResultImpl.createErrorResult;
import static oracle.r2dbc.impl.OracleResultImpl.createQueryResult;
import static oracle.r2dbc.impl.ReadablesMetadata.createOutParametersMetadata;
import static oracle.r2dbc.impl.SqlTypeMap.toJdbcType;

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

  /**
   * Instance of {@code Object} representing a null bind value. This object
   * is stored at indexes of {@link #bindValues} that have been set with a
   * null value.
   */
  private static final Object NULL_BIND = new Object();

  /**
   * Lock that guards access to the {@link #jdbcConnection} and any object
   * created by that connection
   */
  private final AsyncLock jdbcLock;

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
   * Timeout, in seconds, applied to the execution of this {@code Statement}
   */
  private final int timeout;

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
   * @param sql SQL Language statement that may include parameter markers.
   * @param timeout Timeout applied to the execution of the constructed
   * {@code Statement}. Not null. Not negative.
   * @param jdbcConnection JDBC connection to an Oracle Database.
   * @param adapter Adapts JDBC calls into reactive streams.
   * @param jdbcLock
   */
  OracleStatementImpl(
    String sql, Duration timeout, Connection jdbcConnection,
    ReactiveJdbcAdapter adapter, AsyncLock jdbcLock) {
    this.sql = sql;
    this.jdbcConnection = jdbcConnection;
    this.adapter = adapter;
    this.jdbcLock = jdbcLock;

    // The SQL string is parsed to identify parameter markers and allocate the
    // bindValues array accordingly
    this.parameterNames = SqlParameterParser.parse(sql);
    this.bindValues = new Object[parameterNames.size()];

    // Round the timeout up to the nearest whole second, so that it may be
    // set with PreparedStatement.setQueryTimeout(int)
    this.timeout = (int)Math.min(
      Integer.MAX_VALUE,
      timeout.toSeconds() + (timeout.getNano() == 0 ? 0 : 1));
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
    requireOpenConnection(jdbcConnection);
    requireNonNull(value, "value is null");
    requireValidIndex(index);
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
    requireValidIndex(index);
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
    addBatchValues();
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

    if (isOutParameterPresent())
      throw outParameterWithGeneratedValues();

    if (! batch.isEmpty())
      throw generatedValuesWithBatch();

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

    final Publisher<JdbcStatement> statementPublisher;
    if (! batch.isEmpty())
      statementPublisher = createJdbcBatch();
    else if (isOutParameterPresent())
      statementPublisher = createJdbcCall();
    else if (generatedColumns != null)
      statementPublisher = createJdbcReturningGenerated();
    else
      statementPublisher = createJdbcStatement();

    // Allow just one subscriber to the result publisher.
    AtomicBoolean isSubscribed = new AtomicBoolean(false);
    return Flux.defer(() -> {
      if (isSubscribed.compareAndSet(false, true)) {
        return Mono.from(statementPublisher)
          .flatMapMany(JdbcStatement::execute);
      }
      else {
        return Mono.error(new IllegalStateException(
          "Multiple subscribers are not supported by the Oracle R2DBC" +
            " Statement.execute() publisher"));
      }
    });
  }

  /**
   * Creates a {@code JdbcStatement} that executes this statement as a DML
   * statement returning generated values.
   * @return A JDBC call statement publisher
   */
  private Publisher<JdbcStatement> createJdbcStatement() {
    int currentFetchSize = fetchSize;
    Object[] currentBinds = copyBinds();

    return jdbcLock.get(() -> {
      PreparedStatement preparedStatement =
        jdbcConnection.prepareStatement(sql);
      preparedStatement.setFetchSize(currentFetchSize);
      preparedStatement.setQueryTimeout(timeout);
      return new JdbcStatement(preparedStatement, currentBinds);
    });
  }

  /**
   * Creates a {@code JdbcStatement} that executes this statement with a batch
   * of bind values added by {@link #add()}. If one or more values are
   * missing in the current set of binds, the statement executes with all
   * previously added binds, and then emits an error.
   * @return A JDBC batch statement publisher
   */
  private Publisher<JdbcStatement> createJdbcBatch() {

    IllegalStateException invalidBinds;
    try {
      add();
      invalidBinds = null;
    }
    catch (IllegalStateException illegalStateException) {
      invalidBinds = illegalStateException;
    }
    final IllegalStateException finalInvalidBinds = invalidBinds;

    int currentFetchSize = fetchSize;
    Queue<Object[]> currentBatch = batch;
    batch = new LinkedList<>();

    return jdbcLock.get(() -> {
      PreparedStatement preparedStatement =
        jdbcConnection.prepareStatement(sql);
      preparedStatement.setFetchSize(currentFetchSize);
      preparedStatement.setQueryTimeout(timeout);
      return finalInvalidBinds == null
        ? new JdbcBatch(preparedStatement, currentBatch)
        : new JdbcBatchInvalidBinds(
            preparedStatement, currentBatch, finalInvalidBinds);
    });
  }

  /**
   * Creates a {@code JdbcStatement} that executes this statement as a
   * procedural call returning one or more out-parameters.
   * @return A JDBC call statement publisher
   */
  private Publisher<JdbcStatement> createJdbcCall() {
    int currentFetchSize = fetchSize;
    Object[] currentBinds = copyBinds();

    return jdbcLock.get(() -> {
      CallableStatement callableStatement = jdbcConnection.prepareCall(sql);
      callableStatement.setFetchSize(currentFetchSize);
      callableStatement.setQueryTimeout(timeout);
      return new JdbcCall(callableStatement, currentBinds, parameterNames);
    });
  }


  /**
   * Creates a {@code JdbcStatement} that executes this statement as a DML
   * statement returning generated values.
   * @return A JDBC call statement publisher
   */
  private Publisher<JdbcStatement> createJdbcReturningGenerated() {
    int currentFetchSize = fetchSize;
    Object[] currentBinds = copyBinds();
    String[] currentGeneratedColumns = generatedColumns.clone();

    return jdbcLock.get(() -> {
      PreparedStatement preparedStatement =
        currentGeneratedColumns.length == 0
          ? jdbcConnection.prepareStatement(sql, RETURN_GENERATED_KEYS)
          : jdbcConnection.prepareStatement(sql, currentGeneratedColumns);
      preparedStatement.setFetchSize(currentFetchSize);
      preparedStatement.setQueryTimeout(timeout);
      return new JdbcReturningGenerated(preparedStatement, currentBinds);
    });
  }

  /**
   * Binds a {@code value} to all named parameters matching the specified
   * {@code name}. The match is case-sensitive.
   * @param name A parameter name. Not null.
   * @param value A value to bind. May be null.
   * @throws NoSuchElementException if no named parameter matches the
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
      throw new NoSuchElementException(
        "Unrecognized parameter identifier: " + name);
    }
  }

  /**
   * Binds an {@code object} to a parameter {@code index}. If the {@code object}
   * is an instance of {@link Parameter}, then its Java type and SQL type are
   * validated as types that Oracle R2DBC supports. If the {@code object}
   * is not an instance of {@code Parameter}, then only its Java type is
   * validated.
   *
   * @param object Bind value to retain. Not null.
   * @throws IllegalArgumentException If {@code object} is a {@code Parameter},
   * and the class of the value is not supported as a bind value.
   * @throws IllegalArgumentException If {@code object} is a {@code Parameter},
   * and the SQL type is not supported as a bind value.
   * @throws IllegalArgumentException If {@code object} is not a
   * {@code Parameter}, and the class of {@code object} is not supported as a
   * bind value.
   */
  private void bindObject(int index, Object object) {
    if (object == null){
      bindValues[index] = NULL_BIND;
    }
    else if (object instanceof Parameter) {
      bindParameter(index, (Parameter)object);
    }
    else if (object instanceof Parameter.In
      || object instanceof Parameter.Out) {
      throw new IllegalArgumentException(
        "Parameter.In and Parameter.Out bind values must implement Parameter");
    }
    else {
      requireSupportedJavaType(object);
      bindValues[index] = object;
    }
  }

  /**
   * Binds a {@code parameter} to a specified {@code index} of this
   * {@code Statement}.
   * @param index A 0-based parameter index
   * @param parameter Parameter to bind
   * @throws IllegalArgumentException If the Java or SQL type of the
   * {@code parameter} is not supported.
   */
  private void bindParameter(int index, Parameter parameter) {

    if (parameter instanceof Parameter.Out) {
      if (batch.isEmpty())
        throw outParameterWithBatch();
      if (generatedColumns != null)
        throw outParameterWithGeneratedValues();
    }

    // TODO: This method should check if Java type can be converted to the
    //  specified SQL type. If the conversion is unsupported, then JDBC
    //  setObject(...) will throw when this statement is executed. The correct
    //  behavior is to throw IllegalArgumentException here, and not from
    //  execute()
    Type r2dbcType =
      requireNonNull(parameter.getType(), "Parameter type is null");
    SQLType jdbcType = toJdbcType(r2dbcType);

    if (jdbcType == null) {
      throw new IllegalArgumentException(
        "Unsupported SQL type: " + r2dbcType);
    }

    requireSupportedJavaType(parameter.getValue());
    bindValues[index] = parameter;
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
   * Adds the current set of {@link #bindValues} to the {@link #batch}, and
   * then resets the {@code parameters} array to store {@code null} at all
   * positions.
   * @throws IllegalStateException If a parameter has not been set
   * @throws IllegalStateException If an out parameter has been set
   */
  private void addBatchValues() {
    if (generatedColumns != null)
      throw generatedValuesWithBatch();

    for (Object parameter : bindValues) {
      if (parameter == null) {
        throw parameterNotSet();
      }
      else if (parameter instanceof Parameter.Out) {
        throw outParameterWithBatch();
      }
    }

    batch.add(bindValues.clone());
    Arrays.fill(bindValues, null);
  }

  /**
   * Returns {@code true} if {@link #bindValues} contains an out parameter.
   * @return {@code true} if an out parameter is present, otherwise
   * {@code false}
   */
  private boolean isOutParameterPresent() {
    for (Object value : bindValues) {
      if (value instanceof Parameter.Out)
        return true;
    }
    return false;
  }

  /**
   * Returns a copy of the current set of bind values. This method is called
   * before executing with the current set of bind values, so it will verify
   * that all values are set and then clear the current set for the next
   * execution.
   * @return A copy of the bind values
   */
  private Object[] copyBinds() {
    requireAllParametersSet();
    Object[] currentBinds = bindValues.clone();
    Arrays.fill(bindValues, null);
    return currentBinds;
  }

  /**
   * Checks that a bind value has been set for all positions in the
   * current set of {@link #bindValues}
   * @throws IllegalStateException if one or more parameters are not set.
   */
  private void requireAllParametersSet() {
    for (Object parameter : bindValues) {
      if (parameter == null)
        throw parameterNotSet();
    }
  }

  /**
   * Returns an exception indicating that a parameter has not been set.
   * @return Unset parameter exception
   */
  private static IllegalStateException parameterNotSet() {
    return new IllegalStateException("One or more parameters are not set");
  }

  /**
   * Checks that the class type of an {@code object} is supported as a bind
   * value.
   * @param object Object to check. May be null.
   * @throws IllegalArgumentException If the class type of {@code object} is not
   * supported
   */
  private static void requireSupportedJavaType(Object object) {
    if (object != null && toJdbcType(object.getClass()) == null) {
      throw new IllegalArgumentException(
        "Unsupported Java type:" + object.getClass());
    }
  }

  /**
   * Returns an exception indicating that it is not possible to execute a
   * statement that returns both out-parameters and generated values. There
   * is no JDBC API to create a {@link CallableStatement} that returns
   * generated values (aka: generated keys).
   * @return Exception for configuring out-parameters with generated values.
   */
  private static IllegalStateException outParameterWithGeneratedValues() {
    return new IllegalStateException(
      "Statement can not return both out-parameters and generated values");
  }

  /**
   * Returns an exception indicating that it is not possible to execute a
   * statement with a batch of out-parameters. This is not supported by
   * Oracle Database, although it would be possible to emulate it by
   * executing a sequence of {@link CallableStatement}s individually (TODO?)
   * @return Exception for batching out-parameters.
   */
  private static IllegalStateException outParameterWithBatch() {
    return new IllegalStateException(
      "Batch execution with out parameters is not supported");
  }

  /**
   * Returns an exception indicating that it is not possible to execute a
   * statement as a batch and returning generated values. This is not supported
   * by  Oracle Database, although it would be possible to emulate it by
   * executing a sequence of {@link PreparedStatement}s individually (TODO?)
   * @return Exception for batching with generated values
   */
  private static IllegalStateException generatedValuesWithBatch() {
    return new IllegalStateException(
      "Batch execution returning generated values is not supported");
  }

  /**
   * <p>
   * A statement that is executed using JDBC. The base class is implemented to
   * execute SQL that returns an update count, row data, or implicit results
   * (ie: DBMS_SQL.RETURN_RESULT).
   * </p><p>
   * Subclasses may extend the base class to handle other types of results,
   * such as DML returning generated values, a procedural call that
   * returns out-parameters, or a batch DML execution.
   * </p><p>
   * The base class ensures that all resources allocated for the statement
   * execution are eventually deallocated. This includes the
   * {@link #preparedStatement}, along with resources allocoated for bind
   * values, such as {@code java.sql.Blob/Clob}.
   * </p>
   */
  private class JdbcStatement {

    /** The {@code PreparedStatement} that is executed */
    protected final PreparedStatement preparedStatement;

    /** The bind values that are set on the {@link #preparedStatement} */
    protected final Object[] binds;

    /**
     * Publishers that deallocate resources after the
     * {@link #preparedStatement} is executed
     */
    protected final List<Publisher<Void>> deallocations = new ArrayList<>(1);

    /**
     * Constructs a new {@code JdbcStatement} that executes a
     * {@code preparedStatement} with the given {@code binds}.
     * @param preparedStatement Statement to execute. Not null. Retained.
     * @param binds Bind values. Not null. Retained.
     */
    private JdbcStatement(PreparedStatement preparedStatement, Object[] binds) {
      this.preparedStatement = preparedStatement;
      this.binds = binds;
    }

    /**
     * <p>
     * Executes this statement and returns a publisher that emits the results.
     * </p><p>
     * This method first subscribes to the {@link #bind()} publisher, and then
     * subscribes to the {@link #getResults()} publisher after the bind
     * publisher has completed. Subclasses may override the {@code bind} and
     * {@code getResults} methods as needed for different types of binds and
     * results.
     * </p><p>
     * This method is implemented to create {@code Results} of
     * {@link Result.Message} segments if an {@link R2dbcException} is
     * emitted from the {@code bind} or {@code getResults} publishers, or if
     * {@link PreparedStatement#getWarnings()} yields a warning.
     * </p><p>
     * After all {@code Results} have been consumed, the
     * {@link #preparedStatement} is closed.
     * </p>
     * @return A publisher that emits the result of executing this statement
     */
    final Publisher<OracleResultImpl> execute() {
      // Results are collected into a list, and when every result in that
      // list is consumedthe PreparedStatement is
      // closed
      // after every result in the list is consumed.
      return Flux.using(
        () -> new ArrayList<>(1),
        results ->
          Mono.from(bind())
            .thenMany(getResults())
            .concatMap(this::getWarnings)
            .doOnNext(results::add)
            .onErrorResume(R2dbcException.class, r2dbcException ->
              Mono.just(createErrorResult(r2dbcException))),
        this::deallocate);
    }

    /**
     * <p>
     * Sets {@link #binds} on the {@link #preparedStatement}. The
     * returned {@code Publisher} completes after all bind values have
     * materialized and been set on the {@code preparedStatement}.
     * </p><p>
     * The base class implements this method to ignore any bind values that
     * that are instances of {@link Parameter.Out}, and not also an instance of
     * {@link Parameter.In}. Subclasses may override this method handle
     * out-parameters, or to bind a batch of values.
     * </p>
     * @return A {@code Publisher} that emits {@code onComplete} when all
     * {@code binds} have been set.
     */
    protected Publisher<Void> bind() {
      return Flux.concat(jdbcLock.get(() -> {
        List<Publisher<Void>> bindPublishers = new ArrayList<>(0);
        for (int i = 0; i < binds.length; i++) {

          if (binds[i] instanceof Parameter.Out
            && !(binds[i] instanceof Parameter.In))
            continue;

          Object jdbcValue = convertBind(binds[i]);
          SQLType jdbcType =
            binds[i] instanceof Parameter
              ? toJdbcType(((Parameter) binds[i]).getType())
              : null; // JDBC infers the type

          if (jdbcValue instanceof Publisher<?>) {
            int indexFinal = i;
            Publisher<Void> bindPublisher =
              Mono.from((Publisher<?>) jdbcValue)
                .flatMap(allocatedValue ->
                  Mono.from(jdbcLock.run(() ->
                    setBind(indexFinal, allocatedValue, jdbcType))));
            bindPublishers.add(bindPublisher);
          }
          else {
            setBind(i + 1, jdbcValue, jdbcType);
          }
        }
        return Flux.concat(bindPublishers);
      }));
    }

    /**
     * Get results from executing the {@link #preparedStatement}. The base
     * class implements this method to get results of update count, row data,
     * or implicit results (ie: DBMS_SQL.RETURN_RESULT). Subclasses
     * may override this method to produce different types of results.
     * @return A publisher that emits the results.
     */
    protected Publisher<OracleResultImpl> getResults() {
      return Mono.from(adapter.publishSQLExecution(preparedStatement))
        .flatMap(isResultSet ->
          Mono.from(jdbcLock.get(() -> {
            ArrayList<OracleResultImpl> results = new ArrayList<>(1);
            OracleResultImpl result = getCurrentResult(isResultSet);
            while (result != null) {
              results.add(result);
              result = getCurrentResult(
                preparedStatement.getMoreResults(KEEP_CURRENT_RESULT));
            }
            return results;
          })))
        .flatMapMany(Flux::fromIterable);
    }

    /**
     * Returns the current {@code Result} of the {@link #preparedStatement}.
     * This method returns a result of row data if {@code isResultSet} is
     * {@code true}. Otherwise, this method returns a result of an update
     * count if {@link PreparedStatement#getUpdateCount()} returns a value of 0
     * or greater. Otherwise, this method returns {@code null} if
     * {@code isResultSet} is {@code false} and {@code getUpdateCount}
     * returns a negative number.
     * @param isResultSet {@code true} if the current result is row data,
     * otherwise false.
     * @return The current {@code Result} of the {@code preparedStatement}
     * @throws SQLException From JDBC
     * @implNote This method invokes JDBC methods, and should only be called
     * when holding ownership of the {@link #jdbcLock}
     */
    private OracleResultImpl getCurrentResult(boolean isResultSet)
      throws SQLException {
      if (isResultSet) {
        return OracleResultImpl.createQueryResult(
          preparedStatement.getResultSet(), adapter);
      }
      else {
        long updateCount = preparedStatement.getLargeUpdateCount();
        return updateCount >= 0
          ? OracleResultImpl.createUpdateCountResult(updateCount)
          : null;
      }
    }

    /**
     * Returns a {@code Result} that publishes any {@link SQLWarning}s of the
     * {@link #preparedStatement} as {@link io.r2dbc.spi.Result.Message}
     * segments followed by any {@code Segments} of a {@code result}. This
     * method returns the provided {@code result} if the {@code
     * preparedStatement} has
     * no warnings.
     * @param result Result of executing the {@code preparedStatement}
     * @return A {@code Result} having any warning messages of the
     * {@code preparedStatement} along with its execution {@code result}.
     */
    private Publisher<OracleResultImpl> getWarnings(OracleResultImpl result) {
      return jdbcLock.get(() -> {
        SQLWarning warning = preparedStatement.getWarnings();
        preparedStatement.clearWarnings();
        return warning == null
          ? result
          : OracleResultImpl.createWarningResult(warning, result);
      });
    }

    /**
     * <p>
     * Deallocates all resources allocated by this statement. Each publisher
     * in {@link #deallocations} is subscribed to, and the
     * {@link #preparedStatement} is closed after all {@code results} have been
     * consumed.
     * </p><p>
     * Closing the {@link #preparedStatement} must only occur after
     * consuming results, because a result may be backed by a
     * {@link java.sql.ResultSet} or by {@link CallableStatement}.
     * </p><p>
     * If the deallocation of any resource results in an error, an attempt is
     * made to deallocate any remaining resources before emitting the error.
     * </p>
     * @param results Results that must be consumed before closing the
     * {@link #preparedStatement}
     * @return A publisher that completes when all resources have been
     * deallocated
     */
    private Publisher<Void> deallocate(
      Collection<OracleResultImpl> results) {

      AtomicInteger unconsumed = new AtomicInteger(results.size());
      for (OracleResultImpl result : results) {
        boolean isConsumed = !result.onConsumed(Mono.defer(() ->
          unconsumed.decrementAndGet() == 0
            ? Mono.from(jdbcLock.run(preparedStatement::close))
            : Mono.empty()));

        if (isConsumed)
          unconsumed.decrementAndGet();
      }

      // Deallocate now if all results have already been consumed
      if (unconsumed.get() == 0)
        deallocations.add(jdbcLock.run(preparedStatement::close));

      return Flux.concatDelayError(Flux.fromIterable(deallocations));
    }

    /**
     * Sets the {@code value} of a {@code preparedStatement} parameter at the
     * specified {@code index}. If a non-null {@code type} is provided, then it is
     * specified as the SQL type for the bind. Otherwise, if the
     * {@code type} is {@code null}, then the JDBC driver infers the SQL type
     * of the bind.
     * @param index 1-based parameter index
     * @param value Value. May be null.
     * @param type SQL type. May be null.
     * @implNote This method invokes JDBC methods, and should only be called
     * when holding ownership of the {@link #jdbcLock}
     */
    private void setBind(int index, Object value, SQLType type)
      throws SQLException {
      if (type != null)
        preparedStatement.setObject(index, value, type);
      else
        preparedStatement.setObject(index, value);
    }

    /**
     * <p>
     * Converts a {@code value} of a type that is supported by R2DBC into an
     * equivalent type that is supported by JDBC. The object returned by this
     * method will express the same information as the original {@code value}
     * For instance, if this method is called with an {@code io.r2dbc.spi.Blob}
     * type {@code value}, it will convert it into an {@code java.sql.Blob}
     * type value that stores the same content as the R2DBC {@code Blob}.
     * </p><p>
     * If no conversion is necessary, this method returns the original
     * {@code value}. If the conversion requires a database call, this
     * method returns a {@code Publisher} that emits the converted value. If
     * the conversion requires resource allocation, a {@code Publisher} that
     * deallocates resources is added to the {@code discardQueue}.
     * </p>
     *
     * @param value Bind value to convert. May be null.
     * @return Value to set as a bind on the JDBC statement. May be null.
     * @throws IllegalArgumentException If the JDBC driver can not convert a
     *   bind value into a SQL value.
     */
    private Object convertBind(Object value) {
      if (value == null || value == NULL_BIND) {
        return null;
      }
      else if (value instanceof Parameter) {
        return convertBind(((Parameter) value).getValue());
      }
      else if (value instanceof io.r2dbc.spi.Blob) {
        return convertBlobBind((io.r2dbc.spi.Blob) value);
      }
      else if (value instanceof io.r2dbc.spi.Clob) {
        return convertClobBind((io.r2dbc.spi.Clob) value);
      }
      else if (value instanceof ByteBuffer) {
        return convertByteBufferBind((ByteBuffer) value);
      }
      else {
        return value;
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
      io.r2dbc.spi.Blob r2dbcBlob) {
      return Mono.usingWhen(
        jdbcLock.get(jdbcConnection::createBlob),
        jdbcBlob ->
          Mono.from(adapter.publishBlobWrite(r2dbcBlob.stream(), jdbcBlob))
            .thenReturn(jdbcBlob),
        jdbcBlob -> {
          deallocations.add(adapter.publishBlobFree(jdbcBlob));
          return r2dbcBlob.discard();
        });
    }

    /**
     * Converts an R2DBC Clob to a JDBC Clob. The returned {@code Publisher}
     * asynchronously writes the {@code r2dbcClob} content to a JDBC Clob and
     * then emits the JDBC Clob after all content has been written. The JDBC
     * Clob allocates a temporary database Clob that is freed by a {@code
     * Publisher} added to the {@code discardQueue}.
     * @param r2dbcClob An R2DBC Clob. Not null. Retained.
     * @return A JDBC Clob. Not null.
     */
    private Publisher<java.sql.Clob> convertClobBind(
      io.r2dbc.spi.Clob r2dbcClob) {
      return Mono.usingWhen(
        jdbcLock.get(jdbcConnection::createNClob),
        jdbcClob ->
          Mono.from(adapter.publishClobWrite(r2dbcClob.stream(), jdbcClob))
            .thenReturn(jdbcClob),
        jdbcClob -> {
          deallocations.add(adapter.publishClobFree(jdbcClob));
          return r2dbcClob.discard();
        });
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

  }

  /**
   * A statement that is executed to return out-parameters with JDBC. This
   * subclass of {@link JdbcStatement} overrides the base class behavior to
   * register out-parameters with a {@link CallableStatement}, and to return
   * a {@link Result} of out-parameters.
   */
  private class JdbcCall extends JdbcStatement {

    /**
     * The indexes of out-parameter binds in the {@link #preparedStatement}.
     * The array is sorted such that {@code outBindIndexes[0]} is the index
     * of the first out-parameter, and {@code outBindIndexes[0]} is the index
     * of the second out-parameter, and so on.
     */
    private final int[] outBindIndexes;

    /**
     * Metadata for out-parameter binds in the {@link #preparedStatement}.
     */
    private final OutParametersMetadataImpl metadata;

    /**
     * Constructs a new {@code JdbcCall} that synchronizes on the given
     * {@code jdbcLock}, and executes a {@code callableStatement} with the given
     * {@code bindValues} and {@code parameterNames}.
     */
    private JdbcCall(
      CallableStatement callableStatement,
      Object[] bindValues, List<String> parameterNames) {
      super(callableStatement, bindValues);

      outBindIndexes = IntStream.range(0, bindValues.length)
        .filter(i -> bindValues[i] instanceof Parameter.Out)
        .toArray();

      OutParameterMetadata[] metadataArray =
        new OutParameterMetadata[outBindIndexes.length];

      for (int i = 0; i < metadataArray.length; i++) {
        int bindIndex = outBindIndexes[i];
        String name = parameterNames.get(bindIndex);
        metadataArray[i] = createParameterMetadata(
          name == null ? String.valueOf(i) : name,
          ((Parameter)bindValues[bindIndex]).getType());
      }

      this.metadata = createOutParametersMetadata(metadataArray);
    }

    @Override
    protected Publisher<Void> bind() {
      return Flux.concat(super.bind(),
        registerOutParameters());
    }

    /**
     * Invokes {@link CallableStatement#registerOutParameter(int, int)} to
     * register each instance of {@link Parameter.Out} in the given
     * {@code values}
     * @return A publisher that completes when all out-parameter binds are
     * registered.
     */
    private Publisher<Void> registerOutParameters() {
      return jdbcLock.run(() -> {
        for (int i : outBindIndexes) {
          Type type = ((Parameter) binds[i]).getType();
          SQLType jdbcType = toJdbcType(type);
          preparedStatement.unwrap(CallableStatement.class)
            .registerOutParameter(i + 1, jdbcType);
        }
      });
    }

    @Override
    protected Publisher<OracleResultImpl> getResults() {
      return Flux.concat(
        super.getResults(),
        Mono.just(createCallResult(createOutParameters(
          new JdbcOutParameters(), metadata, adapter))));
    }

    /**
     * Out parameter values returned by the database.
     */
    private final class JdbcOutParameters implements JdbcReadable {

      /**
       * {@inheritDoc}
       * <p>
       * Returns the out-parameter value from the {@code CallableStatement} by
       * mapping an R2DBC out-parameter index to a JDBC parameter index. The
       * difference between the two is that R2DBC indexes are relative only
       * to other out-parameters. So for index 0, R2DBC returns the first
       * out-parameter, even if there are in-parameters at lower indexes in
       * the parameterized SQL expression. Likewise, for index 1, R2DBC
       * returns the second out-parameter, even if there are 1 or more
       * in-parameters between the first and second out-parameter.
       * </p>
       */
      @Override
      public <T> T getObject(int index, Class<T> type) {
        // TODO: Throw IllegalArgumentException or IndexOutOfBoundsException
        //  based on the error code of any SQLException thrown
        return fromJdbc(() ->
          preparedStatement.unwrap(CallableStatement.class)
            .getObject(outBindIndexes[index] + 1, type));
      }
    }

  }

  /**
   * A statement that executes with a batch of bind values. This subclass of
   * {@link JdbcStatement} overrides the base class to bind a batch of
   * values, and to execute the JDBC statement using
   * {@link ReactiveJdbcAdapter#publishBatchUpdate(PreparedStatement)}.
   */
  private class JdbcBatch extends JdbcStatement {

    /** Batch of bind values. */
    private final Queue<Object[]> batch;

    private JdbcBatch(
      PreparedStatement preparedStatement, Queue<Object[]> batch) {
      super(preparedStatement, null);
      this.batch = batch;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The returned {@code Publisher} emits 1 {@code Result} having an
     * {@link io.r2dbc.spi.Result.UpdateCount} segment for each set of bind
     * values in the {@link #batch}.
     * </p>
     */
    @Override
    protected Publisher<OracleResultImpl> getResults() {
      int batchSize = batch.size();
      AtomicInteger index = new AtomicInteger(0);

      return Mono.from(bind())
        .thenMany(adapter.publishBatchUpdate(preparedStatement))
        .collect(
          () -> new long[batchSize],
          (updateCounts, updateCount) ->
            updateCounts[index.getAndIncrement()] = updateCount)
        .map(OracleResultImpl::createBatchUpdateResult)
        .onErrorResume(
          error ->
            error instanceof R2dbcException
              && error.getCause() instanceof BatchUpdateException,
          error ->
            Mono.just(createBatchUpdateErrorResult(
              (BatchUpdateException) error.getCause())));
    }

    /**
     * {@code inheritDoc}
     * <p>
     * Binds the first set of values in {@link #binds}, then copies each
     * remaining set of value into {@link #binds} and binds those as well. Calls
     * {@link PreparedStatement#addBatch()} before binding each set of values
     * after the first.
     * </p>
     */
    @Override
    protected Publisher<Void> bind() {
      return Flux.concat(
        super.bind(),
        Flux.fromIterable(batch)
          .concatMap(nextBinds -> {
            System.arraycopy(nextBinds, 0, binds, 0, binds.length);
            return Mono.from(jdbcLock.run(preparedStatement::addBatch))
              .thenEmpty(bind());
          }));
    }
  }

  /**
   * A JDBC batch execution where one or more binds are missing in the final
   * set of bind values.
   */
  private final class JdbcBatchInvalidBinds extends JdbcBatch {

    /** Exception thrown when one or more bind values are missing */
    private final IllegalStateException missingBinds;

    private JdbcBatchInvalidBinds(
      PreparedStatement preparedStatement, Queue<Object[]> batch,
      IllegalStateException missingBinds) {
      super(preparedStatement, batch);
      this.missingBinds = missingBinds;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Allows the batch to execute with all previously added binds, and then
     * emits an error result for the missing binds.
     * </p>
     */
    @Override
    protected Publisher<OracleResultImpl> getResults() {
      return Flux.from(super.execute())
        .concatWithValues(createErrorResult(
          newNonTransientException(
            "One or more binds not set after calling add()", sql,
            missingBinds)));
    }
  }

  /**
   * A statement that returns values generated by a DML command, such as an
   * column declared with an auto-generated value:
   * {@code id NUMBER GENERATED ALWAYS AS IDENTITY}
   */
  private final class JdbcReturningGenerated extends JdbcStatement {

    private JdbcReturningGenerated(
      PreparedStatement preparedStatement, Object[] binds) {
      super(preparedStatement, binds);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Executes the statement as normal, and then concatenates a result with
     * generated values from {@link PreparedStatement#getGeneratedKeys()}.
     * </p>
     * @return
     */
    @Override
    protected Publisher<OracleResultImpl> getResults() {
      return Flux.concat(
        super.execute(),
        jdbcLock.get(() ->
          createQueryResult(preparedStatement.getGeneratedKeys(), adapter)));
    }
  }
}