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
import io.r2dbc.spi.OutParameterMetadata;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.ReadableMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.Type;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleStruct;
import oracle.r2dbc.OracleR2dbcObject;
import oracle.r2dbc.OracleR2dbcObjectMetadata;
import oracle.r2dbc.OracleR2dbcTypes;
import oracle.r2dbc.OracleR2dbcTypes.ObjectType;
import oracle.r2dbc.impl.ReactiveJdbcAdapter.JdbcReadable;
import oracle.r2dbc.impl.ReadablesMetadata.OutParametersMetadataImpl;
import oracle.sql.DATE;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import oracle.sql.TIMESTAMP;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.Struct;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.sql.Statement.CLOSE_ALL_RESULTS;
import static java.sql.Statement.KEEP_CURRENT_RESULT;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static java.util.Objects.requireNonNullElse;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.fromJdbc;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.newNonTransientException;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireOpenConnection;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.runJdbc;
import static oracle.r2dbc.impl.OracleReadableImpl.createOutParameters;
import static oracle.r2dbc.impl.OracleReadableMetadataImpl.createParameterMetadata;
import static oracle.r2dbc.impl.OracleResultImpl.createBatchUpdateErrorResult;
import static oracle.r2dbc.impl.OracleResultImpl.createCallResult;
import static oracle.r2dbc.impl.OracleResultImpl.createErrorResult;
import static oracle.r2dbc.impl.OracleResultImpl.createGeneratedValuesResult;
import static oracle.r2dbc.impl.OracleResultImpl.createQueryResult;
import static oracle.r2dbc.impl.OracleResultImpl.createUpdateCountResult;
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
 * closes cursors after all {@link Result}s emitted by the {@link #execute()}
 * publisher has been fully consumed.
 * </p><p id="fully-consumed-result">
 * To ensure that cursors are eventually closed, application code MUST
 * fully consume every {@link Result} object emitted by the {@link #execute()}
 * {@code Publisher}. A {@code Result} is fully consumed by first subscribing
 * to {@link Result#getRowsUpdated()}, {@link Result#map(BiFunction)},
 * {@link Result#map(Function)}, or {@link Result#flatMap(Function)}, and then
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
   */
  OracleStatementImpl(
    String sql, Duration timeout, Connection jdbcConnection,
    ReactiveJdbcAdapter adapter) {
    this.sql = sql;
    this.jdbcConnection = jdbcConnection;
    this.adapter = adapter;

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
   * </p><pre>
   * SELECT name FROM birthday WHERE month=:x AND day=:x
   * </pre>
   * @throws IllegalArgumentException {@inheritDoc}
   * @throws IllegalArgumentException If the {@code identifier} does match a
   * case-sensitive parameter name that appears in this {@code Statement's}
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
    requireSupportedNullClass(type);
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
   * </p><p>
   * If the specified {@code identifier} matches more than one parameter name,
   * then this method binds the SQL {@code NULL} value to all parameters
   * having a matching name. For instance, when {@code NULL} is bound to the
   * parameter named "x", the following SQL would create a birthday with
   * {@code NULL} values for month and day:
   * </p><pre>
   * INSERT INTO birthday (name, month, day) VALUES ('Plato', :x, :x)
   * </pre>
   * @throws IllegalArgumentException {@inheritDoc}
   * @throws IllegalArgumentException If the {@code identifier} does match a
   * case-sensitive parameter name that appears in this {@code Statement's}
   * SQL command.
   */
  @Override
  public Statement bindNull(String identifier, Class<?> type) {
    requireOpenConnection(jdbcConnection);
    requireNonNull(identifier, "identifier is null");
    requireSupportedNullClass(type);
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
   * @throws IllegalStateException If one or more binds are out-parameters.
   * Returning generated values is not supported when executing a stored
   * procedure.
   * @throws IllegalStateException If one or more binds have been added with
   * {@link #add()}. Returning generated values is not supported when
   * executing a batch DML command.
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
   * result of executing a JDBC PreparedStatement. For typical
   * {@code SELECT, INSERT, UPDATE, and DELETE} commands, a single
   * {@code Result} is published. The {@code Result} will either have
   * zero or more {@link Result.RowSegment}s, or a single
   * {@link Result.UpdateCount} segment, or a single {@link Result.Message}
   * segment if an error occurs. The sections that follow will describe the
   * {@code Result}s published for additional types of SQL that might be
   * executed.
   * </p><p>
   * When this method returns, any bind values previously set on this
   * statement are cleared, and any sets of bind values saved with
   * {@link #add()} are also cleared. Any
   * {@linkplain #fetchSize(int) fetch size} or
   * {@linkplain #returnGeneratedValues(String...) generated values} will be
   * retained between executions.
   * </p>
   * <h3>Executing Batch DML</h3>
   * <p>
   * If a batch of bind values have been {@linkplain #add() added} to this
   * statement, then a single {@code Result} is published. The {@code Result}
   * has an {@link Result.UpdateCount} for each set of added parameters, with
   * the count providing the number of rows effected by those parameters. The
   * order in which {@code UpdateCount}s are published corresponds to the
   * order in which parameters that produced the count were added: The first
   * count is the number of rows effected by the first set of added
   * parameters, the second count is number of rows effected by
   * the second set of added parameters, and so on.
   * </p>
   * <h3>Executing Value Generating DML</h3>
   * <p>
   * If this statement was created with an {@code INSERT} or {@code UPDATE}
   * command, and {@link #returnGeneratedValues(String...)} has configured this
   * statement to return generated values, then a single {@code Result} is
   * published. The {@code Result} has an {@link Result.UpdateCount} segment
   * and one or more {@link Result.RowSegment}s. The update count provides
   * the number of rows effected by the statement, and the row segments provide
   * the values generated for each row that was created or updated.
   * </p><p>
   * If this statement was created with a SQL command that does not return
   * generated values, such as a {@code SELECT} or {@code DELETE}, then the
   * columns specified with {@link #returnGeneratedValues(String...)} are
   * ignored, and {@code Result}s are published as normal, as if
   * {@code returnGeneratedValues} had never been called.
   * </p>
   * <h3>Executing a Stored Procedure</h3>
   * <p>
   * If this statement was created with a stored procedure call (ie: PL/SQL),
   * then a {@code Result} is published for any cursors returned by
   * {@code DBMS_SQL.RETURN_RESULT}, followed by a {@code Result} having an
   * {@link Result.OutSegment} for any out-parameters of the call.
   * When this method returns, any bind values previously set on this
   * statement are cleared, and any sets of bind values saved with
   * {@link #add()} are also cleared.
   * </p><p>
   * The returned publisher initiates SQL execution <i>the first time</i> a
   * subscriber subscribes, before the subscriber emits a {@code request}
   * signal. The returned publisher does not support multiple subscribers. After
   * one subscriber has subscribed, the publisher signals {@code onError}
   * with {@code IllegalStateException} to all subsequent subscribers.
   * </p>
   *
   * @implNote
   * <p>
   * <b>
   * The 21.1 Oracle JDBC Driver does not determine a fetch size based on demand
   * signalled with {@link org.reactivestreams.Subscription#request(long)}.
   * </b>
   * Oracle JDBC will use a fixed fetch size specified with
   * {@link #fetchSize(int)}. If no fetch size is specified, Oracle JDBC will
   * use a default fixed fetch size.
   * </p><p>
   * When executing queries that return a large number of rows, programmers
   * are advised to configure the amount of rows that Oracle JDBC should
   * fetch and buffer by calling {@link #fetchSize(int)}.
   * </p><p>
   * A later release of Oracle JDBC may implement dynamic fetch sizes that are
   * adjusted to based on {@code request} signals from the subscriber.
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
    Object[] currentBinds = transferBinds();

    return adapter.getLock().get(() -> {
      PreparedStatement preparedStatement =
        jdbcConnection.prepareStatement(sql);
      configureJdbcStatement(preparedStatement, currentFetchSize, timeout);
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

    return adapter.getLock().get(() -> {
      PreparedStatement preparedStatement =
        jdbcConnection.prepareStatement(sql);
      configureJdbcStatement(preparedStatement, currentFetchSize, timeout);
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
    Object[] currentBinds = transferBinds();

    return adapter.getLock().get(() -> {
      CallableStatement callableStatement = jdbcConnection.prepareCall(sql);
      configureJdbcStatement(callableStatement, currentFetchSize, timeout);
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
    Object[] currentBinds = transferBinds();
    String[] currentGeneratedColumns = generatedColumns.clone();

    return adapter.getLock().get(() -> {
      PreparedStatement preparedStatement =
        currentGeneratedColumns.length == 0
          ? jdbcConnection.prepareStatement(sql, RETURN_GENERATED_KEYS)
          : jdbcConnection.prepareStatement(sql, currentGeneratedColumns);
      configureJdbcStatement(preparedStatement, currentFetchSize, timeout);
      return new JdbcReturningGenerated(preparedStatement, currentBinds);
    });
  }

  /**
   * Configures a JDBC Statement with values that have been configured on an
   * R2DBC Statement.
   *
   * @param statement The statement to configure. Not null.
   *
   * @param fetchSize Configuration of {@link #fetchSize(int)}, possibly 0 if
   * a default size should be used.
   *
   * @param queryTimeout Configuration of {@link #timeout}, possibly 0 if no
   * timeout should be used.
   *
   * @throws SQLException If the JDBC statement is closed.
   */
  private static void configureJdbcStatement(
    java.sql.Statement statement, int fetchSize, int queryTimeout)
    throws SQLException {

    // It is noted that Oracle JDBC's feature of auto-tuning fetch sizes will
    // be disabled if 0 is passed to setFetchSize. Perhaps similar behavior
    // occurs with methods like setQueryTimeout as well? To be sure, don't call
    // any methods unless non-default values are set.

    if (fetchSize != 0) {
      statement.setFetchSize(fetchSize);
    }

    if (queryTimeout != 0) {
      statement.setQueryTimeout(queryTimeout);
    }
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
      if (! batch.isEmpty())
        throw outParameterWithBatch();
      if (generatedColumns != null)
        throw outParameterWithGeneratedValues();
    }

    requireSupportedSqlType(requireNonNull(
      parameter.getType(), "Parameter type is null"));
    requireSupportedJavaType(parameter.getValue());
    bindValues[index] = parameter;
  }

  /**
   * Checks that the specified 0-based {@code index} is within the range of
   * valid parameter indexes for this statement.
   * @param index A 0-based parameter index
   * @throws IndexOutOfBoundsException If the {@code index} is not within the
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
  private Object[] transferBinds() {
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
   * Checks that the class of an {@code object} is supported as a bind value.
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
   * Checks that the SQL type identified by an {@code r2dbcType} is supported
   * as a bind parameter.
   * @param r2dbcType SQL type. Not null.
   * @throws IllegalArgumentException If the SQL type is not supported.
   */
  private static void requireSupportedSqlType(Type r2dbcType) {
    SQLType jdbcType = toJdbcType(r2dbcType);

    if (jdbcType == null)
      throw new IllegalArgumentException("Unsupported SQL type: " + r2dbcType);
  }

  /**
   * Checks that a class is supported as a null bind type. Oracle JDBC
   * requires a type name when binding a null value for a user defined type, so
   * this method checks for that.
   * @param type Class type to check. May be null.
   * @throws IllegalArgumentException If the class type is null or is the
   * default mapping for a named type.
   */
  private static void requireSupportedNullClass(Class<?> type) {
    requireNonNull(type, "type is null");

    if (OracleR2dbcObject.class.isAssignableFrom(type)) {
      throw new IllegalArgumentException(
        "A type name is required for NULL OBJECT binds. Use: " +
          "bind(Parameters.in(OracleR2dbcTypes.objectType(typeName)))");
    }

    // For backwards compatibility, allow Class<byte[]> for NULL RAW binds
    if (type.isArray() && !byte[].class.equals(type)) {
      throw new IllegalArgumentException(
        "A type name is required for NULL ARRAY binds. Use: " +
          "bind(Parameters.in(OracleR2dbcTypes.arrayType(typeName)))");
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
   * returns out-parameters, or a batch of DML update counts.
   * </p><p>
   * The base class ensures that all resources allocated for the statement
   * execution are eventually deallocated. This includes the
   * {@link #preparedStatement}, along with resources allocated for bind
   * values, such as {@code java.sql.Blob/Clob} objects.
   * </p>
   */
  private class JdbcStatement {

    /** The {@code PreparedStatement} that is executed */
    protected final PreparedStatement preparedStatement;

    /**
     * Collection of results that depend on the JDBC statement to remain open
     * until they are consumed. For instance, a result that retains a
     * {@code ResultSet} would depend on the JDBC statement to remain open, as
     * the {@code ResultSet} is closed when the JDBC statement is closed.
      */
    protected final DependentCounter dependentCounter;

    /** The bind values that are set on the {@link #preparedStatement} */
    protected final Object[] binds;

    /**
     * Publisher that deallocate resources after the
     * {@link #preparedStatement} is executed
     */
    private Publisher<Void> deallocators = Mono.empty();

    /**
     * Constructs a new {@code JdbcStatement} that executes a
     * {@code preparedStatement} with the given {@code binds}.
     * @param preparedStatement Statement to execute. Not null. Retained.
     * @param binds Bind values. Not null. Retained.
     */
    private JdbcStatement(PreparedStatement preparedStatement, Object[] binds) {
      this.preparedStatement = preparedStatement;
      this.binds = binds;

      // Add this statement as a "party" (think j.u.c.Phaser) to the dependent
      // results by calling increment(). After the Result publisher returned by
      // execute() terminates, this statement "arrives" by calling decrement().
      // Calling decrement() after the Result publisher terminates ensures that
      // the JDBC statement can not be closed until all results have had a
      // chance to be emitted to user code.
      dependentCounter = new DependentCounter(closeStatement());
      dependentCounter.increment();
    }

    /**
     * <p>
     * Executes this statement and returns a publisher that emits the results.
     * </p><p>
     * This method first subscribes to the {@link #bind()} publisher, and then
     * subscribes to the {@link #executeJdbc()} publisher after the bind
     * publisher has completed. Subclasses may override the {@code bind} and
     * {@code getResults} methods as needed for different types of binds and
     * results.
     * </p><p>
     * This method is implemented to create {@code Results} of
     * {@link Result.Message} segments if an {@link R2dbcException} is
     * emitted from the {@code bind} or {@code getResults} publishers, or if
     * {@link PreparedStatement#getWarnings()} yields a warning.
     * </p><p>
     * The {@link #preparedStatement} can only be closed after all results that
     * depend on it have been consumed by user code. It is not guaranteed that
     * every result created by this statement will actually reach user code; A
     * cancel signal may occur at any time. Upon cancellation, no more signals
     * are emitted downstream. For this reason, the
     * {@link OracleResultImpl#addDependent()} method must be called only when
     * it is certain that a result will reach the downstream subscriber. This
     * certainty is offered by the {@link Flux#doOnNext(Consumer)} operator.
     * </p>
     * @return A publisher that emits the result of executing this statement.
     * Not null.
     */
    final Publisher<OracleResultImpl> execute() {

      Publisher<Void> deallocate = deallocate();

      return Publishers.concatTerminal(
        Mono.from(bind())
          .thenMany(executeJdbc())
          .map(this::getWarnings)
          .onErrorResume(R2dbcException.class, r2dbcException ->
            Mono.just(createErrorResult(r2dbcException)))
          .doOnNext(OracleResultImpl::addDependent),
        deallocate);
    }

    /**
     * <p>
     * Sets {@link #binds} on the {@link #preparedStatement}, as specified by
     * {@link #bind(Object[])}. This method is called when this statement is
     * executed. Subclasess override this method to perform additional actions.
     * </p>
     * @return A {@code Publisher} that emits {@code onComplete} when all
     * {@code binds} have been set.
     */
    protected Publisher<Void> bind() {
      return bind(binds);
    }

    /**
     * <p>
     * Sets the given {@code binds} on the {@link #preparedStatement}. The
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
    protected final Publisher<Void> bind(Object[] binds) {
      return adapter.getLock().flatMap(() -> {

        List<Publisher<Void>> bindPublishers = null;

        for (int i = 0; i < binds.length; i++) {

          // Out binds are handled in the registerOutParameters method of the
          // JdbcCall subclass.
          if (binds[i] instanceof Parameter.Out
            && !(binds[i] instanceof Parameter.In))
            continue;

          Publisher<Void> bindPublisher = bind(i, binds[i]);

          if (bindPublisher != null) {
            if (bindPublishers == null)
              bindPublishers = new LinkedList<>();

            bindPublishers.add(bindPublisher);
          }
        }

        return bindPublishers == null
          ? Mono.empty()
          : Flux.concat(bindPublishers);
      });
    }

    /**
     * Binds a value to the {@link #preparedStatement}. This method may convert
     * the given {@code value} into an object type that is accepted by JDBC. If
     * value is materialized asynchronously, such as a Blob or Clob, then this
     * method returns a publisher that completes when the materialized value is
     * bound.
     * @param index zero-based bind index
     * @param value value to bind. May be null.
     * @return A publisher that completes when the value is bound, or null if
     * the value is bound synchronously.
     * @implNote The decision to return a null publisher rather than an empty
     * publisher is motivated by reducing object allocation and overhead from
     * subscribe/onSubscribe/onComplete. It is thought that this overhead would
     * be substantial if it were incurred for each bind, of each statement, of
     * each connection.
     */
    private Publisher<Void> bind(int index, Object value) {

      final Object jdbcValue = convertBind(value);

      final SQLType jdbcType;
      final String typeName;

      if (value instanceof Parameter) {
        // Convert the parameter's R2DBC type to a JDBC type. Get the type name
        // by calling getName() on the R2DBC type, not the JDBC type; This
        // ensures that a user defined name will be used, such as one from
        // OracleR2dbcTypes.ArrayType.getName()
        Type type = ((Parameter)value).getType();
        jdbcType = toJdbcType(type);
        typeName = type.getName();
      }
      else {
        // JDBC will infer the type from the class of jdbcValue
        jdbcType = null;
        typeName = null;
      }

      if (jdbcValue instanceof Publisher<?>) {
        return setPublishedBind(index, (Publisher<?>) jdbcValue);
      }
      else {
        setBind(index, jdbcValue, jdbcType, typeName);
        return null;
      }
    }

    /**
     * Binds a published value to the {@link #preparedStatement}. The binding
     * happens asynchronously. The returned publisher that completes after the
     * value is published <em>and bound</em>
     * @param index zero based bind index
     * @param publisher publisher that emits a bound value.
     * @return A publisher that completes after the published value is bound.
     */
    private Publisher<Void> setPublishedBind(
      int index, Publisher<?> publisher) {
      return Mono.from(publisher)
        .flatMap(value -> {
          Publisher<Void> bindPublisher = bind(index, value);
          return bindPublisher == null
            ? Mono.empty()
            : Mono.from(bindPublisher);
        });
    }

    /**
     * Executes the JDBC {@link #preparedStatement} and maps the
     * results into R2DBC {@link Result} objects. The base class implements
     * this method to get results of update count, row data, or implicit
     * results (ie: DBMS_SQL.RETURN_RESULT). Subclasses may override this
     * method to produce different types of results.
     * @return A publisher that emits the results.
     */
    protected Publisher<OracleResultImpl> executeJdbc() {
      return Mono.from(adapter.publishSQLExecution(preparedStatement))
        .flatMapMany(this::getResults);
    }

    /**
     * Publishes the current result of the {@link #preparedStatement}, along
     * with any results that follow after calling
     * {@link PreparedStatement#getMoreResults()}
     *
     * @param isResultSet {@code true} if the current result is a
     * {@code ResultSet}, otherwise {@code false}.
     * @return A publisher that emits all results of the
     * {@code preparedStatement}
     */
    protected final Publisher<OracleResultImpl> getResults(
      boolean isResultSet) {

      return adapter.getLock().flatMap(() -> {

        OracleResultImpl result = getCurrentResult(isResultSet);
        OracleResultImpl nextResult = getCurrentResult(
          preparedStatement.getMoreResults(KEEP_CURRENT_RESULT));

        // Don't allocate a list unless there are multiple results. Multiple
        // results should only happen when using DBMS_SQL.RETURN_RESULT
        // within a PL/SQL call
        if (nextResult == null) {
          return Mono.justOrEmpty(result);
        }
        else {
          ArrayList<OracleResultImpl> results = new ArrayList<>();

          // The first result may be null if additional results follow
          if (result != null)
            results.add(result);

          while (nextResult != null) {
            results.add(nextResult);

            nextResult = getCurrentResult(
              preparedStatement.getMoreResults(KEEP_CURRENT_RESULT));
          }
          return Flux.fromIterable(results);
        }
      });
    }

    /**
     * Adds a {@code publisher} for deallocating a resource that this
     * statement has allocated. The {@code publisher} is subscribed to after
     * this statement has executed, possibly before all results have been
     * consumed. If multiple dealloaction publishers are added, each one is
     * subscribed to sequentially, and errors emitted by the publishers are
     * suppressed until all publishers have been subscribed to.
     * @param publisher Resource deallocation publisher
     */
    protected void addDeallocation(Publisher<Void> publisher) {
      deallocators = Flux.concatDelayError(deallocators, publisher);
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
     */
    private OracleResultImpl getCurrentResult(boolean isResultSet) {
      return fromJdbc(() -> {
        if (isResultSet) {
          return createQueryResult(
            dependentCounter, preparedStatement.getResultSet(), adapter);
        }
        else {
          long updateCount = preparedStatement.getLargeUpdateCount();
          return updateCount >= 0
            ? createUpdateCountResult(updateCount)
            : null;
        }
      });
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
    private OracleResultImpl getWarnings(OracleResultImpl result) {
      return fromJdbc(() -> {
        SQLWarning warning = preparedStatement.getWarnings();
        preparedStatement.clearWarnings();
        return warning == null
          ? result
          : OracleResultImpl.createWarningResult(sql, warning, result);
      });
    }

    /**
     * <p>
     * Deallocates all resources that have been allocated by this statement.
     * If the deallocation of any resource results in an error, an attempt is
     * made to deallocate any remaining resources before emitting the error.
     * </p><p>
     * The returned publisher subscribes to the {@link #deallocators}
     * publisher, and may close the {@link #preparedStatement} if all results
     * have already been consumed when this method is called. This method
     * calls the {@code decrement()} method of {@link #dependentCounter}, in
     * balance with the {@code increment()} call that occur in the constructor
     * of this statement.
     * </p>
     * @return A publisher that completes when all resources have been
     * deallocated
     */
    private Publisher<Void> deallocate() {
      addDeallocation(dependentCounter.decrement());
      return deallocators;
    }

    /**
     * @return A publisher that closes the JDBC {@link #preparedStatement} when
     * subscribed to. Not null.
     */
    private Publisher<Void> closeStatement() {
      return adapter.getLock().run(() -> {
        try {
          // Workaround Oracle JDBC bug #34545179: ResultSet references are
          // retained even when the statement is closed. Calling getMoreResults
          // with the CLOSE_ALL_RESULTS argument forces the driver to
          // de-reference them.
          preparedStatement.getMoreResults(CLOSE_ALL_RESULTS);
        }
        catch (SQLException sqlException) {
          // It may be the case that the JDBC connection was closed, and so the
          // statement was closed with it. Check for this, and ignore the
          // SQLException if so.
          if (!jdbcConnection.isClosed())
            throw sqlException;
        }

        preparedStatement.close();
      });
    }

    /**
     * Sets the {@code value} of a {@code preparedStatement} parameter at the
     * specified {@code index}. If a non-null {@code type} is provided, then it is
     * specified as the SQL type for the bind. Otherwise, if the
     * {@code type} is {@code null}, then the JDBC driver infers the SQL type
     * of the bind.
     * @param index 0-based parameter index
     * @param value Value. May be null.
     * @param type SQL type. May be null.
     * @param typeName Name of a user defined type. May be null.
     */
    private void setBind(
      int index, Object value, SQLType type, String typeName) {
      runJdbc(() -> {
        int jdbcIndex = index + 1;

        if (type == null) {
          preparedStatement.setObject(jdbcIndex, value);
        }
        else if (value == null) {
          preparedStatement.setNull(
            jdbcIndex, type.getVendorTypeNumber(), typeName);
        }
        else {
          preparedStatement.setObject(jdbcIndex, value, type);
        }
      });
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
      else if (value instanceof io.r2dbc.spi.Blob) {
        return convertBlobBind((io.r2dbc.spi.Blob) value);
      }
      else if (value instanceof io.r2dbc.spi.Clob) {
        return convertClobBind((io.r2dbc.spi.Clob) value);
      }
      else if (value instanceof ByteBuffer) {
        return convertByteBufferBind((ByteBuffer) value);
      }
      else if (value instanceof Parameter) {
        return convertParameterBind((Parameter) value);
      }
      else if (value instanceof OracleR2dbcObject) {
        return convertObjectBind((OracleR2dbcObject) value);
      }
      else {
        return value;
      }
    }

    /** Converts a {@code Parameter} bind value to a JDBC bind value */
    private Object convertParameterBind(Parameter parameter) {
      Object value = parameter.getValue();

      if (value == null)
        return null;

      Type type = parameter.getType();

      if (type instanceof OracleR2dbcTypes.ArrayType) {
        return convertArrayBind((OracleR2dbcTypes.ArrayType) type, value);
      }
      else if (type instanceof ObjectType) {
        return convertObjectBind((ObjectType) type, value);
      }
      else {
        return convertBind(value);
      }
    }

    /**
     * Converts a given {@code value} to a JDBC {@link Array} of the given
     * {@code type}.
     */
    private Array convertArrayBind(
      OracleR2dbcTypes.ArrayType type, Object value) {

      // TODO: createOracleArray executes a blocking database call the first
      //  time an OracleArray is created for a given type name. Subsequent
      //  creations of the same type avoid the database call using a cached type
      //  descriptor. If possible, rewrite this use a non-blocking call.
      return fromJdbc(() ->
        jdbcConnection.unwrap(OracleConnection.class)
          .createOracleArray(type.getName(), convertJavaArray(value)));
    }

    /**
     * Converts a bind value for an ARRAY to a Java type that is supported by
     * Oracle JDBC. This method handles cases for standard type mappings of
     * R2DBC and extended type mapping Oracle R2DBC which are not supported by
     * Oracle JDBC.
     */
    private Object convertJavaArray(Object array) {

      if (array == null)
        return null;

      // TODO: R2DBC drivers are only required to support ByteBuffer to
      //  VARBINARY (ie: RAW) conversions. However, a programmer might want to
      //  use byte[][] as a bind for an ARRAY of RAW. If that happens, they
      //  might hit this code by accident. Ideally, they can bind ByteBuffer[]
      //  instead. If absolutely necessary, Oracle R2DBC can do a type look up
      //  on the ARRAY and determine if the base type is RAW or NUMBER.
      if (array instanceof byte[]) {
        // Convert byte to NUMBER. Oracle JDBC does not support creating SQL
        // arrays from a byte[], so convert the byte[] to an short[].
        byte[] bytes = (byte[])array;
        short[] shorts = new short[bytes.length];
        for (int i = 0; i < bytes.length; i++)
          shorts[i] = (short)(0xFF & bytes[i]);

        return shorts;
      }
      else if (array instanceof ByteBuffer[]) {
        // Convert from R2DBC's ByteBuffer representation of binary data into
        // JDBC's byte[] representation
        ByteBuffer[] byteBuffers = (ByteBuffer[]) array;
        byte[][] byteArrays = new byte[byteBuffers.length][];
        for (int i = 0; i < byteBuffers.length; i++) {
          ByteBuffer byteBuffer = byteBuffers[i];
          byteArrays[i] = byteBuffer == null
            ? null
            : convertByteBufferBind(byteBuffers[i]);
        }

        return byteArrays;
      }
      else if (array instanceof Period[]) {
        // Convert from Oracle R2DBC's Period representation of INTERVAL YEAR TO
        // MONTH to Oracle JDBC's INTERVALYM representation.
        Period[] periods = (Period[]) array;
        INTERVALYM[] intervalYearToMonths = new INTERVALYM[periods.length];
        for (int i = 0; i < periods.length; i++) {
          Period period = periods[i];
          if (period == null) {
            intervalYearToMonths[i] = null;
          }
          else {
            // The binary representation is specified in the JavaDoc of
            // oracle.sql.INTERVALYM. In 21.x, the JavaDoc has bug: It neglects
            // to mention that the year value is offset by 0x80000000
            byte[] bytes = new byte[5];
            ByteBuffer.wrap(bytes)
              .putInt(period.getYears() + 0x80000000) // 4 byte year
              .put((byte)(period.getMonths() + 60)); // 1 byte month
            intervalYearToMonths[i] = new INTERVALYM(bytes);
          }
        }

        return intervalYearToMonths;
      }
      else {
        // Check if the bind value is a multidimensional array
        Class<?> componentType = array.getClass().getComponentType();

        if (componentType == null || !componentType.isArray())
          return array;

        int length = java.lang.reflect.Array.getLength(array);
        Object[] converted = new Object[length];

        for (int i = 0; i < length; i++) {
          converted[i] =
            convertJavaArray(java.lang.reflect.Array.get(array, i));
        }

        return converted;
      }
    }

    /**
     * Converts a given {@code object} to a JDBC {@link Struct} of OBJECT type.
     */
    private Object convertObjectBind(OracleR2dbcObject object) {
      return convertObjectBind(
        object.getMetadata().getObjectType(),
        object);
    }

    /**
     * Converts a given {@code value} to a JDBC {@link Struct} of the given
     * OBJECT {@code type}.
     */
    private Object convertObjectBind(ObjectType type, Object value) {

      if (value == null)
        return null;

      final Object[] attributes;

      OracleR2dbcObjectMetadata metadata =
        ReadablesMetadata.createAttributeMetadata(fromJdbc(() ->
          (OracleStruct)jdbcConnection.createStruct(type.getName(), null)));

      if (value instanceof Object[]) {
        attributes = toAttributeArray((Object[])value, metadata);
      }
      else if (value instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> valueMap = (Map<String, Object>)value;
        attributes = toAttributeArray(valueMap, metadata);
      }
      else if (value instanceof io.r2dbc.spi.Readable) {
        attributes = toAttributeArray((io.r2dbc.spi.Readable)value, metadata);
      }
      else {
        // Fallback to a built-in conversion supported by Oracle JDBC
        return value;
      }

      Publisher<Void> conversionPublisher =
        convertUdtValues(attributes, metadata);

      OracleR2dbcExceptions.JdbcSupplier<Struct> structSupplier = () ->
        jdbcConnection.unwrap(OracleConnection.class)
          .createStruct(type.getName(), attributes);

      if (conversionPublisher == null) {
        return structSupplier.get();
      }
      else {
        return Mono.from(conversionPublisher)
          .then(Mono.fromSupplier(structSupplier));
      }
    }

    /**
     * Copies values of an {@code Object[]} from user code into an
     * {@code Object[]} of a length equal to the number of attributes in an
     * OBJECT described by {@code metadata}.
     * @throws IllegalArgumentException If the length of the array from user
     * code is not equal to the number of attributes in the OBJECT type.
     */
    private Object[] toAttributeArray(
      Object[] values, OracleR2dbcObjectMetadata metadata) {

      List<? extends ReadableMetadata> metadatas =
        metadata.getAttributeMetadatas();

      if (values.length != metadatas.size()) {
        throw new IllegalArgumentException(format(
          "Length of Object[]: %d, does not match the number of attributes" +
            " in OBJECT %s: %d",
          values.length,
          metadata.getObjectType().getName(),
          metadatas.size()));
      }

      return values.clone();
    }

    /**
     * Copies values of a {@code Map} from user code into an {@code Object[]}
     * having a length equal to the number of attributes in an OBJECT described
     * by {@code metadata}.
     * @throws IllegalArgumentException If the keys of the Map do not match the
     * attribute names of the OBJECT.
     */
    private Object[] toAttributeArray(
      Map<String, Object> values, OracleR2dbcObjectMetadata metadata) {

      TreeMap<String, Object> treeMap =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      treeMap.putAll(values);

      List<? extends ReadableMetadata> metadatas =
        metadata.getAttributeMetadatas();

      Set<String> remainingNames = treeMap.keySet();
      Object[] attributes = new Object[metadatas.size()];
      for (int i = 0; i < attributes.length; i++) {
        String attributeName = metadatas.get(i).getName();
        Object attribute = treeMap.get(attributeName);

        if (attribute == null && !treeMap.containsKey(attributeName)) {
          throw new IllegalArgumentException(format(
            "Map contains no key for attribute %s of OBJECT %s",
            attributeName,
            metadata.getObjectType().getName()));
        }
        else {
          remainingNames.remove(attributeName);
        }

        attributes[i] = attribute;
      }

      if (!remainingNames.isEmpty()) {
        throw new IllegalArgumentException(format(
          "Map contains keys: [%s], which do not match any attribute" +
            " names of OBJECT %s: [%s]",
          String.join(",", remainingNames),
          metadata.getObjectType().getName(),
          metadatas.stream()
            .map(ReadableMetadata::getName)
            .collect(Collectors.joining(","))));
      }

      return attributes;
    }

    /**
     * Copies values of an {@code io.r2dbc.spi.Readable} from user code into an
     * {@code Object[]} of a length equal to the number of attributes in an
     * OBJECT described by {@code metadata}.
     * @implNote This method does not require the Readable to be an
     * {@code OracleR2dbcObject}. This is done to allow mapping of row or out
     * parameter values into OBJECT values.
     * @throws IllegalArgumentException If the number of values in the Readable
     * is not equal to the number of attributes in the OBJECT type.
     */
    private Object[] toAttributeArray(
      io.r2dbc.spi.Readable readable, OracleR2dbcObjectMetadata metadata) {

      Object[] attributes = new Object[metadata.getAttributeMetadatas().size()];

      for (int i = 0; i < attributes.length; i++) {
        try {
          attributes[i] = readable.get(i);
        }
        catch (IndexOutOfBoundsException indexOutOfBoundsException) {
          throw new IllegalArgumentException(format(
            "Readable contains less values than the number of attributes, %d," +
              " in OBJECT %s",
            attributes.length,
            metadata.getObjectType().getName()));
        }
      }

      try {
        readable.get(attributes.length);
        throw new IllegalArgumentException(format(
          "Readable contains more values than the number of attributes, %d," +
            " in OBJECT %s",
          attributes.length,
          metadata.getObjectType().getName()));
      }
      catch (IndexOutOfBoundsException indexOutOfBoundsException) {
        // An out-of-bound index is expected if the number of values in the
        // Readable matches the number of attributes in the OBJECT.
      }

      return attributes;
    }

    private Publisher<Void> convertUdtValues(
      Object[] values, OracleR2dbcObjectMetadata metadata) {

      LinkedList<Publisher<Void>> publishers = null;

      for (int i = 0; i < values.length; i++) {

        // Apply any conversion for objects not supported by the setObject
        // methods of OraclePreparedStatement.
        values[i] = convertBind(values[i]);

        // Apply any conversion for objects not supported by the
        // createOracleArray or createStruct methods of OracleConnection
        if (values[i] instanceof Period) {
          values[i] = convertPeriodUdtValue((Period) values[i]);
        }
        else if (values[i] instanceof LocalDateTime) {
          values[i] = convertLocalDateTimeUdtValue((LocalDateTime) values[i]);
        }
        else if (values[i] instanceof LocalDate) {
          values[i] = convertLocalDateUdtValue((LocalDate) values[i]);
        }
        else if (values[i] instanceof LocalTime) {
          values[i] = convertLocalTimeUdtValue((LocalTime) values[i]);
        }
        else if (values[i] instanceof Duration) {
          values[i] = convertDurationUdtValue((Duration) values[i]);
        }
        else if (values[i] instanceof byte[]
          && R2dbcType.BLOB.equals(
            metadata.getAttributeMetadata(i).getType())) {
          values[i] = convertBlobUdtValue((byte[]) values[i]);
        }
        else if (values[i] instanceof CharSequence
          && R2dbcType.CLOB.equals(
            metadata.getAttributeMetadata(i).getType())) {
          values[i] = convertClobUdtValue((CharSequence) values[i]);
        }

        // Check if the value is published asynchronously (ie: a BLOB or CLOB)
        if (values[i] instanceof Publisher<?>) {
          if (publishers == null)
            publishers = new LinkedList<>();

          final int valueIndex = i;
          publishers.add(
            Mono.from((Publisher<?>)values[i])
              .doOnSuccess(value -> values[valueIndex] = value)
              .then());
        }
      }

      return publishers == null
        ? null
        : Flux.concat(publishers);
    }
    /**
     * Converts a {@code LocalDateTime} to a {@code TIMESTAMP} object. This
     * conversion is only required when passing an {@code Object[]} to the
     * {@code createOracleArray} or {@code createStruct} methods of
     * {@link OracleConnection}. A built-in conversion is implemented by Oracle
     * JDBC for the {@code setObject} methods of {@link PreparedStatement}.
     */
    private TIMESTAMP convertLocalDateTimeUdtValue(LocalDateTime localDateTime) {
      return fromJdbc(() -> TIMESTAMP.of(localDateTime));
    }

    /**
     * Converts a {@code LocalDate} to a {@code DATE} object. This
     * conversion is only required when passing an {@code Object[]} to the
     * {@code createOracleArray} or {@code createStruct} methods of
     * {@link OracleConnection}. A built-in conversion is implemented by Oracle
     * JDBC for the {@code setObject} methods of {@link PreparedStatement}.
     */
    private DATE convertLocalDateUdtValue(LocalDate localDate) {
      return fromJdbc(() -> DATE.of(localDate));
    }

    /**
     * Converts a {@code LocalTime} to a {@code TIMESTAMP} object. This
     * conversion is only required when passing an {@code Object[]} to the
     * {@code createOracleArray} or {@code createStruct} methods of
     * {@link OracleConnection}. A built-in conversion is implemented by Oracle
     * JDBC for the {@code setObject} methods of {@link PreparedStatement}.
     * @implNote Mapping this to TIMESTAMP to avoid loss of precision. Other
     * object types like DATE or java.sql.Time do not have nanosecond precision.
     */
    private TIMESTAMP convertLocalTimeUdtValue(LocalTime localTime) {
      return fromJdbc(() ->
        TIMESTAMP.of(LocalDateTime.of(LocalDate.EPOCH, localTime)));
    }

    /**
     * Converts a {@code Duration} to an {@code INTERVALDS} object. This
     * conversion is only required when passing an {@code Object[]} to the
     * {@code createOracleArray} or {@code createStruct} methods of
     * {@link OracleConnection}. A built-in conversion is implemented by Oracle
     * JDBC for the {@code setObject} methods of {@link PreparedStatement}.
     */
    private INTERVALDS convertDurationUdtValue(Duration duration) {
      // The binary representation is specified in the JavaDoc of
      // oracle.sql.INTERVALDS. In 21.x, the JavaDoc has bug: It neglects
      // to mention that the day and fractional second values are offset by
      // 0x80000000
      byte[] bytes = new byte[11];
      ByteBuffer.wrap(bytes)
        .putInt((int)(duration.toDaysPart()) + 0x80000000)// 4 byte day
        .put((byte)(duration.toHoursPart() + 60))// 1 byte hour
        .put((byte)(duration.toMinutesPart() + 60))// 1 byte minute
        .put((byte)(duration.toSecondsPart() + 60))// 1 byte second
        .putInt(duration.toNanosPart() + 0x80000000);// 4 byte fractional second
      return new INTERVALDS(bytes);
    }

    /**
     * Converts a {@code Period} to an {@code INTERVALYM} object. This
     * conversion is only required when passing an {@code Object[]} to the
     * {@code createOracleArray} or {@code createStruct} methods of
     * {@link OracleConnection}. A built-in conversion is implemented by Oracle
     * JDBC for the {@code setObject} methods of {@link PreparedStatement}.
     */
    private INTERVALYM convertPeriodUdtValue(Period period) {
      // The binary representation is specified in the JavaDoc of
      // oracle.sql.INTERVALYM. In 21.x, the JavaDoc has bug: It neglects
      // to mention that the year value is offset by 0x80000000
      byte[] bytes = new byte[5];
      ByteBuffer.wrap(bytes)
        .putInt(period.getYears() + 0x80000000) // 4 byte year
        .put((byte)(period.getMonths() + 60)); // 1 byte month
      return new INTERVALYM(bytes);
    }

    /**
     * Converts a {@code byte[]} to a {@code java.sql.Blob} object. This
     * conversion is only required when passing an {@code Object[]} to the
     * {@code createOracleArray} or {@code createStruct} methods of
     * {@link OracleConnection}. A built-in conversion is implemented by Oracle
     * JDBC for the {@code setObject} methods of {@link PreparedStatement}.
     */
    private Publisher<java.sql.Blob> convertBlobUdtValue(byte[] bytes) {
      return convertBlobBind(Blob.from(Mono.just(ByteBuffer.wrap(bytes))));
    }

    /**
     * Converts a {@code String} to a {@code java.sql.Clob} object. This
     * conversion is only required when passing an {@code Object[]} to the
     * {@code createOracleArray} or {@code createStruct} methods of
     * {@link OracleConnection}. A built-in conversion is implemented by Oracle
     * JDBC for the {@code setObject} methods of {@link PreparedStatement}.
     */
    private Publisher<java.sql.Clob> convertClobUdtValue(
      CharSequence charSequence) {
      return convertClobBind(Clob.from(Mono.just(charSequence)));
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
        adapter.getLock().get(jdbcConnection::createBlob),
        jdbcBlob ->
          Mono.from(adapter.publishBlobWrite(r2dbcBlob.stream(), jdbcBlob))
            .thenReturn(jdbcBlob),
        jdbcBlob -> {
          addDeallocation(adapter.publishBlobFree(jdbcBlob));
          return r2dbcBlob.discard();
        });
    }

    /**
     * <p>
     * Converts an R2DBC Clob to a JDBC Clob. The returned {@code Publisher}
     * asynchronously writes the {@code r2dbcClob} content to a JDBC Clob and
     * then emits the JDBC Clob after all content has been written. The JDBC
     * Clob allocates a temporary database Clob that is freed by a
     * {@code Publisher} added to the {@code discardQueue}.
     * </p><p>
     * This method allocates an {@code NClob} in order to have to JDBC
     * encode the data with a unicode character set.
     * </p>
     * @param r2dbcClob An R2DBC Clob. Not null. Retained.
     * @return A JDBC Clob. Not null.
     */
    private Publisher<java.sql.Clob> convertClobBind(
      io.r2dbc.spi.Clob r2dbcClob) {
      return Mono.usingWhen(
        adapter.getLock().get(jdbcConnection::createNClob),
        jdbcClob ->
          Mono.from(adapter.publishClobWrite(r2dbcClob.stream(), jdbcClob))
            .thenReturn(jdbcClob),
        jdbcClob -> {
          addDeallocation(adapter.publishClobFree(jdbcClob));
          return r2dbcClob.discard();
        });
    }

    /**
     * Converts a ByteBuffer to a byte array. The {@code byteBuffer} contents,
     * delimited by its position and limit, are copied into the returned byte
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
     * Constructs a new {@code JdbcCall} that executes a
     * {@code callableStatement} with the given {@code bindValues} and
     * {@code parameterNames}.
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

        // Use the parameter name, or the index if the parameter is unnamed
        String name = requireNonNullElse(
          parameterNames.get(bindIndex), String.valueOf(i));
        metadataArray[i] = createParameterMetadata(
          name, ((Parameter)bindValues[bindIndex]).getType());
      }

      this.metadata = createOutParametersMetadata(metadataArray);
    }

    @Override
    protected Publisher<Void> bind() {
      return Flux.concat(super.bind(), registerOutParameters());
    }

    /**
     * Invokes {@link CallableStatement#registerOutParameter(int, int)} to
     * register each instance of {@link Parameter.Out} in the given
     * {@code values}
     * @return A publisher that completes when all out-parameter binds are
     * registered.
     */
    private Publisher<Void> registerOutParameters() {
      return adapter.getLock().run(() -> {
        CallableStatement callableStatement =
          preparedStatement.unwrap(CallableStatement.class);

        for (int i : outBindIndexes) {
          Type type = ((Parameter) binds[i]).getType();
          SQLType jdbcType = toJdbcType(type);

          if (type instanceof OracleR2dbcTypes.ArrayType
              || type instanceof OracleR2dbcTypes.ObjectType) {
            // Call registerOutParameter with the user defined type name
            // returned by Type.getName(). Oracle JDBC throws an exception if a
            // name is provided for a built-in type, like VARCHAR, etc. So
            // this branch should only be taken for user defined types, like
            // ARRAY or OBJECT.
            callableStatement.registerOutParameter(
              i + 1, jdbcType, type.getName());
          }
          else {
            callableStatement.registerOutParameter(i + 1, jdbcType);
          }
        }
      });
    }

    @Override
    protected Publisher<OracleResultImpl> executeJdbc() {
      return Flux.concat(
        super.executeJdbc(),
        Mono.just(createCallResult(
          dependentCounter,
          createOutParameters(
            fromJdbc(preparedStatement::getConnection),
            dependentCounter,
            new JdbcOutParameters(), metadata, adapter),
          adapter)));
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

    /** Number of batched bind values */
    private final int batchSize;

    private JdbcBatch(
      PreparedStatement preparedStatement, Queue<Object[]> batch) {
      super(preparedStatement, null);
      this.batch = batch;
      this.batchSize = batch.size();
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
      Publisher<?>[] bindPublishers = new Publisher<?>[batchSize];
      for (int i = 0; i < batchSize; i++) {
        bindPublishers[i] = Flux.concat(
          bind(batch.remove()),
          adapter.getLock().run(preparedStatement::addBatch));
      }
      return Flux.concat(bindPublishers).cast(Void.class);
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
    protected Publisher<OracleResultImpl> executeJdbc() {
      AtomicInteger index = new AtomicInteger(0);

      return Flux.from(adapter.publishBatchUpdate(preparedStatement))
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
    protected Publisher<OracleResultImpl> executeJdbc() {
      return Flux.from(super.executeJdbc())
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
     * Overrides the base implementation to include
     * {@link PreparedStatement#getGeneratedKeys()} with the first result, if
     * the generated keys {@code ResultSet} is not empty.
     * </p><p>
     * Oracle JDBC throws a {@code SQLException} when invoking
     * {@code getMetadata()} on an empty generated keys {@code ResultSet}, so
     * Oracle R2DBC should not even attempt to map that into a {@code Result} of
     * {@code Row} segments.
     * </p><p>
     * If the generated keys {@code ResultSet} is empty, then this method
     * behaves as if {@link Statement#returnGeneratedValues(String...)} had
     * never been called at all; It will return whatever results are available
     * from executing the statement, even if there are no generated values to
     * return.
     * </p><p>
     * The generated keys {@code ResultSet} will be empty if the
     * SQL was not an UPDATE or INSERT, because Oracle Database does not
     * support returning generated values for any other type of statement.
     * </p>
     */
    @Override
    protected Publisher<OracleResultImpl> executeJdbc() {
      return Mono.from(adapter.publishSQLExecution(preparedStatement))
        .flatMapMany(isResultSet -> {
          if (isResultSet) {
            return super.getResults(true);
          }
          else {
            return adapter.getLock().flatMap(() -> {
              ResultSet generatedKeys = preparedStatement.getGeneratedKeys();

              if (generatedKeys.isBeforeFirst()) {
                return Mono.just(createGeneratedValuesResult(
                  preparedStatement.getLargeUpdateCount(),
                    dependentCounter, generatedKeys, adapter))
                  .concatWith(super.getResults(
                    preparedStatement.getMoreResults(KEEP_CURRENT_RESULT)));
              }
              else {
                return super.getResults(false);
              }
            });
          }
        });
    }

  }
}
