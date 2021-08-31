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

package oracle.r2dbc.samples;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;
import oracle.jdbc.pool.OracleDataSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.NoSuchElementException;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

/**
 * <p>
 * This class presents a series of code examples demonstrating usages
 * of both JDBC and R2DBC APIs that produce identical database interactions.
 * The target audience for these examples are programmers seeking to learn
 * learn how R2DBC can accomplish the same work as JDBC.
 * </p><p>
 * Examples in this class read the database connection configuration from a
 * file named "config.properties" that exists in the current directory when
 * this demo is run. There is an example config.properties file in the
 * /sample directory (relative to the root this repository).
 * </p><b>
 * List of Examples
 * </b><ol>
 *   <li>
 *     Configuring Database Connections
 *     <ul>
 *       With JDBC: {@link #configureJdbc()}
 *     </ul>
 *     <ul>
 *       With R2DBC: {@link #configureR2dbc()}
 *     </ul>
 *   </li>
 *   <li>
 *     Executing a SQL Query
 *     <ul>
 *       With JDBC: {@link #queryJdbc(java.sql.Connection)}
 *     </ul>
 *     <ul>
 *       With R2DBC: {@link #queryR2dbc(io.r2dbc.spi.Connection)}
 *     </ul>
 *   </li>
 *   <li>
 *     Executing an UPDATE
 *     <ul>
 *       With JDBC: {@link #updateJdbc(java.sql.Connection)}
 *     </ul>
 *     <ul>
 *       With R2DBC: {@link #updateR2dbc(io.r2dbc.spi.Connection)}
 *     </ul>
 *   </li>
 *   <li>
 *     Executing an INSERT
 *     <ul>
 *       With JDBC: {@link #insertJdbc(java.sql.Connection)}
 *     </ul>
 *     <ul>
 *       With R2DBC: {@link #insertR2dbc(io.r2dbc.spi.Connection)}
 *     </ul>
 *   </li>
 *   <li>
 *     Attempt to INSERT a row which may already exist. If the row already
 *     exists, then UPDATE it.
 *     <ul>
 *       With JDBC: {@link #tryInsertJdbc(java.sql.Connection)}
 *     </ul>
 *     <ul>
 *       With R2DBC: {@link #tryInsertR2dbc(io.r2dbc.spi.Connection)}
 *     </ul>
 *   </li>
 *   <li>
 *     Attempt to UPDATE a row which may not exist. If the row doesn't exist,
 *     then INSERT it.
 *     <ul>
 *       With JDBC: {@link #tryUpdateJdbc(java.sql.Connection)}
 *     </ul>
 *     <ul>
 *       With R2DBC: {@link #tryUpdateR2dbc(io.r2dbc.spi.Connection)}
 *     </ul>
 *   </li>
 *   <li>
 *     Attempt to UPDATE a row which may not exist. If the row doesn't exist,
 *     then attempt to INSERT the row, which now may already exist. If the row
 *     already exists, then repeatedly attempt the UPDATE and INSERT until
 *     one succeeds.
 *     <ul>
 *       With JDBC: {@link #loopJdbc(java.sql.Connection)}
 *     </ul>
 *     <ul>
 *       With R2DBC: {@link #loopR2dbc(io.r2dbc.spi.Connection)}
 *     </ul>
 *   </li>
 * </ol>
 */
public final class JdbcToR2dbc {

  /** Error code of ORA-00001 Unique Constraint ... Violated */
  public static final int UNIQUE_CONSTRAINT_VIOLATION = 1;

  /**
   * Configures JDBC to connect with a database. Configuration is read from a
   * "config.properties" file in the current directory. This method applies the
   * same configuration as {@link #configureR2dbc()}.
   * @return {@code DataSource} configured to connect with a database
   */
  static DataSource configureJdbc() throws SQLException {

    OracleDataSource dataSource = new oracle.jdbc.pool.OracleDataSource();
    dataSource.setDriverType("thin");
    dataSource.setServerName(DatabaseConfig.HOST);
    dataSource.setPortNumber(DatabaseConfig.PORT);
    dataSource.setServiceName(DatabaseConfig.SERVICE_NAME);
    dataSource.setUser(DatabaseConfig.USER);
    dataSource.setPassword(DatabaseConfig.PASSWORD);
    return dataSource;

  }

  /**
   * Configures JDBC to connect with a database. Configuration is read from a
   * "config.properties" file in the current directory. This method applies the
   * same configuration as {@link #configureJdbc()}.
   * @return {@code ConnectionFactory} configured to connect with a database
   */
  static ConnectionFactory configureR2dbc() {

    return ConnectionFactories.get(ConnectionFactoryOptions.builder()
      .option(DRIVER, "oracle")
      .option(HOST, DatabaseConfig.HOST)
      .option(PORT, DatabaseConfig.PORT)
      .option(DATABASE, DatabaseConfig.SERVICE_NAME)
      .option(USER, DatabaseConfig.USER)
      .option(PASSWORD, DatabaseConfig.PASSWORD)
      .build());

  }

  /**
   * Executes a SQL query with JDBC. This method performs the same database
   * interactions as {@link #queryR2dbc(io.r2dbc.spi.Connection)}.
   * @return Value returned by a SQL query
   */
  static String queryJdbc(java.sql.Connection connection) throws SQLException {

    try (java.sql.Statement statement  = connection.createStatement()) {
      ResultSet resultSet =
        statement.executeQuery("SELECT 'Hello, JDBC!' FROM sys.dual");

      if (resultSet.next())
        return resultSet.getString(1);
      else
        throw new NoSuchElementException("Query returned zero rows");
    }

  }

  /**
   * Executes a SQL query with R2DBC. This method performs the same database
   * interactions as {@link #queryJdbc(java.sql.Connection)}.
   * @return Value returned by a SQL query
   */
  static Publisher<String> queryR2dbc(io.r2dbc.spi.Connection connection) {

    return Flux.from(connection.createStatement(
      "SELECT 'Hello, R2DBC!' FROM sys.dual")
      .execute())
      .flatMap(result ->
        result.map(row -> row.get(0, String.class)))
      .switchIfEmpty(Flux.error(
        new NoSuchElementException("Query returned zero rows")));

  }

  /**
   * Executes an INSERT with JDBC. This method performs the same database
   * interactions as {@link #insertR2dbc(io.r2dbc.spi.Connection)}.
   * @return Count of inserted rows.
   */
  static int insertJdbc(java.sql.Connection connection) throws SQLException {

    try (PreparedStatement preparedStatement = connection.prepareStatement(
      "INSERT INTO JdbcToR2dbcTable(id, value) VALUES (?, ?)")) {
      preparedStatement.setInt(1, 0);
      preparedStatement.setString(2, "JDBC");
      return preparedStatement.executeUpdate();
    }

  }

  /**
   * Executes an INSERT with R2DBC. This method performs the same database
   * interactions as {@link #insertJdbc(java.sql.Connection)}.
   * @return {@code Publisher} emitting the count of inserted rows.
   */
  static Publisher<Integer> insertR2dbc(io.r2dbc.spi.Connection connection) {

    return Flux.from(connection.createStatement(
      "INSERT INTO JdbcToR2dbcTable(id, value) VALUES (?, ?)")
      .bind(0, 0)
      .bind(1, "R2DBC")
      .execute())
      .flatMap(Result::getRowsUpdated);

  }

  /**
   * Executes an UPDATE with JDBC. This method performs the same database
   * interactions as {@link #updateR2dbc(io.r2dbc.spi.Connection)}.
   * @return Count of updated rows.
   */
  static int updateJdbc(java.sql.Connection connection) throws SQLException {

    try (PreparedStatement preparedStatement = connection.prepareStatement(
      "UPDATE JdbcToR2dbcTable SET value = ? WHERE id = ?")) {
      preparedStatement.setString(1, "JDBC");
      preparedStatement.setInt(2, 0);
      return preparedStatement.executeUpdate();
    }

  }

  /**
   * Executes an UPDATE with R2DBC. This method performs the same database
   * interactions as {@link #updateJdbc(java.sql.Connection)}.
   * @return {@code Publisher} emitting the count of updated rows.
   */
  static Publisher<Integer> updateR2dbc(io.r2dbc.spi.Connection connection) {

    return Flux.from(connection.createStatement(
      "UPDATE JdbcToR2dbcTable SET value = ? WHERE id = ?")
      .bind(0, "R2DBC")
      .bind(1, 0)
      .execute())
      .flatMap(Result::getRowsUpdated);

  }

  /**
   * Tries to update a row with JDBC. If the updated row does not exist,
   * then this method tries to insert it. This method performs the same
   * database interactions as {@link #tryUpdateR2dbc(io.r2dbc.spi.Connection)}
   * @param connection Database connection
   * @return Count of updated rows
   */
  static int tryUpdateJdbc(java.sql.Connection connection) throws SQLException {

    // Try to update the row
    int updateCount = updateJdbc(connection);

    // If the row does not exist, then insert it.
    if (updateCount == 0)
      return insertJdbc(connection);
    else
      return updateCount;

  }

  /**
   * Tries to update a row with R2DBC. If the updated row does not exist,
   * then this method tries to insert it. This method performs the same
   * database interactions as {@link #tryUpdateJdbc(java.sql.Connection)}
   * @param connection Database connection
   * @return {@code Publisher} emitting the count of updated rows.
   */
  static Publisher<Integer> tryUpdateR2dbc(io.r2dbc.spi.Connection connection) {

    // Try to update the row
    return Flux.from(updateR2dbc(connection))
      .flatMap(updateCount -> {

        // If the row does not exist, then insert it.
        if (updateCount == 0)
          return insertR2dbc(connection);
        else
          return Flux.just(updateCount);

      });

  }

  /**
   * Tries to insert a row with JDBC. If the INSERT fails because the row
   * already exists, this method tries to update it. This method performs the
   * same database interactions as
   * {@link #tryInsertR2dbc(io.r2dbc.spi.Connection)}.
   * @param connection Database connection
   * @return Count of updated rows
   */
  static int tryInsertJdbc(java.sql.Connection connection) throws SQLException {

    try {
      // Try to insert the row
      return insertJdbc(connection);
    }
    catch (SQLException sqlException) {

      // If the row already exists, then update it.
      if (sqlException.getErrorCode() == UNIQUE_CONSTRAINT_VIOLATION)
        return updateJdbc(connection);
      else
        throw sqlException;

    }

  }

  /**
   * Tries to insert a row with R2DBC. If the INSERT fails because the row
   * already exists, this method tries to update it. This method performs the
   * same database interactions as {@link #tryInsertJdbc(java.sql.Connection)}.
   * @param connection Database connection
   * @return {@code Publisher} emitting the count of updated rows.
   */
  static Publisher<Integer> tryInsertR2dbc(io.r2dbc.spi.Connection connection) {

    // Try to insert the row
    return Flux.from(insertR2dbc(connection))
      .onErrorResume(R2dbcException.class, r2dbcException -> {

        // If the row already exists, then update it.
        if (r2dbcException.getErrorCode() == UNIQUE_CONSTRAINT_VIOLATION)
          return updateR2dbc(connection);
        else
          return Flux.error(r2dbcException);

      });

  }

  /**
   * Tries to insert a row with JDBC. If the
   * Uses JDBC to execute a loop where a failed INSERT is handled by
   * executing an UPDATE instead. The loop exits when the INSERT succeeds or
   * the UPDATE successfully updates a row. This method performs the same
   * database interactions as the
   * {@link #loopR2dbc(io.r2dbc.spi.Connection)} method.
   * @param connection Database connection
   * @return Count of updated rows
   * @throws SQLException If the database interaction fails with an
   * unexpected error.
   */
  static int loopJdbc(java.sql.Connection connection) throws SQLException {

    do {
      try {
        // Try to update the row, or insert it if it does not exist
        return tryUpdateJdbc(connection);
      }
      catch (SQLException sqlException) {

        // If another database session has inserted the row before this
        // one did, then recover from failure by continuing the loop.
        if (sqlException.getErrorCode() != UNIQUE_CONSTRAINT_VIOLATION)
          throw sqlException;

      }
    } while (true);

  }

  /**
   * <p>
   * Uses R2DBC to execute a loop where a failed INSERT is handled by
   * executing an UPDATE instead. The loop exits when the INSERT succeeds or
   * the UPDATE successfully updates a row. This method performs the same
   * database interactions as the
   * {@link #loopJdbc(java.sql.Connection)} method.
   * </p><p>
   * Synchronous programming would typically implement a loop using
   * {@code while} or {@code for} statements. This synchronous model works
   * well for {@linkplain #loopJdbc(java.sql.Connection) JDBC calls}
   * where an iteration of the loop blocks until the call completes, and the
   * result of call can then be evaluated to decide if the loop should repeat
   * for another iteration.
   * </p><p>
   * With asynchronous programming, {@code while} and {@code for} can not be
   * used to execute database calls in a loop. The exit condition of the loop
   * can not be evaluated until the result of the database call is known. So,
   * instead of using {@code while} or {@code for}, this method implements the
   * loop using recursion. If the INSERT results in failure, or if the UPDATE
   * results in a row count of zero, then the {@code Publisher} returned by
   * this method completes with the result of another {@code Publisher}
   * returned by a recursive invocation of this method.
   * </p>
   * @param connection Database connection
   * @return {@code Publisher} emitting the count of updated rows.
   */
  static Publisher<Integer> loopR2dbc(io.r2dbc.spi.Connection connection) {

    // Try to update the row, or insert it if it does not exist
    return Flux.from(tryUpdateR2dbc(connection))
      .onErrorResume(R2dbcException.class, r2dbcException -> {

        // If another database session has inserted the row before this
        // one did, then recover from failure by recursively invoking this
        // method.
        if (r2dbcException.getErrorCode() != UNIQUE_CONSTRAINT_VIOLATION)
          return Flux.error(r2dbcException);
        else
          return loopR2dbc(connection);

      });

  }

  /**
   * Executes all examples with both JDBC and R2DBC.
   */
  public static void main(String[] args) {
    createTable();
    try {
      useJdbc();
      useR2dbc();
    }
    finally {
      dropTable();
    }
  }

  /**
   * Executes all examples with JDBC. The {@link #createTable()} method
   * should be called prior to invoking this method.
   */
  static void useJdbc() {
    try (java.sql.Connection connection = configureJdbc().getConnection()) {
      System.out.println(
        queryJdbc(connection));
      System.out.println(
        tryInsertJdbc(connection) + " row(s) updated.");
      System.out.println(
        tryUpdateJdbc(connection) + " row(s) updated.");
      System.out.println(
        loopJdbc(connection) + " row(s) updated.");
    }
    catch (SQLException sqlException) {
      throw new RuntimeException(sqlException);
    }
  }

  /**
   * Executes all examples with R2DBC. The {@link #createTable()} method
   * should be called prior to invoking this method.
   */
  static void useR2dbc() {
    Flux.usingWhen(
      configureR2dbc().create(),
      connection ->
        Flux.from(queryR2dbc(connection))
          .doOnNext(System.out::println)
          .thenMany(tryInsertR2dbc(connection))
          .doOnNext(updateCount ->
            System.out.println(updateCount + " row(s) updated."))
          .thenMany(tryUpdateR2dbc(connection))
          .doOnNext(updateCount ->
            System.out.println(updateCount + " row(s) updated."))
          .thenMany(loopR2dbc(connection))
          .doOnNext(updateCount ->
            System.out.println(updateCount + " row(s) updated.")),
      io.r2dbc.spi.Connection::close)
      .blockLast(Duration.ofSeconds(30));
  }

  /**
   * Creates the database table used by the example methods
   */
  static void createTable() {
    Flux.usingWhen(configureR2dbc().create(),
      connection ->
        Flux.from(connection.createStatement(
          "CREATE TABLE JdbcToR2dbcTable (" +
            "id NUMBER PRIMARY KEY, value VARCHAR2(128))")
          .execute())
          .flatMap(Result::getRowsUpdated)
          .onErrorReturn(error ->
            // Check if the error is ORA-00955 for creating a table that already
            // exists. If so, then ignore it.
            error instanceof R2dbcException
              && ((R2dbcException)error).getErrorCode() == 955,
            0),
      io.r2dbc.spi.Connection::close)
      .blockLast(Duration.ofSeconds(15));
  }

  /**
   * Drops the database table used by the example methods
   */
  static void dropTable() {
    Flux.usingWhen(configureR2dbc().create(),
      connection ->
        Flux.from(connection.createStatement("DROP TABLE JdbcToR2dbcTable")
          .execute())
          .flatMap(Result::getRowsUpdated),
      io.r2dbc.spi.Connection::close)
      // Print any error and then discard it. This method will be called from
      // a finally block so we don't want it to throw the error over another
      // error from the try block.
      .doOnError(System.out::println)
      .onErrorReturn(1)
      .blockLast(Duration.ofSeconds(15));
  }

}
