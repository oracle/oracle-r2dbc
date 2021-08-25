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
 *       With JDBC: {@link #executeSqlJdbc(java.sql.Connection)}
 *     </ul>
 *     <ul>
 *       With R2DBC: {@link #executeSqlR2dbc(io.r2dbc.spi.Connection)}
 *     </ul>
 *   </li>
 *   <li>
 *     Handling Errors
 *     <ul>
 *       With JDBC: {@link #handleErrorJdbc(java.sql.Connection)}
 *     </ul>
 *     <ul>
 *       With R2DBC: {@link #handleErrorR2dbc(io.r2dbc.spi.Connection)}
 *     </ul>
 *   </li>
 *   <li>
 *     Conditional Execution
 *     <ul>
 *       With JDBC: {@link #executeConditionallyJdbc(java.sql.Connection)}
 *     </ul>
 *     <ul>
 *       With R2DBC: {@link #executeConditionallyR2dbc(io.r2dbc.spi.Connection)}
 *     </ul>
 *   </li>
 *   <li>
 *     Looping Execution
 *     <ul>
 *       With JDBC: {@link #executeLoopJdbc(java.sql.Connection)}
 *     </ul>
 *     <ul>
 *       With R2DBC: {@link #executeLoopR2dbc(io.r2dbc.spi.Connection)}
 *     </ul>
 *   </li>
 * </ol>
 */
public final class JdbcToR2dbc {

  /**
   * Returns a JDBC {@link DataSource} that is configured to connect with the
   * same database as the R2DBC {@link ConnectionFactory} returned by
   * {@link #configureR2dbc()}.
   * @return A JDBC {@code DataSource} configured to connect with a database
   */
  static DataSource configureJdbc() throws SQLException {
    // Construct a DataSource using a vendor specific class name
    // Create and configure a JDBC DataSource using vendor specific APIs of
    // the Oracle JDBC Driver
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
   * Returns an R2DBC {@link ConnectionFactory} that is configured to connect
   * with the same database as the JDBC {@link DataSource} returned by
   * {@link #configureJdbc()}.
   * @return An R2DBC {@code ConnectionFactory} configured to connect with a
   * database
   */
  static ConnectionFactory configureR2dbc() {
    // Create and configure ConnectionFactory using the ConnectionFactories and
    // ConnectionFactoryOptions class of the R2DBC API
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
   * Uses JDBC to execute a SQL query and map the result to a {@link String}.
   * This method performs the same database interactions as the
   * {@link #handleErrorR2dbc(io.r2dbc.spi.Connection)} method.
   * @return Value returned by a SQL query
   */
  static String executeSqlJdbc(java.sql.Connection connection)
    throws SQLException {

    try (java.sql.Statement statement  = connection.createStatement()) {
      ResultSet resultSet =
        statement.executeQuery("SELECT 'Hello, JDBC!' FROM sys.dual");

      if (resultSet.next())
        return resultSet.getString(1);
      else
        throw new RuntimeException("Query returned zero rows");
    }
  }

  /**
   * Uses R2DBC to execute a SQL query and map the result to a {@link String}.
   * This method performs the same database interactions as the
   * {@link #handleErrorJdbc(java.sql.Connection)} method.
   * @return Publisher of value returned by a SQL query
   */
  static Publisher<String> executeSqlR2dbc(io.r2dbc.spi.Connection connection) {
    return Flux.from(connection.createStatement(
      "SELECT 'Hello, R2DBC!' FROM sys.dual")
      .execute())
      .flatMap(result ->
        result.map(row -> row.get(0, String.class)))
      .switchIfEmpty(Flux.error(
        new RuntimeException("Query returned zero rows")));
  }

  /**
   * Uses JDBC to handle a failed INSERT by executing an UPDATE instead. This
   * method performs the same database interactions as the
   * {@link #handleErrorR2dbc(io.r2dbc.spi.Connection)} method.
   * @param connection Database connection
   * @return Number of rows updated
   * @throws SQLException If the database interaction fails with an
   * unexpected error.
   */
  static int handleErrorJdbc(java.sql.Connection connection)
    throws SQLException {

    try (PreparedStatement preparedStatement = connection.prepareStatement(
      "INSERT INTO JdbcToR2dbcTable(id, value) VALUES (?, ?)")) {
      preparedStatement.setInt(1, 0);
      preparedStatement.setString(2, "JDBC");
      return preparedStatement.executeUpdate();
    }
    catch (SQLException sqlException) {
      // Check if the error code is ORA-00001 for a unique constraint violation.
      // If so, then update the existing row.
      // Note: This doesn't handle cases where the row can be deleted.
      if (sqlException.getErrorCode() == 1) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(
          "UPDATE JdbcToR2dbcTable SET value = ? WHERE id = ?")) {
          preparedStatement.setString(1, "JDBC");
          preparedStatement.setInt(2, 0);
          return preparedStatement.executeUpdate();
        }
      }
      else {
        throw sqlException;
      }
    }
  }

  /**
   * Uses R2DBC to handle a failed INSERT by executing an UPDATE instead. This
   * method performs the same database interactions as the
   * {@link #handleErrorJdbc(java.sql.Connection)} method.
   * @param connection Database connection
   * @return A {@code Publisher} that emits {@code onNext} with the number of
   * rows updated, or emits {@code onError} with an {@link R2dbcException} if
   * the database interaction fails with an unexpected error.
   */
  static Publisher<Integer> handleErrorR2dbc(
    io.r2dbc.spi.Connection connection) {

    return Flux.from(connection.createStatement(
      "INSERT INTO JdbcToR2dbcTable(id, value) VALUES (?, ?)")
      .bind(0, 0)
      .bind(1, "R2DBC")
      .execute())
      .flatMap(result -> result.getRowsUpdated())
      .onErrorResume(R2dbcException.class, r2dbcException -> {
        // Check if the error code is ORA-00001 for a unique constraint
        // violation. If so, then update the existing row.
        // Note: This doesn't handle cases where the row can be deleted.
        if (r2dbcException.getErrorCode() == 1) {
          return Flux.from(connection.createStatement(
            "UPDATE JdbcToR2dbcTable SET value = ? WHERE id = ?")
            .bind(0, "R2DBC")
            .bind(1, 0)
            .execute())
            .flatMap(result -> result.getRowsUpdated());
        }
        else {
          return Flux.error(r2dbcException);
        }
      });
  }

  /**
   * Uses JDBC to conditionally execute an INSERT if an UPDATE returns a row
   * count of zero.
   * @param connection Database connection
   * @return Number of rows updated
   * @throws SQLException If the database interaction fails with an
   * unexpected error.
   */
  static int executeConditionallyJdbc(java.sql.Connection connection)
    throws SQLException {
    try (PreparedStatement updateStatement = connection.prepareStatement(
      "UPDATE JdbcToR2dbcTable SET value = ? WHERE id = ?")) {
      updateStatement.setString(1, "JDBC");
      updateStatement.setInt(2, 1);

      int updateCount = updateStatement.executeUpdate();
      if (updateCount != 0) {
        return updateCount;
      }
      else {
        try (PreparedStatement insertStatement = connection.prepareStatement(
          "INSERT INTO JdbcToR2dbcTable(id, value) VALUES (?, ?)")) {
          insertStatement.setInt(1, 1);
          insertStatement.setString(2, "JDBC");
          return insertStatement.executeUpdate();
        }
      }
    }
  }

  /**
   * Uses R2DBC to conditionally execute an INSERT if an UPDATE returns a row
   * count of zero. This  method performs the same database interactions as the
   * {@link #executeConditionallyJdbc(Connection)} method.
   * @param connection Database connection
   * @return A {@code Publisher} that emits {@code onNext} with the number of
   * rows updated, or emits {@code onError} with an {@link R2dbcException} if
   * the database interaction fails with an unexpected error.
   */
  static Publisher<Integer> executeConditionallyR2dbc(
    io.r2dbc.spi.Connection connection) {

    return Flux.from(connection.createStatement(
      "UPDATE JdbcToR2dbcTable SET value = ? WHERE id = ?")
      .bind(0, "R2DBC")
      .bind(1, 1)
      .execute())
      .flatMap(result -> result.getRowsUpdated())
      .flatMap(updateCount -> {
        // Check if a row was updated. If not, then this means the row does
        // not exist, and so it will be inserted.
        // Note: This doesn't handle cases where the row can be inserted
        // concurrently by another database session.
        if (updateCount != 0) {
          return Flux.just(updateCount);
        }
        else {
          return Flux.from(connection.createStatement(
            "INSERT INTO JdbcToR2dbcTable(id, value) VALUES (?, ?)")
            .bind(0, 1)
            .bind(1, "R2DBC")
            .execute())
            .flatMap(result -> result.getRowsUpdated());
        }
      });
  }

  /**
   * Uses JDBC to execute a loop where a failed INSERT is handled by
   * executing an UPDATE instead. The loop exits when the INSERT succeeds or
   * the UPDATE successfully updates a row. This method performs the same
   * database interactions as the
   * {@link #executeLoopR2dbc(io.r2dbc.spi.Connection)} method.
   * @param connection Database connection
   * @return Number of rows updated
   * @throws SQLException If the database interaction fails with an
   * unexpected error.
   */
  static int executeLoopJdbc(java.sql.Connection connection)
    throws SQLException {

    while (true) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(
        "INSERT INTO JdbcToR2dbcTable(id, value) VALUES (?, ?)")) {
        preparedStatement.setInt(1, 0);
        preparedStatement.setString(2, "JDBC");
        return preparedStatement.executeUpdate();
      }
      catch (SQLException sqlException) {
        // Check if the error code is ORA-00001 for a unique constraint violation.
        // If so, then update the existing row.
        // Note: This doesn't handle cases where the row can be deleted.
        if (sqlException.getErrorCode() == 1) {
          try (PreparedStatement preparedStatement = connection.prepareStatement(
            "UPDATE JdbcToR2dbcTable SET value = ? WHERE id = ?")) {
            preparedStatement.setString(1, "JDBC");
            preparedStatement.setInt(2, 0);
            int updateCount = preparedStatement.executeUpdate();

            if (updateCount != 0)
              return updateCount;
          }
        }
        else {
          throw sqlException;
        }
      }
    }
  }

  /**
   * <p>
   * Uses R2DBC to execute a loop where a failed INSERT is handled by
   * executing an UPDATE instead. The loop exits when the INSERT succeeds or
   * the UPDATE successfully updates a row. This method performs the same
   * database interactions as the
   * {@link #executeLoopJdbc(java.sql.Connection)} method.
   * </p><p>
   * Synchronous programming would typically implement a loop using
   * {@code while} or {@code for} statements. This synchronous model works
   * well for {@linkplain #executeLoopJdbc(java.sql.Connection) JDBC calls}
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
   * @return A {@code Publisher} that emits {@code onNext} with the number of
   * rows updated, or emits {@code onError} with an {@link R2dbcException} if
   * the database interaction fails with an unexpected error.
   */
  static Publisher<Integer> executeLoopR2dbc(
    io.r2dbc.spi.Connection connection) {

    return Flux.from(connection.createStatement(
      "INSERT INTO JdbcToR2dbcTable(id, value) VALUES (?, ?)")
      .bind(0, 0)
      .bind(1, "R2DBC")
      .execute())
      .flatMap(result -> result.getRowsUpdated())
      .onErrorResume(R2dbcException.class, r2dbcException -> {
        // Check if the error code is ORA-00001 for a unique constraint
        // violation. If so, then update the existing row.
        // Note: This doesn't handle cases where the row can be deleted.
        if (r2dbcException.getErrorCode() == 1) {
          return Flux.from(connection.createStatement(
            "UPDATE JdbcToR2dbcTable SET value = ? WHERE id = ?")
            .bind(0, "R2DBC")
            .bind(1, 0)
            .execute())
            .flatMap(result -> result.getRowsUpdated())
            .flatMap(updateCount -> {
              // Check if a row was updated. If not, then this means the row does
              // not exist, and so it will be inserted. The insert is
              // executed by recursively invoking this method.
              if (updateCount != 0)
                return Flux.just(updateCount);
              else
                return executeLoopR2dbc(connection);
            });
        }
        else {
          return Flux.error(r2dbcException);
        }
      });
  }

  /**
   * Executes all examples with both JDBC and R2DBC.
   */
  public static void main(String[] args) throws SQLException {
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
        executeSqlJdbc(connection));
      System.out.println(
        handleErrorJdbc(connection) + " row(s) updated.");
      System.out.println(
        executeConditionallyJdbc(connection) + " row(s) updated.");
      System.out.println(
        executeLoopJdbc(connection) + " row(s) updated.");
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
        Flux.from(executeSqlR2dbc(connection))
          .doOnNext(System.out::println)
          .thenMany(handleErrorR2dbc(connection))
          .doOnNext(updateCount ->
            System.out.println(updateCount + " row(s) updated."))
          .thenMany(executeConditionallyR2dbc(connection))
          .doOnNext(updateCount ->
            System.out.println(updateCount + " row(s) updated."))
          .thenMany(executeLoopR2dbc(connection))
          .doOnNext(updateCount ->
            System.out.println(updateCount + " row(s) updated.")),
      connection -> connection.close())
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
      connection -> connection.close())
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
      connection -> connection.close())
      // Print any error and then discard it. This method will be called from
      // a finally block so we don't want it to throw the error over another
      // error from the try block.
      .doOnError(System.out::println)
      .onErrorReturn(1)
      .blockLast(Duration.ofSeconds(15));
  }

}
