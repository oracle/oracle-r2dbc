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

package oracle.r2dbc.test;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import oracle.jdbc.OracleConnection;
import oracle.r2dbc.util.SharedConnectionFactory;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Stores configuration used by integration tests that connect to a database.
 * The configuration is read from a resource file named "config.properties"
 * which is located by {@link ClassLoader#getResourceAsStream(String)}.
 */
public final class DatabaseConfig {

  private DatabaseConfig() {}

  /**
   * Returns the hostname of the server where a test database listens for
   * connections, specified as {@code HOST} in the "config.properties" file.
   * @return Hostname of a test database.
   */
  public static String host() {
    return HOST;
  }

  /**
   * Returns the port number where a test database listens for connections,
   * specified as {@code PORT} in the "config.properties" file.
   * @return Port number of a test database.
   */
  public static int port() {
    return PORT;
  }

  /**
   * Returns the service name that a test database identifies itself as,
   * specified as {@code DATABASE} in the "config.properties" file.
   * @return Service name of a test database.
   */
  public static String serviceName() {
    return SERVICE_NAME;
  }

  /**
   * Returns the user name that a test database authenticates, specified as
   * {@code USER} in the "config.properties" file.
   * @return User name of a test database.
   */
  public static String user() {
    return USER;
  }

  /**
   * Returns the password that a test database authenticates, specified as
   * {@code PASSWORD} in the "config.properties" file.
   * @return Password of a test database.
   */
  public static String password() {
    return PASSWORD;
  }

  /**
   * Returns the maximum duration that a test should wait for a database
   * connection to be created, specified as a number of seconds with
   * {@code connectTimeout} in the "config.properties" file
   * @return Connection timeout of a test database.
   */
  public static Duration connectTimeout() {
    return CONNECT_TIMEOUT;
  }

  /**
   * Returns the maximum duration that a test should wait for a SQL command
   * to execute, specified as a number of seconds with {@code sqlTimeout} in
   * "config.properties" file.
   * @return Connection timeout of a test database.
   */
  public static Duration sqlTimeout() {
    return SQL_TIMEOUT;
  }

  /**
   * <p>
   * Returns a publisher that emits a newly created {@code Connection}. The
   * connection is created according to the the values specified in the
   * "config.properties" file.
   * </p><p>
   * To reduce test latency, {@link #sharedConnection()} should be used in
   * favor of this method whenever it is possible to do so. This method
   * should only be used when a test requires more than one connection, or if
   * a test must verify the {@link Connection#close()} method.
   * </p>
   * @return A publisher of a newly created connection to a test database.
   */
  public static Publisher<? extends Connection> newConnection() {
    return CONNECTION_FACTORY.create();
  }

  /**
   * <p>
   * Returns a publisher that emits a shared {@code Connection}. The
   * shared connection is created according to the the values specified in the
   * "config.properties" file.
   * </p><p>
   * Tests which use the shared connection method can eliminate the latency of
   * creating a new connection.
   * </p><p>
   * Each call to this method returns a publisher that emits the <em>same
   * connection</em>. This method is not suitable for tests that require
   * multiple connections, or tests that verify the
   * {@link Connection#close()} method.
   * </p>
   *
   * @implNote The shared connection is closed abruptly when the JVM is
   * shutdown and closes the shared connection's network socket. An Oracle
   * Database will detect the closed socket and deallocate resources
   * that were allocated for the connection's session.
   * TODO: Close the shared connection by calling {@link Connection#close()}.
   *   The call to close() should happen after JUnit has run the last test in
   *   a suite of tests.
   *
   * @return A publisher of a shared connection to a test database.
   */
  public static Publisher<? extends Connection> sharedConnection() {
    return SHARED_CONNECTION_FACTORY.create();
  }

  /**
   * Returns the major version number of the database specified in the
   * "config.properties" file. Tests can call this method to determine if the
   * database is expected to support a particular feature, such as the JSON
   * column type that was introduced in version 21.1 of Oracle Database.
   * @return The major version number of the test database.
   */
  public static int databaseVersion() {
    try (var jdbcConnection = DriverManager.getConnection(String.format(
      "jdbc:oracle:thin:@%s:%s/%s", host(), port(), serviceName()),
      user(), password())) {
      return jdbcConnection.getMetaData().getDatabaseMajorVersion();
    }
    catch (SQLException sqlException) {
      throw new AssertionError(sqlException);
    }
  }

  /**
   * Queries the {@code user_errors} data dictionary view and prints all rows.
   * When writing new tests that declare a PL/SQL procedure or function,
   * "ORA-17110: executed completed with a warning" results if the PL/SQL has
   * a syntax error. The error details will be printed by calling this method.
   */
  public static void showErrors(Connection connection) {
      Flux.from(connection.createStatement(
        "SELECT * FROM user_errors ORDER BY sequence")
        .execute())
        .flatMap(result ->
          result.map((row, metadata) ->
            metadata.getColumnNames()
              .stream()
              .map(name -> name + ": " + row.get(name))
              .collect(Collectors.joining("\n"))))
      .toStream()
      .map(errorText -> "\n" + errorText)
      .forEach(System.err::println);
  }

  private static final String HOST;
  private static final int PORT;
  private static final String SERVICE_NAME;
  private static final String USER;
  private static final String PASSWORD;
  private static final Duration CONNECT_TIMEOUT;
  private static final Duration SQL_TIMEOUT;
  private static final ConnectionFactory CONNECTION_FACTORY;
  private static final ConnectionFactory SHARED_CONNECTION_FACTORY;

  private static final String CONFIG_FILE_NAME = "config.properties";
  static {
    try (InputStream inputStream =
           DatabaseConfig.class.getClassLoader()
             .getResourceAsStream(CONFIG_FILE_NAME)) {

      if (inputStream == null) {
        throw new FileNotFoundException(
          CONFIG_FILE_NAME + " resource not found. " +
          "Check if it exists under src/test/resources/");
      }

      Properties prop = new Properties();
      prop.load(inputStream);

      HOST = prop.getProperty("HOST");
      PORT = Integer.parseInt(prop.getProperty("PORT"));
      SERVICE_NAME = prop.getProperty("DATABASE");
      USER = prop.getProperty("USER");
      PASSWORD = prop.getProperty("PASSWORD");
      CONNECT_TIMEOUT = Duration.ofSeconds(
        Long.parseLong(prop.getProperty("CONNECT_TIMEOUT")));
      SQL_TIMEOUT = Duration.ofSeconds(
        Long.parseLong(prop.getProperty("SQL_TIMEOUT")));

      CONNECTION_FACTORY = ConnectionFactories.get(
        ConnectionFactoryOptions.builder()
          .option(ConnectionFactoryOptions.DRIVER, "oracle")
          .option(ConnectionFactoryOptions.HOST, HOST)
          .option(ConnectionFactoryOptions.PORT, PORT)
          .option(ConnectionFactoryOptions.DATABASE, SERVICE_NAME)
          .option(ConnectionFactoryOptions.USER, USER)
          .option(ConnectionFactoryOptions.PASSWORD, PASSWORD)
          // Disable statement caching in order to verify cursor closing;
          // Cached statements don't close their cursors
          .option(Option.valueOf(
            OracleConnection.CONNECTION_PROPERTY_IMPLICIT_STATEMENT_CACHE_SIZE),
            0)
          .build());

      SHARED_CONNECTION_FACTORY = new SharedConnectionFactory(
        CONNECTION_FACTORY.create(),
        CONNECTION_FACTORY.getMetadata());
    }
    catch (Throwable initializationFailure) {
      // Most test cases require a database connection; If it can't be
      // configured, then the test run can not proceed. Print the failure and
      // terminate the JVM:
      initializationFailure.printStackTrace();
      System.exit(-1);

      // This throw is dead code; exit(-1) doesn't return. This throw helps
      // javac to understand that the final fields are always initialized when
      // this static initialization block returns successfully.
      throw new RuntimeException(initializationFailure);
    }
  }
}
