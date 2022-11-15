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

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.Result;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.datasource.OracleDataSource;
import oracle.r2dbc.OracleR2dbcOptions;
import oracle.r2dbc.test.DatabaseConfig;
import oracle.r2dbc.util.TestContextFactory;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.r2dbc.spi.ConnectionFactoryOptions.CONNECT_TIMEOUT;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.STATEMENT_TIMEOUT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static java.lang.String.format;
import static oracle.r2dbc.test.DatabaseConfig.connectTimeout;
import static oracle.r2dbc.test.DatabaseConfig.connectionFactoryOptions;
import static oracle.r2dbc.test.DatabaseConfig.host;
import static oracle.r2dbc.test.DatabaseConfig.password;
import static oracle.r2dbc.test.DatabaseConfig.port;
import static oracle.r2dbc.test.DatabaseConfig.protocol;
import static oracle.r2dbc.test.DatabaseConfig.serviceName;
import static oracle.r2dbc.test.DatabaseConfig.sharedConnection;
import static oracle.r2dbc.test.DatabaseConfig.sqlTimeout;
import static oracle.r2dbc.test.DatabaseConfig.user;
import static oracle.r2dbc.util.Awaits.awaitError;
import static oracle.r2dbc.util.Awaits.awaitExecution;
import static oracle.r2dbc.util.Awaits.awaitNone;
import static oracle.r2dbc.util.Awaits.awaitOne;
import static oracle.r2dbc.util.Awaits.awaitUpdate;
import static oracle.r2dbc.util.Awaits.tryAwaitExecution;
import static oracle.r2dbc.util.Awaits.tryAwaitNone;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies that
 * {@link OracleReadableMetadataImpl} implements behavior that is specified in
 * it's class and method level javadocs.
 */
public class OracleReactiveJdbcAdapterTest {

  /**
   * Verifies the implementation of
   * {@link OracleReactiveJdbcAdapter#createDataSource(ConnectionFactoryOptions)}
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testCreateDataSource() throws SQLException {

    // Verify extended options that configure Oracle JDBC Driver connection
    // properties. The defaultProperties variable contains properties that
    // are set to default values by OracleReactiveJdbcAdapter and the Oracle
    // JDBC Driver
    Properties defaultProperties = new Properties();
    defaultProperties.setProperty(
      OracleConnection.CONNECTION_PROPERTY_J2EE13_COMPLIANT, "true");
    defaultProperties.setProperty(
      OracleConnection.CONNECTION_PROPERTY_ENABLE_AC_SUPPORT, "false");
    defaultProperties.setProperty(
      OracleConnection.CONNECTION_PROPERTY_IMPLICIT_STATEMENT_CACHE_SIZE, "25");
    defaultProperties.setProperty(
      OracleConnection.CONNECTION_PROPERTY_DEFAULT_LOB_PREFETCH_SIZE,
      "1048576");

    // Expect only default connection properties when no extended
    // options are supplied
    assertEquals(defaultProperties,
      OracleReactiveJdbcAdapter.getInstance()
        .createDataSource(ConnectionFactoryOptions.builder()
          .option(HOST, host())
          .option(PORT, port())
          .option(DATABASE, serviceName())
          .build())
        .unwrap(OracleDataSource.class)
        .getConnectionProperties());

    // Expect only default connection properties when no supported options are
    // supplied
    assertEquals(defaultProperties,
      OracleReactiveJdbcAdapter.getInstance()
        .createDataSource(ConnectionFactoryOptions.builder()
          .option(HOST, host())
          .option(PORT, port())
          .option(DATABASE, serviceName())
          .option(Option.valueOf(
            OracleConnection.CONNECTION_PROPERTY_RETAIN_V9_BIND_BEHAVIOR),
            "true")
          .option(Option.valueOf("This is not supported"), "true")
          .build())
        .unwrap(OracleDataSource.class)
        .getConnectionProperties());

    // Expect additional connection properties when supported options are
    // supplied
    Properties expected = (Properties)defaultProperties.clone();
    expected.setProperty(
      OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORETYPE,
      "PKCS12");
    expected.setProperty(
      OracleConnection.CONNECTION_PROPERTY_THIN_SSL_VERSION,
      "TLSv1.3");
    expected.setProperty(
      OracleConnection.CONNECTION_PROPERTY_TNS_ADMIN,
      "/home/oracle/admin");

    assertEquals(expected,
      OracleReactiveJdbcAdapter.getInstance()
        .createDataSource(ConnectionFactoryOptions.builder()
          .option(HOST, host())
          .option(PORT, port())
          .option(DATABASE, serviceName())
          .option(Option.valueOf(
            OracleConnection
              .CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORETYPE),
            "PKCS12")
          .option(Option.valueOf(
            OracleConnection.CONNECTION_PROPERTY_THIN_SSL_VERSION),
            "TLSv1.3")
          .option(Option.valueOf(
            OracleConnection.CONNECTION_PROPERTY_TNS_ADMIN),
            "/home/oracle/admin")
          .build())
        .unwrap(OracleDataSource.class)
        .getConnectionProperties());

    // Expect the TNS_ADMIN property to be set when an option specifies
    // it's short form name
    assertEquals(expected,
      OracleReactiveJdbcAdapter.getInstance()
        .createDataSource(ConnectionFactoryOptions.builder()
          .option(HOST, host())
          .option(PORT, port())
          .option(DATABASE, serviceName())
          .option(Option.valueOf(
            OracleConnection
              .CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORETYPE),
            "PKCS12")
          .option(Option.valueOf(
            OracleConnection.CONNECTION_PROPERTY_THIN_SSL_VERSION),
            "TLSv1.3")
          .option(Option.valueOf("TNS_ADMIN"), "/home/oracle/admin")
          .build())
        .unwrap(OracleDataSource.class)
        .getConnectionProperties());
  }

  /**
   * Verifies that
   * {@link OracleReactiveJdbcAdapter#createDataSource(ConnectionFactoryOptions)}
   * handles Oracle Net Descriptors and the
   * {@link OracleConnection#CONNECTION_PROPERTY_TNS_ADMIN} property
   * correctly. The TNS ADMIN property configures a file system path to a
   * directory containing tnsnames.ora and ojdbc.properties files. If the TNS
   * admin property is handled correctly, then descriptors should be read
   * from tnsnames.ora, and connection properties should be read from
   * ojdbc.properties.
   */
  @Test
  public void testTnsAdmin() throws IOException {

    // Create an Oracle Net Descriptor
    String descriptor = createDescriptor();

    // Create a tnsnames.ora file with an alias for the descriptor
    Files.writeString(Path.of("tnsnames.ora"),
      "test_alias="+descriptor, StandardOpenOption.CREATE_NEW);

    // Get the current working directory, with percent encodings to replace
    // any characters that are invalid in a URL
    String userDir = System.getProperty("user.dir")
      .replace(" ", "%20");

    try {
      // Expect to connect with the descriptor in the R2DBC URL
      awaitNone(awaitOne(
        ConnectionFactories.get(format(
          "r2dbc:oracle://%s:%s@?oracle.r2dbc.descriptor=%s",
          user(), password(), descriptor))
          .create())
        .close());
      awaitNone(awaitOne(
        ConnectionFactories.get(ConnectionFactoryOptions.parse(format(
          "r2dbc:oracle://@?oracle.r2dbc.descriptor=%s", descriptor))
          .mutate()
          .option(USER, user())
          .option(PASSWORD, password())
          .build())
          .create())
        .close());

      // Expect to connect with the tnsnames.ora file, when a URL specifies
      // the file path and an alias
      awaitNone(awaitOne(
        ConnectionFactories.get(format(
          "r2dbc:oracle://%s:%s@?oracle.r2dbc.descriptor=%s&TNS_ADMIN=%s",
          user(), password(), "test_alias", userDir))
          .create())
          .close());
      awaitNone(awaitOne(
        ConnectionFactories.get(ConnectionFactoryOptions.parse(
          format(
            "r2dbc:oracle://@?oracle.r2dbc.descriptor=%s&TNS_ADMIN=%s",
            "test_alias", userDir))
          .mutate()
          .option(USER, user())
          .option(PASSWORD, password())
          .build())
          .create())
        .close());

      // Expect IllegalArgumentException if HOST, PORT, DATABASE, or SSL options
      // are provided with a descriptor
      assertThrows(IllegalArgumentException.class, () ->
        ConnectionFactories.get(
          "r2dbc:oracle://"+host()+"?oracle.r2dbc.descriptor="+descriptor));
      assertThrows(IllegalArgumentException.class, () ->
        ConnectionFactories.get("r2dbc:oracle://"
            +host()+":"+port()+"?oracle.r2dbc.descriptor="+descriptor));
      assertThrows(IllegalArgumentException.class, () ->
        ConnectionFactories.get("r2dbc:oracle://"+host()+":"+port()+"/"
          +serviceName()+"?oracle.r2dbc.descriptor="+descriptor));
      assertThrows(IllegalArgumentException.class, () ->
        ConnectionFactories.get("r2dbc:oracle://"+host()+"/"
          +serviceName()+"?oracle.r2dbc.descriptor="+descriptor));
      assertThrows(IllegalArgumentException.class, () ->
        ConnectionFactories.get("r2dbc:oracle:///"
            +serviceName()+"?oracle.r2dbc.descriptor="+descriptor));
      assertThrows(IllegalArgumentException.class, () ->
        ConnectionFactories.get("r2dbcs:oracle://" + // r2dbcs is SSL=true
          "?oracle.r2dbc.descriptor="+descriptor));
      assertThrows(IllegalArgumentException.class, () ->
        ConnectionFactories.get(
          "r2dbc:oracle://?oracle.r2dbc.descriptor="+descriptor+"&ssl=true"));

      // Create an ojdbc.properties file containing the user name
      Files.writeString(Path.of("ojdbc.properties"),
        format("user=%s", user()),
        StandardOpenOption.CREATE_NEW);
      try {
        // Expect to connect with the tnsnames.ora and ojdbc.properties files,
        // when a URL specifies their path and an alias, the properties file
        // specifies a user, and a standard option specifies the password.
        awaitNone(awaitOne(
          ConnectionFactories.get(ConnectionFactoryOptions.parse(format(
            "r2dbc:oracle://?oracle.r2dbc.descriptor=%s&TNS_ADMIN=%s",
            "test_alias", userDir))
            .mutate()
            .option(PASSWORD, password())
            .build())
            .create())
            .close());
      }
      finally {
        Files.delete(Path.of("ojdbc.properties"));
      }
    }
    finally {
      Files.delete(Path.of("tnsnames.ora"));
    }
  }

  /**
   * Verifies that the {@link ConnectionFactoryOptions#CONNECT_TIMEOUT} option
   * is applied, as specified in the javadoc of
   * {@link OracleReactiveJdbcAdapter#createDataSource(ConnectionFactoryOptions)}
   */
  @Test
  public void testConnectTimeout()
    throws InterruptedException, ExecutionException, TimeoutException,
      IOException {

    // Create a server that doesn't allow the driver to complete connection
    // establishment. This will ensure that the timeout expires.
    try (ServerSocketChannel listeningChannel = ServerSocketChannel.open()) {
      listeningChannel.configureBlocking(true);
      listeningChannel.bind(null);

      verifyConnectTimeout(listeningChannel, ConnectionFactoryOptions.builder()
        .option(DRIVER, "oracle")
        .option(HOST, "localhost")
        .option(PORT, listeningChannel.socket().getLocalPort())
        .option(DATABASE, serviceName())
        .option(CONNECT_TIMEOUT, Duration.ofSeconds(2))
        .build());

      verifyConnectTimeout(listeningChannel, ConnectionFactoryOptions.parse(
        "r2dbc:oracle://localhost:" + listeningChannel.socket().getLocalPort()
          + "?connectTimeout=PT2S")); // The value is parsed as a Duration

      verifyConnectTimeout(listeningChannel, ConnectionFactoryOptions.builder()
        .option(DRIVER, "oracle")
        .option(HOST, "localhost")
        .option(PORT, listeningChannel.socket().getLocalPort())
        .option(DATABASE, serviceName())
        .option(CONNECT_TIMEOUT, Duration.ofMillis(500))
        .build());

      verifyConnectTimeout(listeningChannel, ConnectionFactoryOptions.parse(
        "r2dbc:oracle://localhost:" + listeningChannel.socket().getLocalPort()
          + "?connectTimeout=PT0.5S")); // The value is parsed as a Duration
    }
  }

  /**
   * Verifies that the {@link ConnectionFactoryOptions#STATEMENT_TIMEOUT} option
   * is applied, as specified in the javadoc of
   * {@link OracleReactiveJdbcAdapter#createDataSource(ConnectionFactoryOptions)}
   */
  @Test
  public void testStatementTimeout() {
    Connection connection0 =
      Mono.from(ConnectionFactories.get(connectionFactoryOptions()
        .mutate()
        .option(STATEMENT_TIMEOUT, Duration.ofSeconds(2))
        // Disable OOB to support testing with an 18.x database
        .option(Option.valueOf(
          OracleConnection.CONNECTION_PROPERTY_THIN_NET_DISABLE_OUT_OF_BAND_BREAK),
          "true")
        .build())
        .create())
        .block(connectTimeout());
    Connection connection1 =
      Mono.from(sharedConnection()).block(connectTimeout());

    try {
      // Lock a table
      awaitExecution(connection1.createStatement(
        "CREATE TABLE testStatementTimeout (v NUMBER)"));
      awaitUpdate(1, connection1.createStatement(
        "INSERT INTO testStatementTimeout VALUES(0)"));
      Result lockingResult = Mono.from(connection1.createStatement(
        "SELECT * FROM testStatementTimeout FOR UPDATE")
        .execute())
        .block(sqlTimeout());

      try {
        // Attempt to update the locked table. Expect the 2 second timeout to
        // trigger. Allow up to 10 seconds of variance to account for slow
        // systems.
        Duration start = Duration.ofNanos(System.nanoTime());
        awaitError(R2dbcTimeoutException.class,
          Mono.from(connection0.createStatement(
            "UPDATE testStatementTimeout SET v=1 WHERE v=0")
            .execute())
            .flatMapMany(Result::getRowsUpdated));
        Duration actual = Duration.ofNanos(System.nanoTime()).minus(start);
        assertTrue(actual.toSeconds() >= 2,
          "Timeout triggered too soon: " + actual);
        assertTrue(actual.toSeconds() < 12,
          "Timeout triggered too late: " + actual);
      }
      finally {
        // Consume the result to close the cursor
        awaitNone(lockingResult.getRowsUpdated());
      }
    }
    finally {
      tryAwaitNone(connection0.close());
      tryAwaitExecution(connection1.createStatement(
        "DROP TABLE testStatementTimeout"));
      tryAwaitNone(connection1.close());
    }

  }

  /**
   * Verifies the {@link oracle.r2dbc.OracleR2dbcOptions#EXECUTOR} option
   */
  @Test
  public void testExecutorOption() {

    // Create a custom executor that increments a count when Runnables are
    // submitted, and then delegates to a single threaded executor.
    AtomicInteger count = new AtomicInteger(0);
    ExecutorService singleThread = Executors.newSingleThreadExecutor();
    Executor testExecutor = runnable -> {
      count.incrementAndGet();
      singleThread.execute(runnable);
    };

    // Create a connection that is configured to use the custom executor
    Connection connection = awaitOne(ConnectionFactories.get(
      connectionFactoryOptions()
        .mutate()
        .option(OracleR2dbcOptions.EXECUTOR, testExecutor)
        .build())
        .create());

    try {
      // Make some asynchronous database calls and expect the executor's
      // count to be incremented
      awaitOne(Set.of(0, 1, 2, 3), Flux.merge(
        connection.createStatement("SELECT 0 FROM sys.dual").execute(),
        connection.createStatement("SELECT 1 FROM sys.dual").execute(),
        connection.createStatement("SELECT 2 FROM sys.dual").execute(),
        connection.createStatement("SELECT 3 FROM sys.dual").execute())
        .flatMap(result ->
          result.map(row -> row.get(0, Integer.class)))
        .collect(Collectors.toSet()));
      assertTrue(count.get() != 0);
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the
   * {@link OracleR2dbcOptions#VSESSION_OSUSER},
   * {@link OracleR2dbcOptions#VSESSION_TERMINAL},
   * {@link OracleR2dbcOptions#VSESSION_PROCESS},
   * {@link OracleR2dbcOptions#VSESSION_PROGRAM}, and
   * {@link OracleR2dbcOptions#VSESSION_MACHINE} options.
   */
  @Test
  public void testVSessionOptions() {
    String osuser = "test-osuser";
    String terminal = "test-terminal";
    String process = "test-process";
    String program = "test-program";
    String machine = "test-machine";

    // Verify configuration with URL parameters
    Connection connection = awaitOne(ConnectionFactories.get(
      ConnectionFactoryOptions.parse(
        format("r2dbc:oracle:%s//%s:%d/%s" +
          "?v$session.osuser=%s" +
          "&v$session.terminal=%s" +
          "&v$session.process=%s" +
          "&v$session.program=%s" +
          "&v$session.machine=%s",
          Optional.ofNullable(protocol())
            .map(protocol -> protocol + ":")
            .orElse(""),
          host(), port(), serviceName(),
          osuser, terminal, process, program, machine))
      .mutate()
      .option(USER, user())
      .option(PASSWORD, password())
      .build())
      .create());
    try {
      Result result = awaitOne(connection.createStatement(
        "SELECT count(*)" +
          " FROM v$session" +
          " WHERE osuser=?" +
          " AND terminal=?" +
          " AND process=?" +
          " AND program=?" +
          " AND machine=?")
        .bind(0, osuser)
        .bind(1, terminal)
        .bind(2, process)
        .bind(3, program)
        .bind(4, machine)
        .execute());

      assertEquals(
        Integer.valueOf(1),
        awaitOne(result.map(row -> row.get(0, Integer.class))));
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }
  /**
   * Verifies the use of the LDAP protocol in an r2dbc:oracle URL.
   */
  @Test
  public void testLdapUrl() throws Exception {

    // Configure Oracle R2DBC with an R2DBC URL having the LDAP protocol and the
    // given path.
    String ldapPath = "sales,cn=OracleContext,dc=com";
    ConnectionFactory ldapConnectionFactory = ConnectionFactories.get(
      ConnectionFactoryOptions.parse(format(
          "r2dbc:oracle:ldap://ldap.example.com:9999/%s", ldapPath))
        .mutate()
        .option(ConnectionFactoryOptions.USER, DatabaseConfig.user())
        .option(ConnectionFactoryOptions.PASSWORD, DatabaseConfig.password())
        .build());

    // Set up the mock LDAP context factory. See JavaDoc of TestContextFactory
    // for details about this.
    TestContextFactory.bind(ldapPath, createDescriptor());

    // Now verify that the LDAP URL is resolved to the descriptor
    Connection ldapConnection = awaitOne(ldapConnectionFactory.create());
    try {
      assertEquals(
        "Hello, LDAP",
        awaitOne(
          awaitOne(ldapConnection.createStatement(
              "SELECT 'Hello, LDAP' FROM sys.dual")
            .execute())
            .map(row -> row.get(0))));
    }
    finally {
      tryAwaitNone(ldapConnection.close());
    }
  }

  /**
   * Verifies the use of the LDAP protocol in an r2dbc:oracle URL having
   * multiple LDAP endpoints
   */
  @Test
  public void testMultiLdapUrl() throws Exception {

    // Configure Oracle R2DBC with an R2DBC URL having the LDAP protocol and
    // multiple LDAP endpoints. Only the last endpoint will contain the given
    // path, and so the previous endpoints are invalid.
    String ldapPath = "cn=salesdept,cn=OracleContext,dc=com/salesdb";
    ConnectionFactory ldapConnectionFactory = ConnectionFactories.get(
      ConnectionFactoryOptions.parse(format(
        "r2dbc:oracle:" +
          "ldap://ldap1.example.com:7777/cn=salesdept0,cn=OracleContext,dc=com/salesdb" +
          "%%20ldap://ldap1.example.com:7777/cn=salesdept1,cn=OracleContext,dc=com/salesdb" +
          "%%20ldap://ldap3.example.com:7777/%s", ldapPath))
        .mutate()
        .option(ConnectionFactoryOptions.USER, DatabaseConfig.user())
        .option(ConnectionFactoryOptions.PASSWORD, DatabaseConfig.password())
        .build());

    // Set up the mock LDAP context factory. A descriptor is bound to the last
    // endpoint only. See JavaDoc of TestContextFactory for details about this.
    TestContextFactory.bind("salesdb", createDescriptor());

    // Now verify that the LDAP URL is resolved to the descriptor
    Connection ldapConnection = awaitOne(ldapConnectionFactory.create());
    try {
      assertEquals(
        "Hello, LDAP",
        awaitOne(
          awaitOne(ldapConnection.createStatement(
            "SELECT 'Hello, LDAP' FROM sys.dual")
            .execute())
            .map(row -> row.get(0))));
    }
    finally {
      tryAwaitNone(ldapConnection.close());
    }
  }

  /**
   * Returns an Oracle Net Descriptor having the values configured by
   * {@link DatabaseConfig}
   * @return An Oracle Net Descriptor for the test database.
   */
  private static String createDescriptor() {
    return format(
      "(DESCRIPTION=(ADDRESS=(HOST=%s)(PORT=%d)(PROTOCOL=%s))" +
        "(CONNECT_DATA=(SERVICE_NAME=%s)))",
      host(), port(),
      Objects.requireNonNullElse(protocol(), "tcp"),
      serviceName());
  }

  /**
   * Verifies the {@link OracleR2dbcOptions#TIMEZONE_AS_REGION} option
   */
  @Test
  public void testTimezoneAsRegion() {
    // Set the timezone to that of Warsaw. When JDBC opens a connection, it will
    // read the Warsaw timezone from TimeZone.getDefault().
    TimeZone warsawTimeZone = TimeZone.getTimeZone("Europe/Warsaw");
    TimeZone timeZoneRestored = TimeZone.getDefault();
    TimeZone.setDefault(warsawTimeZone);
    try {

      // Configure the JDBC connection property with a URL parameter. This has
      // JDBC express the session timezone as an offset of UTC (+02:00), rather
      // than a name (Europe/Warsaw).
      Connection connection = awaitOne(ConnectionFactories.get(
        ConnectionFactoryOptions.parse(format(
          "r2dbc:oracle://%s:%d/%s?oracle.jdbc.timezoneAsRegion=false",
          host(), port(), serviceName()))
          .mutate()
          .option(USER, user())
          .option(PASSWORD, password())
          .build())
        .create());
      try {

        // Query the session timezone, and expect it to be expressed as an
        // offset, rather than a name.
        assertEquals(
          ZonedDateTime.now(warsawTimeZone.toZoneId())
            .getOffset()
            .toString(),
          awaitOne(awaitOne(connection.createStatement(
            "SELECT sessionTimeZone FROM sys.dual")
            .execute())
            .map(row ->
              row.get(0, String.class))));
      }
      finally {
        tryAwaitNone(connection.close());
      }
    }
    finally {
      TimeZone.setDefault(timeZoneRestored);
    }
  }

  /**
   * Verifies that an attempt to connect with a {@code listeningChannel}
   * results in an {@link R2dbcTimeoutException}.
   * @param listeningChannel Listens for an R2DBC connection
   */
  private void verifyConnectTimeout(
    ServerSocketChannel listeningChannel, ConnectionFactoryOptions options)
    throws InterruptedException, ExecutionException, TimeoutException,
    IOException {

    // Accept the R2DBC driver's connection, and then leave it hanging. The
    // timeout should expire.
    CompletableFuture<SocketChannel> r2dbcConnectFuture =
      CompletableFuture.supplyAsync(() -> {
        try {
          return listeningChannel.accept();
        }
        catch (IOException ioException) {
          throw new CompletionException(ioException);
        }
      }, runnable -> new Thread(runnable).start());

    // Try to connect
    try {
      long start = System.currentTimeMillis();
      awaitError(R2dbcTimeoutException.class,
        ConnectionFactories.get(options).create());

      // Compare the actual duration to the configured timeout
      Duration timeoutDelta =
        Duration.ofMillis(System.currentTimeMillis() - start)
          .minus(Duration.parse(
            options.getValue(Option.valueOf("connectTimeout")).toString()));

      // Expect the actual duration to be at least as much as the configured
      // timeout
      if (timeoutDelta.isNegative())
        fail("Timeout triggered too soon: " + timeoutDelta);

      // Expect the actual duration to be no more than two seconds longer
      // than the configured timeout. This should be a generous enough offset
      // to allow for testing on slow systems
      if (timeoutDelta.toSeconds() > 2)
        fail("Timeout triggered too late: " + timeoutDelta);

    }
    catch (Throwable unexpected) {
      unexpected.printStackTrace();
    }
    finally {
      // Close the connection that was accepted
      r2dbcConnectFuture
        .get(connectTimeout().toSeconds(), TimeUnit.SECONDS)
        .close();
    }
  }
}
