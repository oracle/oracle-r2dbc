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
import io.r2dbc.spi.R2dbcException;
import oracle.r2dbc.test.DatabaseConfig;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static oracle.r2dbc.OracleR2dbcOptions.published;
import static oracle.r2dbc.OracleR2dbcOptions.supplied;
import static oracle.r2dbc.test.DatabaseConfig.connectionFactoryOptions;
import static oracle.r2dbc.test.DatabaseConfig.password;
import static oracle.r2dbc.util.Awaits.awaitError;
import static oracle.r2dbc.util.Awaits.awaitOne;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies that
 * {@link OracleConnectionFactoryProviderImpl} implements behavior that
 * is specified in it's class and method level javadocs.
 */
public class OracleConnectionFactoryProviderImplTest {

  /**
   * Verifies that {@link OracleConnectionFactoryProviderImpl} is identified
   * a service provider for {@link io.r2dbc.spi.ConnectionFactoryProvider}
   */
  @Test
  public void testDiscovery() {
    assertTrue(
      ConnectionFactories.supports(
        ConnectionFactoryOptions.builder()
          .option(ConnectionFactoryOptions.DRIVER, "oracle")
          .build()));
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionFactoryProviderImpl#create(io.r2dbc.spi.ConnectionFactoryOptions)}
   */
  @Test
  public void testCreate() {
    OracleConnectionFactoryProviderImpl provider =
      new OracleConnectionFactoryProviderImpl();

    try {
      provider.create(null);
      fail("create(null) did not throw IllegalArgumentException");
    }
    catch (IllegalArgumentException expected) {
      // IllegalArgumentException is expected with a null argument
    }

    try {
      provider.create(
        ConnectionFactoryOptions.builder()
          .option(ConnectionFactoryOptions.DRIVER, "oracle")
          .build());
      fail(
        "create(ConnectionFactoryOptions) did not throw IllegalStateException");
    }
    catch (IllegalStateException expected) {
      // IllegalArgumentException is expected when options required by
      // OracleConnectionFactoryImpl are missing.
    }

    assertNotNull(
      provider.create(
        ConnectionFactoryOptions.builder()
          .option(ConnectionFactoryOptions.DRIVER, "oracle")
          .option(ConnectionFactoryOptions.HOST, "dbhost")
          .option(ConnectionFactoryOptions.PORT, 1521)
          .option(ConnectionFactoryOptions.DATABASE, "service_name")
          .build()));
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionFactoryProviderImpl#getDriver()}
   */
  @Test
  public void testGetDriver() {
    OracleConnectionFactoryProviderImpl provider =
      new OracleConnectionFactoryProviderImpl();

    assertEquals("oracle", provider.getDriver());
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionFactoryProviderImpl#supports(ConnectionFactoryOptions)}
   */
  @Test
  public void testSupports() {
    OracleConnectionFactoryProviderImpl provider =
      new OracleConnectionFactoryProviderImpl();

    try {
      provider.supports(null);
      fail("supports(null) did not throw IllegalArgumentException");
    }
    catch (IllegalArgumentException expected) {
      // Expected to throw IllegalArgumentException given a null argument
    }

    assertTrue(
      provider.supports(
        ConnectionFactoryOptions.builder()
          .option(ConnectionFactoryOptions.DRIVER, "oracle")
          .build()));

    assertFalse(
      provider.supports(
        ConnectionFactoryOptions.builder()
          .option(ConnectionFactoryOptions.DRIVER, "not oracle")
          .build()));

    assertFalse(
      provider.supports(
        ConnectionFactoryOptions.builder()
          .option(ConnectionFactoryOptions.DRIVER, "")
          .build()));

    assertFalse(
      provider.supports(
        ConnectionFactoryOptions.builder()
          .build()));

    Option<String> unsupported = Option.valueOf("not supported by oracle");
    assertTrue(
      provider.supports(
        ConnectionFactoryOptions.builder()
          .option(ConnectionFactoryOptions.DRIVER, "oracle")
          .option(unsupported, "expect Oracle R2DBC to ignore this")
          .build()));
  }

  @Test
  public void testSupplierOption() {
    Supplier<String> hostSupplier = DatabaseConfig::host;
    Supplier<Integer> portSupplier = DatabaseConfig::port;
    Supplier<String> databaseSupplier = DatabaseConfig::serviceName;
    TestSupplier<String> userSupplier =
      new TestSupplier<>(DatabaseConfig.user());
    Supplier<CharSequence> passwordSupplier = DatabaseConfig::password;

    ConnectionFactoryOptions connectionFactoryOptions =
      connectionFactoryOptions()
        .mutate()
        .option(supplied(HOST), hostSupplier)
        .option(supplied(PORT), portSupplier)
        .option(supplied(DATABASE), databaseSupplier)
        .option(supplied(USER), userSupplier)
        .option(supplied(PASSWORD), passwordSupplier)
        .build();

    ConnectionFactory connectionFactory =
      ConnectionFactories.get(connectionFactoryOptions);

    // Round 1: Expect success as all values are supplied
    verifyConnection(connectionFactory);

    // Round 2: Expect ORA-01017 has an invalid username is supplied
    userSupplier.value = "not" + DatabaseConfig.user();
    R2dbcException r2dbcException = verifyConnectionError(connectionFactory);
    assertEquals(1017, r2dbcException.getErrorCode());

    // Round 3: Expect success as all values are supplied
    userSupplier.value = DatabaseConfig.user();
    verifyConnection(connectionFactory);

  }

  @Test
  public void testSupplierOptionNull() {
    Supplier<String> hostSupplier = DatabaseConfig::host;
    Supplier<Integer> portSupplier = DatabaseConfig::port;
    Supplier<String> databaseSupplier = DatabaseConfig::serviceName;
    Supplier<String> userSupplier = DatabaseConfig::user;
    TestSupplier<CharSequence> passwordSupplier = new TestSupplier(password());

    ConnectionFactoryOptions connectionFactoryOptions =
      connectionFactoryOptions()
        .mutate()
        .option(supplied(HOST), hostSupplier)
        .option(supplied(PORT), portSupplier)
        .option(supplied(DATABASE), databaseSupplier)
        .option(supplied(USER), userSupplier)
        .option(supplied(PASSWORD), passwordSupplier)
        .option(
          // Oracle Database doesn't support this option, and Oracle R2DBC
          // throws an exception if it is set. The supplied null value should
          // have it not set.
          supplied(ConnectionFactoryOptions.LOCK_WAIT_TIMEOUT),
          () -> null)
        .build();

    ConnectionFactory connectionFactory =
      ConnectionFactories.get(connectionFactoryOptions);

    // Round 1: Verify success with no lock wait timeout and a password
    verifyConnection(connectionFactory);

    // Round 2: Verify failure with a null password. The expected error code
    // may depend on the version of the test database. Expect ORA-01005 with a
    // 23.3 database.
    passwordSupplier.value = null;
    R2dbcException r2dbcException = verifyConnectionError(connectionFactory);
    assertEquals(1005, r2dbcException.getErrorCode());
  }

  @Test
  public void testSupplierOptionError() {
    class TestException extends RuntimeException { }

    Supplier<String> hostSupplier = DatabaseConfig::host;
    Supplier<Integer> portSupplier = DatabaseConfig::port;
    Supplier<String> databaseSupplier = DatabaseConfig::serviceName;
    TestSupplier<String> userSupplier = new TestSupplier<>(new TestException());
    Supplier<CharSequence> passwordSupplier = DatabaseConfig::password;

    ConnectionFactoryOptions connectionFactoryOptions =
      connectionFactoryOptions()
        .mutate()
        .option(supplied(HOST), hostSupplier)
        .option(supplied(PORT), portSupplier)
        .option(supplied(DATABASE), databaseSupplier)
        .option(supplied(USER), userSupplier)
        .option(supplied(PASSWORD), passwordSupplier)
        .build();

    ConnectionFactory connectionFactory =
      ConnectionFactories.get(connectionFactoryOptions);

    // Round 1: Expect a failure from the TestSupplier
    R2dbcException r2dbcException = verifyConnectionError(connectionFactory);

    assertTrue(
      r2dbcException.getCause() instanceof TestException,
      "Unexpected cause: " + r2dbcException.getCause());

    // Round 2: Expect success as the TestSupplier no longer throws an error
    userSupplier.error = null;
    userSupplier.value = DatabaseConfig.user();
    verifyConnection(connectionFactory);
  }

  @Test
  public void testPublisherOption() {
    Publisher<String> hostPublisher = Mono.fromSupplier(DatabaseConfig::host);
    Publisher<Integer> portPublisher = Mono.fromSupplier(DatabaseConfig::port);
    Publisher<String> databasePublisher = Mono.fromSupplier(DatabaseConfig::serviceName);
    TestSupplier<String> userPublisher =
      new TestSupplier<>(DatabaseConfig.user());
    Publisher<CharSequence> passwordPublisher = Mono.fromSupplier(DatabaseConfig::password);

    ConnectionFactoryOptions connectionFactoryOptions =
      connectionFactoryOptions()
        .mutate()
        .option(published(HOST), hostPublisher)
        .option(published(PORT), portPublisher)
        .option(published(DATABASE), databasePublisher)
        .option(published(USER), Mono.fromSupplier(userPublisher))
        .option(published(PASSWORD), passwordPublisher)
        .build();

    ConnectionFactory connectionFactory =
      ConnectionFactories.get(connectionFactoryOptions);

    // Round 1: Expect success as all values are published
    verifyConnection(connectionFactory);

    // Round 2: Expect ORA-01017 has an invalid username is published
    userPublisher.value = "not" + DatabaseConfig.user();
    R2dbcException r2dbcException = verifyConnectionError(connectionFactory);
    assertEquals(1017, r2dbcException.getErrorCode());

    // Round 3: Expect success as all values are published
    userPublisher.value = DatabaseConfig.user();
    verifyConnection(connectionFactory);

  }

  @Test
  public void testPublisherOptionNull() {
    Publisher<String> hostPublisher = Mono.fromSupplier(DatabaseConfig::host);
    Publisher<Integer> portPublisher = Mono.fromSupplier(DatabaseConfig::port);
    Publisher<String> databasePublisher = Mono.fromSupplier(DatabaseConfig::serviceName);
    Publisher<String> userPublisher = Mono.fromSupplier(DatabaseConfig::user);
    TestSupplier<CharSequence> passwordPublisher = new TestSupplier(password());

    ConnectionFactoryOptions connectionFactoryOptions =
      connectionFactoryOptions()
        .mutate()
        .option(published(HOST), hostPublisher)
        .option(published(PORT), portPublisher)
        .option(published(DATABASE), databasePublisher)
        .option(published(USER), userPublisher)
        .option(published(PASSWORD), Mono.fromSupplier(passwordPublisher))
        .option(
          // Oracle Database doesn't support this option, and Oracle R2DBC
          // throws an exception if it is set. The published null value should
          // have it not set.
          published(ConnectionFactoryOptions.LOCK_WAIT_TIMEOUT),
          Mono.empty())
        .build();

    ConnectionFactory connectionFactory =
      ConnectionFactories.get(connectionFactoryOptions);

    // Round 1: Verify success with no lock wait timeout and a password
    verifyConnection(connectionFactory);

    // Round 2: Verify failure with a null password. The expected error code
    // may depend on the version of the test database. Expect ORA-01005 with a
    // 23.3 database.
    passwordPublisher.value = null;
    R2dbcException r2dbcException = verifyConnectionError(connectionFactory);
    assertEquals(1005, r2dbcException.getErrorCode());
  }

  @Test
  public void testPublisherOptionError() {
    class TestException extends RuntimeException { }

    Publisher<String> hostPublisher = Mono.fromSupplier(DatabaseConfig::host);
    Publisher<Integer> portPublisher = Mono.fromSupplier(DatabaseConfig::port);
    Publisher<String> databasePublisher = Mono.fromSupplier(DatabaseConfig::serviceName);
    TestSupplier<String> userPublisher = new TestSupplier<>(new TestException());
    Publisher<CharSequence> passwordPublisher = Mono.fromSupplier(DatabaseConfig::password);

    ConnectionFactoryOptions connectionFactoryOptions =
      connectionFactoryOptions()
        .mutate()
        .option(published(HOST), hostPublisher)
        .option(published(PORT), portPublisher)
        .option(published(DATABASE), databasePublisher)
        .option(published(USER), Mono.fromSupplier(userPublisher))
        .option(published(PASSWORD), passwordPublisher)
        .build();

    ConnectionFactory connectionFactory =
      ConnectionFactories.get(connectionFactoryOptions);

    // Round 1: Expect a failure from the TestSupplier
    R2dbcException r2dbcException = verifyConnectionError(connectionFactory);

    assertTrue(
      r2dbcException.getCause() instanceof TestException,
      "Unexpected cause: " + r2dbcException.getCause());

    // Round 2: Expect success as the TestSupplier no longer throws an error
    userPublisher.error = null;
    userPublisher.value = DatabaseConfig.user();
    verifyConnection(connectionFactory);
  }

  /** Verifies that a connection can be created with the given options */
  private void verifyConnection(ConnectionFactory connectionFactory) {

    awaitOne(
      1,
      Flux.usingWhen(
        connectionFactory.create(),
        connection ->
          Flux.from(
              connection.createStatement("SELECT 1 FROM sys.dual").execute())
            .flatMap(result ->
              result.map(row -> row.get(0, Integer.class))),
        Connection::close));
  }

  /**
   * Verifies that a connection fails to be created with the given options,
   * and returns the exception.
   */
  private R2dbcException verifyConnectionError(
    ConnectionFactory connectionFactory) {

    return awaitError(
      R2dbcException.class,
      Flux.usingWhen(
        connectionFactory.create(),
        connection ->
          Flux.from(
              connection.createStatement("SELECT 1 FROM sys.dual").execute())
            .flatMap(result ->
              result.map(row -> row.get(0, Integer.class))),
        Connection::close));
  }

  private static final class TestSupplier<T> implements Supplier<T> {

    T value;

    RuntimeException error;

    TestSupplier(RuntimeException error) {
      this.error = error;
    }

    TestSupplier(T value) {
      this.value = value;
    }

    @Override
    public T get() {
      if (error != null)
        throw error;

      return value;
    }
  }

}
