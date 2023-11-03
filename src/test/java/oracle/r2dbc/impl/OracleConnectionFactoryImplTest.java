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
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.R2dbcException;
import oracle.r2dbc.test.DatabaseConfig;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.SQLException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static oracle.r2dbc.test.DatabaseConfig.connectionFactoryOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies that
 * {@link OracleConnectionFactoryImpl} implements behavior that is specified
 * in it's class and method level javadocs.
 */
public class OracleConnectionFactoryImplTest {

  /**
   * Verifies that {@link OracleConnectionFactoryImpl} can be discovered by
   * {@link ConnectionFactories}.
   */
  @Test
  public void testDiscovery() {
    assertEquals(
      OracleConnectionFactoryImpl.class,
      ConnectionFactories
        .get(ConnectionFactoryOptions.builder()
          .option(ConnectionFactoryOptions.DRIVER, "oracle")
          .option(HOST, "dbhost")
          .option(PORT, 1521)
          .option(ConnectionFactoryOptions.DATABASE, "service_name")
          .build())
        .getClass());

    assertEquals(
      OracleConnectionFactoryImpl.class,
      ConnectionFactories
        .find(ConnectionFactoryOptions.builder()
          .option(ConnectionFactoryOptions.DRIVER, "oracle")
          .option(HOST, "dbhost")
          .option(PORT, 1521)
          .option(ConnectionFactoryOptions.DATABASE, "service_name")
          .build())
        .getClass());

    assertEquals(
      OracleConnectionFactoryImpl.class,
      ConnectionFactories
        .get("r2dbc:oracle://dbhost:1521/service_name")
        .getClass());

    assertEquals(
      OracleConnectionFactoryImpl.class,
      ConnectionFactories
        .get("r2dbc:oracle://user:password@dbhost:1521/service_name")
        .getClass());

    assertEquals(
      OracleConnectionFactoryImpl.class,
      ConnectionFactories
        .get(
          "r2dbc:oracle://user:password@dbhost:1521/service_name?option=value")
        .getClass());
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionFactoryImpl#create()}
   * TODO: Verify resource allocation upon Subscription.request(long), and
   * resource release upon Subscription.cancel(). Consider querying V$SESSION
   * to check if JDBC connections are being opened and closed correctly.
   */
  @Test
  public void testCreate() {
    Publisher<? extends Connection> connectionPublisher =
      new OracleConnectionFactoryImpl(connectionFactoryOptions()).create();
    verifyConnectionPublisher(connectionPublisher);
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionFactoryImpl#create()} when connection creation
   * fails.
   */
  @Test
  public void testCreateFailure() {
    // Connect with the wrong username
    Publisher<? extends Connection> connectionPublisher =
      new OracleConnectionFactoryImpl(connectionFactoryOptions()
        .mutate()
        .option(ConnectionFactoryOptions.USER,
          "Wrong" + DatabaseConfig.user())
        .build())
        .create();

    // Expect publisher to signal onError with an R2DBCException
    try {
      Flux.from(connectionPublisher)
        .doOnNext(connection -> Mono.from(connection.close()).subscribe())
        .blockLast(DatabaseConfig.connectTimeout());
      fail("Connection publisher did not signal onError");
    }
    catch (R2dbcException expected) {
      Throwable cause = expected.getCause();
      assertTrue(
        cause instanceof SQLException,
        "Unexpected type returned by R2dbcException.getCause(): " +
          cause.getClass());

      assertEquals(
        1017, // ORA-01017 is the error code for a wrong user name
        ((SQLException)cause).getErrorCode());
    }

    // Expect publisher to reject multiple subscribers
    try {
      Mono.from(connectionPublisher)
        .block(Duration.ofSeconds(1));
      fail("Connection publisher did not reject multiple subscribers");
    }
    catch (IllegalStateException expected) { }
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionFactoryImpl#getMetadata()}
   */
  @Test
  public void testGetMetadata() {
    assertEquals("Oracle Database",
      new OracleConnectionFactoryImpl(
        ConnectionFactoryOptions.builder()
          .option(ConnectionFactoryOptions.DRIVER, "oracle")
          .option(HOST, "dbhost")
          .option(PORT, 1521)
          .option(ConnectionFactoryOptions.DATABASE, "service_name")
          .build())
        .getMetadata()
        .getName());
  }

  /** Verifies that a publisher emits connections to multiple subscribers */
  private static void verifyConnectionPublisher(
    Publisher<? extends Connection> connectionPublisher) {

    // Expect publisher to emit one connection to each subscriber
    Set<Connection> connections = new HashSet<>();
    Flux.from(connectionPublisher)
      .doOnNext(connections::add)
      .doOnNext(connection -> Mono.from(connection.close()).subscribe())
      .blockLast(DatabaseConfig.connectTimeout());
    assertEquals(1, connections.size());
    Flux.from(connectionPublisher)
      .doOnNext(connections::add)
      .doOnNext(connection -> Mono.from(connection.close()).subscribe())
      .blockLast(DatabaseConfig.connectTimeout());
    assertEquals(2, connections.size());
    Flux.from(connectionPublisher)
      .doOnNext(connections::add)
      .doOnNext(connection -> Mono.from(connection.close()).subscribe())
      .blockLast(DatabaseConfig.connectTimeout());
    assertEquals(3, connections.size());
  }

}
