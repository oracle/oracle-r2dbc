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
import io.r2dbc.spi.Option;
import oracle.jdbc.OracleConnection;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;

import static oracle.r2dbc.samples.DatabaseConfig.HOST;
import static oracle.r2dbc.samples.DatabaseConfig.PASSWORD;
import static oracle.r2dbc.samples.DatabaseConfig.PORT;
import static oracle.r2dbc.samples.DatabaseConfig.SERVICE_NAME;
import static oracle.r2dbc.samples.DatabaseConfig.USER;
import static oracle.r2dbc.samples.DatabaseConfig.WALLET_LOCATION;

/**
 * Sample code that uses an Oracle Wallet to authenticate with an Oracle
 * Database instance. To read the wallet, the Oracle PKI library must be on the
 * JVM classpath when this demo is run. The maven coordinates for Oracle PKI
 * are:
 * <pre>
 *  <dependency>
 *    <groupId>com.oracle.database.security</groupId>
 *    <artifactId>oraclepki</artifactId>
 *    <version>21.1.0.0</version>
 *  </dependency>
 *  <dependency>
 *    <groupId>com.oracle.database.security</groupId>
 *    <artifactId>osdt_cert</artifactId>
 *    <version>21.1.0.0</version>
 *  </dependency>
 *  <dependency>
 *    <groupId>com.oracle.database.security</groupId>
 *    <artifactId>osdt_core</artifactId>
 *    <version>21.1.0.0</version>
 *  </dependency>
 * </pre>
 *
 * Database connection configuration, including the wallet location, must be
 * written to a file named "config.properties" that exists in the current
 * directory when this demo is run. There is an example config.properties
 * file in the /sample directory (relative to the root this repository).
 */
public class TcpsConnectDemo {


  public static void main(String[] args) throws URISyntaxException {

    // The R2DBC URL format can configure a TCPS/SSL/TLS enabled connection
    // by specifying the "r2dbcs" schema (rather than "r2dbc"). The path to a
    // wallet file is specified in the query section of the URL as the value of
    // an Oracle JDBC connection property: "oracle.net.wallet_location"
    //
    // Special characters that appear in an R2DBC URL must be escaped using
    // percent encoding. Characters that don't require an escape are limited
    // to a-z, A-Z, 0-9, and a few other symbols. All other characters must be
    // encoded as a percent sign (%) followed by the hexadecimal digits of
    // the character's byte value. Multi-byte characters are escaped as a
    // sequence percent encodings, each representing one byte.
    //
    // Fortunately, the java.net.URI class implements percent encoding, so it
    // is used by the code below to generate an R2DBC URL.
    String r2dbcsUrl = new URI("r2dbcs:oracle", // schema
        USER + ":" + PASSWORD, // userInfo
        HOST, // host
        PORT, // port
        "/" + SERVICE_NAME, // path
        "oracle.net.wallet_location=" + WALLET_LOCATION
          + "&oracle.jdbc.fanEnabled=false",  // query
        null) // fragment
      .toASCIIString();

    // The URI.toASCIIString() call above returns a URL like this:
    // r2dbcs:oracle:user:p%40ssword!:host.example.com:1522/service.name?oracle.net.wallet_location=/path/to/wallet/&oracle.jdbc.fanEnabled=false

    // With the r2dbcs URL, open a connection and execute SQL
    Flux.from(sayHello(ConnectionFactories.get(r2dbcsUrl)))
      .doOnNext(System.out::println)
      .blockLast(Duration.ofSeconds(60));

    // As an alternative to the URL, a ConnectionFactoryOptions.Builder
    // offers a programmatic API to configure a ConnectionFactory. If the
    // wallet is password protected, then the Builder API must be used to
    // configure the password. Passwords should never be configured in the URL.
    ConnectionFactoryOptions options =
      ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, "oracle")
        .option(ConnectionFactoryOptions.HOST, HOST)
        .option(ConnectionFactoryOptions.PORT, PORT)
        .option(ConnectionFactoryOptions.DATABASE, SERVICE_NAME)
        .option(ConnectionFactoryOptions.USER, USER)
        .option(ConnectionFactoryOptions.PASSWORD, PASSWORD)
        // To configure a TCPS/SSL/TLS enabled ConnectionFactory, set the SSL
        // option to true, and then specify the path to a wallet location...
        .option(ConnectionFactoryOptions.SSL, true)
        // The Oracle JDBC wallet location connection property can be
        // configured as an R2DBC Option. The connection property name,
        // "oracle.net.wallet_location", is the argument to the
        // Option.valueOf(String) factory method.
        // This is an extended option; It is only recognized by the Oracle
        // R2DBC Driver. Other R2DBC drivers might use a different Option to
        // configure their TLS certificates.
        .option(Option.valueOf(
          OracleConnection.CONNECTION_PROPERTY_WALLET_LOCATION),
          WALLET_LOCATION)
        .option(Option.valueOf(
          OracleConnection.CONNECTION_PROPERTY_FAN_ENABLED),
          "false")
        // To set a wallet password, the Option.sensitiveValueOf(String)
        // factory method is used. Any property that stores a password in
        // clear text needs to be handled carefully; This factory method is
        // used for Options that are configured with sensitive information.
        // .option(Option.sensitiveValueOf(
        //   OracleConnection.CONNECTION_PROPERTY_WALLET_PASSWORD),
        //   readPasswordSecurely()) // TODO: Prompt user to type password?
        .build();

    // Open a connection and execute SQL
    Flux.from(sayHello(
      ConnectionFactories.get(options)))
      .doOnNext(System.out::println)
      .blockLast(Duration.ofSeconds(60));
  }

  /**
   * Publishes the result of opening a connection and executing a SQL query
   * that returns a greeting message.
   * @param connectionFactory Factory configured to open connections
   * @return A publisher that emits a greeting
   */
  static Publisher<String> sayHello(ConnectionFactory connectionFactory) {
    return Mono.from(connectionFactory.create())
      .flatMapMany(connection ->
        Flux.from(connection.createStatement(
          "SELECT 'Hello, Oracle' FROM sys.dual")
          .execute())
          .flatMap(result ->
            result.map((row, metadata) -> row.get(0, String.class)))
          .concatWith(Mono.from(
            connection.close()).cast(String.class)));
  }
}
