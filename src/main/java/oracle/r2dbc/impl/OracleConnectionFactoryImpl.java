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
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import javax.sql.DataSource;
import java.time.Duration;

/**
 * <p>
 * Implementation of the {@link ConnectionFactory} SPI for Oracle Database.
 * </p><p>
 * Instances of this class open database connections using a JDBC
 * {@link javax.sql.DataSource}. JDBC API calls are adapted into Reactive
 * Streams APIs using a {@link ReactiveJdbcAdapter}.
 * </p><p>
 * {@code Connections} created by this factory are initially
 * configured with {@linkplain Connection#isAutoCommit() auto-commit} mode
 * enabled and have a {@linkplain Connection#getTransactionIsolationLevel()
 * transaction isolation level} of {@linkplain IsolationLevel#READ_COMMITTED
 * READ_COMMITTED}.
 * </p>
 * <h3 id="required_options">Required Options</h3><p>
 * This implementation requires the following options for connection creation:
 * </p><dl>
 *   <dt>{@link ConnectionFactoryOptions#DRIVER}</dt>
 *   <dd>Must have the value "oracle"</dd>
 *   <dt>{@link ConnectionFactoryOptions#HOST}</dt>
 *   <dd>IP address or hostname of an Oracle Database</dd>
 * </dl>
 * <h3 id="supported_options">Supported Options</h3><p>
 * This implementation supports the following options for connection creation:
 * </p><dl>
 *   <dt>{@link ConnectionFactoryOptions#PORT}</dt>
 *   <dd>Port number of an Oracle Database</dd>
 *   <dt>{@link ConnectionFactoryOptions#DATABASE}</dt>
 *   <dd>Service name (not an SID) of an Oracle Database</dd>
 *   <dt>{@link ConnectionFactoryOptions#USER}</dt>
 *   <dd>Name of an Oracle Database user</dd>
 *   <dt>{@link ConnectionFactoryOptions#PASSWORD}</dt>
 *   <dd>
 *     Password of an Oracle Database user. The value may be an instance
 *     of a mutable {@link CharSequence}, such {@link java.nio.CharBuffer},
 *     that may be cleared after creating an instance of
 *     {@code OracleConnectionFactoryImpl}.
 *   </dd>
 *   <dt>{@link ConnectionFactoryOptions#CONNECT_TIMEOUT}</dt>
 *   <dd>
 *     Maximum wait time when requesting a {@code Connection}. If the
 *     duration expires, a {@code Connection} {@code Subscriber} receives
 *     {@code onError} with an {@link io.r2dbc.spi.R2dbcTimeoutException}.
 *     The duration is rounded up to the nearest whole second. The query
 *     section of an R2DBC URL may provide a value in the format specified by
 *     {@link Duration#parse(CharSequence)}.
 *   </dd>
 *   <dt>{@link ConnectionFactoryOptions#SSL}</dt>
 *   <dd>
 *     If set to {@code true}, the driver connects to Oracle Database using
 *     TCPS (ie: SSL/TLS).
 *   </dd>
 * </dl>
 *
 * @author  harayuanwang, michael-a-mcmahon
 * @since   0.1.0
 */
final class OracleConnectionFactoryImpl implements ConnectionFactory {

  /** Adapts Oracle JDBC Driver APIs into Reactive Streams APIs */
  private final ReactiveJdbcAdapter adapter;

  /** JDBC data source that this factory uses to open connections */
  private final DataSource dataSource;

  /**
   * <p>
   * Constructs a new factory that applies the values specified by the {@code
   * options} parameter when opening a database connection. This constructor
   * fails if any <a href="#required_options">required options</a>
   * are not specified by the {@code options} parameter.
   * </p><p>
   * Where curly brackets {enclose} the name of a required {@code Option},
   * and angle brackets [enclose] the name of an optional {@code Option}, the
   * values specified by the {@code options} parameter are used to compose a
   * JDBC URL for database connectivity as:
   * </p><pre>
   * jdbc:oracle:thin:@{HOST}[:PORT][/DATABASE]
   * </pre><p>
   * Note that the syntax used is {@code /{DATABASE}} and not
   * {@code :{DATABASE}}. This forward slash syntax has the {@code DATABASE}
   * option interpreted as a service name. The {@code DATABASE} option is not
   * interpreted as a system ID (SID) using the colon syntax.
   * </p><p>
   * Traditional database authentication is configured by option values for
   * {@link ConnectionFactoryOptions#USER} with
   * {@link ConnectionFactoryOptions#PASSWORD}. These options are not
   * required because Oracle Database supports alternative methods of
   * authentication that do not require a user name and password, such as
   * trusted TLS certificates.
   * </p><p>
   * Well-known options {@link ConnectionFactoryOptions#CONNECT_TIMEOUT} and
   * {@link ConnectionFactoryOptions#SSL} are supported.
   * </p><p>
   * Any extended options are applied as Oracle JDBC connection properties.
   * An extended option is any option that is not declared by
   * {@link ConnectionFactoryOptions}. See
   * {@link OracleReactiveJdbcAdapter#createDataSource(ConnectionFactoryOptions)}
   * for a list of Oracle JDBC connection properties that are supported.
   * </p>
   *
   * @param options Options applied when opening a connection to a database.
   *
   * @throws IllegalArgumentException If the value of a required option is
   * null.
   *
   * @throws IllegalStateException If the value of a required option is not
   * specified.
   *
   * @throws IllegalArgumentException If the {@code oracle-net-descriptor}
   * {@code Option} is provided with any other options that might have
   * conflicting values, such as {@link ConnectionFactoryOptions#HOST}.
   */
  OracleConnectionFactoryImpl(ConnectionFactoryOptions options) {
    OracleR2dbcExceptions.requireNonNull(options, "options is null.");
    adapter = ReactiveJdbcAdapter.getOracleAdapter();
    dataSource = adapter.createDataSource(options);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by opening a database connection using
   * the JDBC {@link DataSource} that this factory initialized when it was
   * constructed.
   * </p><p>
   * The returned publisher does not attempt to open a JDBC connection until
   * a subscriber has signalled demand, and either emits a single connection or
   * emits {@code onError} with an {@link R2dbcException}. The returned
   * publisher releases any resources allocated to a JDBC connection if a
   * subscriber cancels it's subscription <i>before</i> the returned
   * publisher has emitted a connection. Subscribers MUST eventually
   * {@linkplain Connection#close() close} any connection that is emitted by
   * the returned publisher, so that the database can reclaim the resources
   * allocated for that connection.
   * </p><p>
   * The returned publisher does not support multiple subscribers. After a
   * subscriber has subscribed, the returned publisher emits {@code onError}
   * with an {@link IllegalStateException} to all subsequent subscribers.
   * </p>
   */
  @Override
  public Publisher<Connection> create() {
    return Mono.fromDirect(adapter.publishConnection(dataSource))
      .map(conn -> new OracleConnectionImpl(adapter, conn));
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning an implementation of the
   * {@code ConnectionFactoryMetaData} SPI that names "Oracle Database" as the
   * database product that this factory's JDBC datasource can open
   * connections to.
   * </p>
   */
  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return () -> "Oracle Database";
  }
}