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

import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcTimeoutException;
import oracle.jdbc.OracleBlob;
import oracle.jdbc.OracleClob;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleConnectionBuilder;
import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.OracleResultSet;
import oracle.jdbc.OracleRow;
import oracle.jdbc.datasource.OracleDataSource;
import oracle.r2dbc.impl.OracleR2dbcExceptions.ThrowingSupplier;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.getOrHandleSQLException;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.runOrHandleSQLException;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.toR2dbcException;
import static org.reactivestreams.FlowAdapters.toFlowPublisher;
import static org.reactivestreams.FlowAdapters.toFlowSubscriber;
import static org.reactivestreams.FlowAdapters.toPublisher;

/**
 * <p>
 * Implementation of {@link ReactiveJdbcAdapter} for the Oracle JDBC Driver.
 * This adapter is compatible with the 21c release of Oracle JDBC. The
 * implementation adapts the behavior of the Reactive Extensions APIs of Oracle
 * JDBC to conform with the R2DBC standards. Typically, the adaption of Reactive
 * Extensions for R2DBC conformance requires the following:
 * </p><ul>
 *   <li>
 *     Interfacing with {@code org.reactivestreams}: All Reactive Extensions
 *     APIs interface with the {@link Flow} equivalents of the {@code org
 *     .reactivestreams} types.
 *   </li>
 *   <li>
 *     Deferred Execution: Most Reactive Extension APIs do not defer
 *     execution.
 *   </li>
 *   <li>
 *     Type Conversion: Most Reactive Extensions APIs do not emit or consume
 *     the same types as R2DBC SPIs.
 *   </li>
 * </ul><p>
 * A instance of this class is obtained by invoking {@link #getInstance()}.
 * </p><p>
 * All JDBC type parameters supplied to the methods of this class must
 * {@linkplain Wrapper#isWrapperFor(Class) wrap} an Oracle JDBC interface
 * defined in the {@code oracle.jdbc} package. If a method is invoked with a
 * parameter that is not an instance of an {@code oracle.jdbc} subtype, then
 * the method returns a Publisher that signals {@code onError} with a
 * {@link R2dbcException} to all subscribers.
 * </p>
 *
 *  @author  michael-a-mcmahon
 *  @since   0.1.0
 */
final class OracleReactiveJdbcAdapter implements ReactiveJdbcAdapter {

  /** Sole instance of this class */
  private static final OracleReactiveJdbcAdapter INSTANCE =
    new OracleReactiveJdbcAdapter();

  /**
   * The set of JDBC connection properties that this adapter supports. Each
   * property in this set is represented as an {@link Option} having the name
   * of the supported JDBC connection property. When a property is configured
   * with a sensitive value, such as a password, it is represented in this
   * set as a {@linkplain Option#sensitiveValueOf(String) sensitive Option}.
   * If a new Option is added to this set, then it <i>must</i> be documented
   * in the javadoc of {@link #createDataSource(ConnectionFactoryOptions)},
   * and in any other reference that lists which options the Oracle R2DBC Driver
   * supports. Undocumented options are useless; Other programmers won't be
   * able to use an option if they have no way to understand what the option
   * does or how it should be configured.
   */
  private static final Set<Option<CharSequence>>
    SUPPORTED_CONNECTION_PROPERTY_OPTIONS = Set.of(

      // Support TNS_ADMIN (tnsnames.ora, ojdbc.properties).
      Option.valueOf(OracleConnection.CONNECTION_PROPERTY_TNS_ADMIN),

      // Support wallet properties for TCPS/SSL/TLS
      Option.valueOf(OracleConnection.CONNECTION_PROPERTY_WALLET_LOCATION),
      Option.sensitiveValueOf(
        OracleConnection.CONNECTION_PROPERTY_WALLET_PASSWORD),

      // Support keystore properties for TCPS/SSL/TLS
      Option.valueOf(
        OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORE),
      Option.valueOf(
        OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORETYPE),
      Option.sensitiveValueOf(
        OracleConnection
          .CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTOREPASSWORD),

      // Support truststore properties for TCPS/SSL/TLS
      Option.valueOf(
        OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTORE),
      Option.valueOf(
        OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTORETYPE),
      Option.sensitiveValueOf(
        OracleConnection
          .CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTOREPASSWORD),

      // Support authentication services (RADIUS, KERBEROS, and TCPS)
      Option.valueOf(
        OracleConnection.CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_SERVICES),

      // Support fine grained configuration for TCPS/SSL/TLS
      Option.valueOf(
        OracleConnection.CONNECTION_PROPERTY_THIN_SSL_CERTIFICATE_ALIAS),
      Option.valueOf(
        OracleConnection.CONNECTION_PROPERTY_THIN_SSL_SERVER_DN_MATCH),
      Option.valueOf(
        OracleConnection.CONNECTION_PROPERTY_THIN_SSL_SERVER_CERT_DN),
      Option.valueOf(
        OracleConnection.CONNECTION_PROPERTY_THIN_SSL_VERSION),
      Option.valueOf(
        OracleConnection.CONNECTION_PROPERTY_THIN_SSL_CIPHER_SUITES),
      Option.valueOf(
        OracleConnection
          .CONNECTION_PROPERTY_THIN_SSL_KEYMANAGERFACTORY_ALGORITHM),
      Option.valueOf(
        OracleConnection
          .CONNECTION_PROPERTY_THIN_SSL_TRUSTMANAGERFACTORY_ALGORITHM),
      Option.valueOf(
        OracleConnection.CONNECTION_PROPERTY_SSL_CONTEXT_PROTOCOL),

      // Because of bug 32378754, the FAN support in the driver may cause a 10s
      // delay to connect. As a workaround the following property can be set
      // to false to disable FAN support in the driver.
      Option.valueOf(
        OracleConnection.CONNECTION_PROPERTY_FAN_ENABLED),

      // Support statement cache configuration
      Option.valueOf(
        OracleConnection.CONNECTION_PROPERTY_IMPLICIT_STATEMENT_CACHE_SIZE)

    );

  /**
   * Java object types that are supported by
   * {@link OraclePreparedStatement#setObject(int, Object)} in 21c.
   */
  private static final Set<Class<?>> SUPPORTED_BIND_TYPES = Set.of(
    // The following types are listed in Table B-4 of the JDBC 4.3
    // Specification as types supported by setObject
    String.class,
    Boolean.class,
    Byte.class,
    Short.class,
    Integer.class,
    Long.class,
    Float.class,
    Double.class,
    java.math.BigDecimal.class,
    byte[].class,
    java.math.BigInteger.class,
    java.sql.Date.class,
    java.sql.Time.class,
    java.sql.Timestamp.class,
    java.sql.Clob.class,
    java.sql.Blob.class,
    java.sql.Array.class,
    java.sql.Struct.class,
    java.sql.Ref.class,
    java.net.URL.class,
    java.sql.RowId.class,
    java.sql.NClob.class,
    java.sql.SQLXML.class,
    java.util.Calendar.class,
    java.util.Date.class,
    java.time.LocalDate.class,
    java.time.LocalTime.class,
    java.time.LocalDateTime.class,
    java.time.OffsetTime.class,
    java.time.OffsetDateTime.class,

    // The following types are not listed in Table B-4, but are supported by
    // Oracle JDBC
    java.time.Period.class,
    java.time.Duration.class,
    oracle.sql.json.OracleJsonObject.class
  );

  /**
   * Used to construct the {@link #INSTANCE} of this singleton class.
   */
  private OracleReactiveJdbcAdapter() { }

  /**
   * Returns an instance of this adapter.
   * @return An Oracle JDBC adapter
   */
  static OracleReactiveJdbcAdapter getInstance() {
    return INSTANCE;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the ReactiveJdbcAdapter API by returning an instance of
   * {@link OracleDataSource} that implements the Reactive Extensions APIs for
   * creating connections.
   * </p>
   *
   * <h3>Required Standard Options</h3>
   * <p>
   * This implementation requires values to be set for the following options:
   * </p><ul>
   *   <li>{@link ConnectionFactoryOptions#HOST}</li>
   * </ul><p>
   * The values set for these options are used to compose an Oracle JDBC URL as:
   * </p><pre>
   * jdbc:oracle:thin:@HOST
   * </pre><p>
   * This minimal JDBC URL may specify a TNS alias as a the HOST value when
   * Oracle JDBC is configured to read a tnsnames.ora file.
   * </p>
   *
   * <h3>Optional Standard Options</h3>
   * <p>
   * This implementation supports optional values that are set for the
   * following options:
   * </p><ul>
   *   <li>{@link ConnectionFactoryOptions#PORT}</li>
   *   <li>{@link ConnectionFactoryOptions#DATABASE}</li>
   *   <li>{@link ConnectionFactoryOptions#USER}</li>
   *   <li>{@link ConnectionFactoryOptions#PASSWORD}</li>
   *   <li>{@link ConnectionFactoryOptions#CONNECT_TIMEOUT}</li>
   *   <li>{@link ConnectionFactoryOptions#SSL}</li>
   * </ul><p>
   * When PORT and DATABASE are present, an Oracle JDBC URL is composed as:
   * </p><pre>
   * jdbc:oracle:thin:@HOST:PORT/DATABASE
   * </pre><p>
   * Note that {@code DATABASE} is interpreted as the service name of an Oracle
   * Database; It is not interpreted as a system identifier (SID).
   * </p><p>
   * Values set for {@code USER} and {@code PASSWORD} options are used to
   * authenticate with an Oracle Database.
   * </p><p>
   * A value set for {@code CONNECT_TIMEOUT} will be rounded up to the nearest
   * whole second. When a value is set, any connection request that exceeds the
   * specified duration of seconds will automatically be cancelled. The
   * cancellation will result in an {@code onError} signal delivering an
   * {@link io.r2dbc.spi.R2dbcTimeoutException} to a connection {@code
   * Subscriber}.
   * </p><p>
   * A value of {@code true} set for {@code SSL} will configure the Oracle
   * JDBC Driver to connect using the TCPS protocol (ie: SSL/TLS).
   * </p>
   *
   * <h3>Extended Options</h3>
   * <p>
   * This implementation supports extended options in the two lists that
   * follow. These lists are divided between sensitive and non-sensitive
   * options. A sensitive option should be configured using an instance of
   * {@code Option} returned by calling
   * {@link Option#sensitiveValueOf(String)}. For example, where the
   * readPasswordSecurely method returns a String storing a clear text
   * password, a wallet password would be configured as:
   * </p><pre>
   * ConnectionFactoryOptions.builder()
   *   .option(Option.sensitiveValueOf(
   *     OracleConnection.CONNECTION_PROPERTY_WALLET_PASSWORD),
   *     readPasswordSecurely())
   *     ...
   * </pre><p>
   * Although it may be possible to configure sensitive options in the query
   * section of an R2DBC URL, Oracle R2DBC programmers are advised to use a
   * more secure method whenever possible.
   * </p><p>
   * Non-sensitive options may be configured either programmatically by
   * with {@link Option#valueOf(String)}, or by including name=value pairs
   * in the query section of an R2DBC URL. For example, a wallet location
   * could be configured programmatically as:
   * </p><pre>
   * ConnectionFactoryOptions.builder()
   *   .option(Option.valueOf(
   *     OracleConnection.CONNECTION_PROPERTY_WALLET_LOCATION),
   *     "/path/to/my/wallet")
   *     ...
   * </pre><p>
   * Alternatively, the same wallet location could be configured in an R2DBC URL
   * as:
   * </p><pre>
   * r2dbcs:oracle://host.example.com:1522/service.name?oracle.net.wallet_location=/path/to/my/wallet
   * </pre><p>
   * Each of the extended options listed have the name of an Oracle JDBC
   * connection property, and may be configured with any {@code String} value
   * that is accepted for that connection property. These properties are
   * specified in the javadoc of {@link OracleConnection}.
   * </p><h4>Sensitive Properties</h4>
   * <ul>
   *   <li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_WALLET_PASSWORD
   *     oracle.net.wallet_password}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTOREPASSWORD
   *     javax.net.ssl.keyStorePassword}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTOREPASSWORD
   *     javax.net.ssl.trustStorePassword}
   *   </li>
   * </ul>
   * <h4>Non-Sensitive Properties</h4>
   * <ul>
   *   <li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_TNS_ADMIN
   *     oracle.net.tns_admin}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_WALLET_LOCATION
   *     oracle.net.wallet_location}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORE
   *     javax.net.ssl.keyStore}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORETYPE
   *     javax.net.ssl.keyStoreType}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTORE
   *     javax.net.ssl.trustStore}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTORETYPE
   *     javax.net.ssl.trustStoreType}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_SERVICES
   *     oracle.net.authentication_services}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_THIN_SSL_CERTIFICATE_ALIAS
   *     oracle.net.ssl_certificate_alias}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_THIN_SSL_SERVER_DN_MATCH
   *     oracle.net.ssl_server_dn_match}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_THIN_SSL_SERVER_CERT_DN
   *     oracle.net.ssl_server_cert_dn}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_THIN_SSL_VERSION
   *     oracle.net.ssl_version}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_THIN_SSL_CIPHER_SUITES
   *     oracle.net.ssl_cipher_suites}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_THIN_SSL_KEYMANAGERFACTORY_ALGORITHM
   *     ssl.keyManagerFactory.algorithm}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_THIN_SSL_TRUSTMANAGERFACTORY_ALGORITHM
   *     ssl.trustManagerFactory.algorithm}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_SSL_CONTEXT_PROTOCOL
   *     oracle.net.ssl_context_protocol}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_FAN_ENABLED
   *     oracle.jdbc.fanEnabled}
   *   </li><li>
   *   {@linkplain OracleConnection#CONNECTION_PROPERTY_IMPLICIT_STATEMENT_CACHE_SIZE
   *     oracle.jdbc.implicitStatementCacheSize}
   *   </li>
   * </ul>
   *
   * @implNote The returned {@code DataSource} is configured to create
   * connections that encode character bind values using the National
   * Character Set of an Oracle Database. In 21c, the National Character Set
   * must be either UTF-8 or UTF-16; This ensures that unicode bind data is
   * properly encoded by Oracle JDBC. If the data source is not configured
   * this way, the Oracle JDBC Driver uses the default character set of the
   * database, which may not support Unicode characters.
   */
  @Override
  public DataSource createDataSource(ConnectionFactoryOptions options) {

    OracleDataSource oracleDataSource =
      getOrHandleSQLException(oracle.jdbc.pool.OracleDataSource::new);

    runOrHandleSQLException(() ->
      oracleDataSource.setURL(composeJdbcUrl(options)));
    configureStandardOptions(oracleDataSource, options);
    configureExtendedOptions(oracleDataSource, options);
    configureJdbcDefaults(oracleDataSource);

    return oracleDataSource;
  }

  /**
   * Composes an Oracle JDBC URL from {@code ConnectionFactoryOptions}, as
   * specified in the javadoc of
   * {@link #createDataSource(ConnectionFactoryOptions)}
   * @param options R2DBC options. Not null.
   * @return An Oracle JDBC URL composed from R2DBC options
   */
  private static String composeJdbcUrl(ConnectionFactoryOptions options) {
    String host = options.getRequiredValue(ConnectionFactoryOptions.HOST);
    Integer port = options.getValue(ConnectionFactoryOptions.PORT);
    String serviceName = options.getValue(ConnectionFactoryOptions.DATABASE);
    Boolean isTcps = parseOptionValue(
      ConnectionFactoryOptions.SSL, options, Boolean.class, Boolean::valueOf);

    return String.format("jdbc:oracle:thin:@%s%s%s%s",
      Boolean.TRUE.equals(isTcps) ? "tcps:" : "",
      host,
      port != null ? (":" + port) : "",
      serviceName != null ? ("/" + serviceName) : "");
  }

  /**
   * Configures an {@code oracleDataSource} with the values of standard R2DBC
   * {@code Options}. Standard options are those declared by
   * {@link ConnectionFactoryOptions}. The values of these options are used
   * to configure the {@code oracleDataSource} as specified in the javadoc of
   * {@link #createDataSource(ConnectionFactoryOptions)}
   * @param oracleDataSource An data source to configure
   * @param options R2DBC options. Not null.
   */
  private static void configureStandardOptions(
    OracleDataSource oracleDataSource, ConnectionFactoryOptions options) {

    String user = options.getValue(ConnectionFactoryOptions.USER);
    if (user != null)
      runOrHandleSQLException(() -> oracleDataSource.setUser(user));

    CharSequence password = options.getValue(ConnectionFactoryOptions.PASSWORD);
    if (password != null) {
      runOrHandleSQLException(() ->
        oracleDataSource.setPassword(password.toString()));
    }

    Duration timeout = parseOptionValue(
      ConnectionFactoryOptions.CONNECT_TIMEOUT, options, Duration.class,
      Duration::parse);
    if (timeout != null) {
      runOrHandleSQLException(() ->
        oracleDataSource.setLoginTimeout(
          Math.toIntExact(timeout.getSeconds())
            // Round up to nearest whole second
            + timeout.getNano() == 0 ? 0 : 1));
    }

  }

  /**
   * Configures an {@code oracleDataSource} with the values of extended R2DBC
   * {@code Options}. Extended options are those declared as
   * {@link #SUPPORTED_CONNECTION_PROPERTY_OPTIONS} in this class. The values
   * of these options are used to configure the {@code oracleDataSource} as
   * specified in the javadoc of
   * {@link #createDataSource(ConnectionFactoryOptions)}
   * @param oracleDataSource An data source to configure
   * @param options R2DBC options. Not null.
   */
  private static void configureExtendedOptions(
    OracleDataSource oracleDataSource, ConnectionFactoryOptions options) {

    // Handle the short form of the TNS_ADMIN option
    String tnsAdmin = options.getValue(Option.valueOf("TNS_ADMIN"));
    if (tnsAdmin != null) {
      // Configure using the long form: oracle.net.tns_admin
      runOrHandleSQLException(() ->
        oracleDataSource.setConnectionProperty(
          OracleConnection.CONNECTION_PROPERTY_TNS_ADMIN, tnsAdmin));
    }

    // Apply any extended options as connection properties
    for (Option<CharSequence> option : SUPPORTED_CONNECTION_PROPERTY_OPTIONS) {
      // Using Object as the value type allows options to be set as types like
      // Boolean or Integer. These types make sense for numeric or boolean
      // connection property values, such as statement cache size, or enable x.
      Object value = options.getValue(option);
      if (value != null) {
        runOrHandleSQLException(() ->
          oracleDataSource.setConnectionProperty(
            option.name(), value.toString()));
      }
    }
  }

  /**
   * <p>
   * Parses the value of an {@code option} to return an instance of it's
   * {@code type}. This method returns the value if it is already an instance
   * of {@code type}, or if it is {@code null}. If the value is an instance
   * of {@code String}, then this method returns the output of a {@code parser}
   * function when the {@code String} value is applied as input.
   * </p><p>
   * This method is used for {@link Option} values that may be specified in the
   * query section of an R2DBC URL. When a value is parsed from a URL query,
   * {@link io.r2dbc.spi.ConnectionFactoryOptions} will need to store that
   * value as a {@link String}, even if the {@code Option} is not declared
   * with the generic type of {@code String}.
   * </p>
   * @param option An option to parse the value of. Not null.
   * @param options Values of options
   * @param type Value type of an {@code option}. Not null.
   * @param parser Parses an option value if it is an instance of {@code String}
   * @param <T> Value type of an {@code option}. Not null.
   * @return The value of the {@code option}. May be null.
   * @throws IllegalArgumentException If the value of {@code option} is not an
   * instance of {@code T}, {@code String}, or {@code null}
   * @throws IllegalArgumentException If the {@code parser} throws an
   * exception.
   */
  private static <T> T parseOptionValue(
    Option<T> option, ConnectionFactoryOptions options, Class<T> type,
    Function<String, T> parser) {
    T value = options.getValue(option);

    if (value == null) {
      return null;
    }
    else if (type.isInstance(value)) {
      return value;
    }
    else if (value instanceof String) {
      try {
        return parser.apply((String) value);
      }
      catch (Throwable parseFailure) {
        throw new IllegalArgumentException(
          "Failed to parse the value of Option: " + option.name(),
          parseFailure);
      }
    }
    else {
      throw new IllegalArgumentException(String.format(
        "Value of Option %s has an unexpected type: %s. Expected Type is: %s.",
        option.name(), value.getClass(), type));
    }
  }

  /**
   * Configures an {@code oracleDataSource} with any connection properties that
   * this adapter requires by default.
   * @param oracleDataSource An data source to configure
   */
  private static void configureJdbcDefaults(OracleDataSource oracleDataSource) {

    // Have the Oracle JDBC Driver implement behavior that the JDBC
    // Specification defines as correct. The javadoc for this property lists
    // all of it's effects. One effect is to have ResultSetMetaData describe
    // FLOAT columns as the FLOAT type, rather than the NUMBER type. This
    // effect allows the Oracle R2DBC Driver obtain correct metadata for
    // FLOAT type columns. The property is deprecated, but the deprecation note
    // explains that setting this to "false" is deprecated, and that it
    // should be set to true; If not set, the 21c driver uses a default value
    // of false.
    @SuppressWarnings("deprecation")
    String enableJdbcSpecCompliance =
      OracleConnection.CONNECTION_PROPERTY_J2EE13_COMPLIANT;
    runOrHandleSQLException(() ->
      oracleDataSource.setConnectionProperty(enableJdbcSpecCompliance, "true"));

    // Have the Oracle JDBC Driver cache PreparedStatements by default.
    runOrHandleSQLException(() -> {
      // Don't override a value set by user code
      String userValue = oracleDataSource.getConnectionProperty(
        OracleConnection.CONNECTION_PROPERTY_IMPLICIT_STATEMENT_CACHE_SIZE);

      if (userValue == null) {
        // The default value of the OPEN_CURSORS parameter in the 21c
        // and 19c databases is 50:
        // https://docs.oracle.com/en/database/oracle/oracle-database/21/refrn/OPEN_CURSORS.html#GUID-FAFD1247-06E5-4E64-917F-AEBD4703CF40
        // Assuming this default, then a default cache size of 25 will keep
        // each session at or below 50% of it's cursor capacity, which seems
        // reasonable.
        oracleDataSource.setConnectionProperty(
          OracleConnection.CONNECTION_PROPERTY_IMPLICIT_STATEMENT_CACHE_SIZE,
          "25");
      }
    });

    // TODO: Disable the result set cache? This is needed to support the
    //  SERIALIZABLE isolation level, which requires result set caching to be
    //  disabled.
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the ReactiveJdbcAdapter API by opening a connection with the
   * behavior of
   * {@link OracleConnectionBuilder#buildConnectionPublisherOracle()} adapted to
   * conform with the R2DBC standards.
   * </p>
   * @implNote ORA-18714 errors are mapped to a timeout exception. This error
   * code indicates that a login timeout has expired. Oracle JDBC throws that
   * as a SQLRecoverableException, not as a SQLTimeoutException, so
   * {@link OracleR2dbcExceptions} won't map it to the correct R2DBC
   * exception type.
   */
  @Override
  public Publisher<? extends Connection> publishConnection(
    DataSource dataSource) {
    OracleDataSource oracleDataSource = unwrapOracleDataSource(dataSource);
    return Mono.from(adaptFlowPublisher(() ->
        oracleDataSource
          .createConnectionBuilder()
          .buildConnectionPublisherOracle()))
      .onErrorMap(R2dbcException.class, error ->
        error.getErrorCode() == 18714 // ORA-18714 : Login timeout expired
          ? new R2dbcTimeoutException(error.getMessage(),
              error.getSqlState(), error.getErrorCode(), error.getCause())
          : error);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the ReactiveJdbcAdapter API by executing SQL with the
   * behavior {@link OraclePreparedStatement#executeAsyncOracle()} adapted to
   * conform with the R2DBC standards.
   * </p>
   */
  @Override
  public Publisher<Boolean> publishSQLExecution(
    PreparedStatement sqlStatement) {
    OraclePreparedStatement oraclePreparedStatement =
        unwrapOraclePreparedStatement(sqlStatement);
    return adaptFlowPublisher(oraclePreparedStatement::executeAsyncOracle);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the ReactiveJdbcAdapter API by executing SQL DML with the
   * behavior of {@link OraclePreparedStatement#executeBatchAsyncOracle()}
   * adapted to conform with the R2DBC standards.
   * </p>
   */
  @Override
  public Publisher<Long> publishBatchUpdate(
    PreparedStatement batchUpdateStatement) {
    OraclePreparedStatement oraclePreparedStatement =
      unwrapOraclePreparedStatement(batchUpdateStatement);
    return adaptFlowPublisher(oraclePreparedStatement::executeBatchAsyncOracle);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the ReactiveJdbcAdapter API by fetching row data with the
   * behavior of {@link OracleResultSet#publisherOracle(Function)} adapted to
   * conform with the R2DBC standards.
   * </p>
   */
  @Override
  public <T> Publisher<T> publishRows(
    ResultSet resultSet, Function<JdbcRow, T> rowMappingFunction) {
    OracleResultSet oracleResultSet = unwrapOracleResultSet(resultSet);

    return adaptFlowPublisher(() ->
      oracleResultSet.publisherOracle(oracleRow ->
        rowMappingFunction.apply(new OracleJdbcRow(oracleRow))));
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the ReactiveJdbcAdapter API by committing a transaction with
   * the behavior of {@link OracleConnection#commitAsyncOracle()} adapted to
   * conform with the R2DBC standards. The {@code commitAsyncOracle} API is
   * adapted with a publisher that only emits {@code onComplete} if
   * auto-commit is enabled. The {@code commitAsyncOracle} API is specified
   * to throw {@code SQLException} when auto-commit is enabled, where as this
   * adapter API is specified emit {@code onComplete}.
   * </p>
   */
  @Override
  public Publisher<Void> publishCommit(Connection connection) {
    OracleConnection oracleConnection = unwrapOracleConnection(connection);
    return adaptFlowPublisher(() -> {
      if (oracleConnection.getAutoCommit())
        return toFlowPublisher(Mono.empty());
      else
        return oracleConnection.commitAsyncOracle();
    });
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the ReactiveJdbcAdapter API by rolling back a transaction
   * with the behavior of {@link OracleConnection#rollbackAsyncOracle()}
   * adapted to conform with the R2DBC standards. The {@code rollbackAsyncOracle}
   * API is adapted with a publisher that only emits {@code onComplete} if
   * auto-commit is enabled. The {@code rollbackAsyncOracle} API is specified
   * to throw {@code SQLException} when auto-commit is enabled, where as this
   * adapter API is specified emit {@code onComplete}.
   * </p>
   */
  @Override
  public Publisher<Void> publishRollback(Connection connection) {
    OracleConnection oracleConnection = unwrapOracleConnection(connection);
    return adaptFlowPublisher(() -> {
        if (oracleConnection.getAutoCommit())
          return toFlowPublisher(Mono.empty());
        else
          return oracleConnection.rollbackAsyncOracle();
      });
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the ReactiveJdbcAdapter API by closing a connection with the
   * behavior of {@link OracleConnection#closeAsyncOracle()} adapted to conform
   * with the R2DBC standards.
   * </p>
   */
  @Override
  public Publisher<Void> publishClose(Connection connection) {
    return adaptFlowPublisher(
      unwrapOracleConnection(connection)::closeAsyncOracle);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the ReactiveJdbcAdapter API by publishing the content of a
   * BLOB, with the behavior of {@link OracleBlob#publisherOracle(long)}
   * adapted to conform with the R2DBC standards.
   * </p>
   */
  public Publisher<ByteBuffer> publishBlobRead(Blob blob)
    throws R2dbcException {
    OracleBlob oracleBlob = castAsType(blob, OracleBlob.class);
    return Flux.from(adaptFlowPublisher(() -> oracleBlob.publisherOracle(1L)))
      .map(ByteBuffer::wrap);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the ReactiveJdbcAdapter API by publishing the content of a
   * CLOB, with the behavior of {@link OracleClob#publisherOracle(long)}
   * adapted to conform with the R2DBC standards.
   * </p>
   */
  public Publisher<String> publishClobRead(Clob clob)
    throws R2dbcException {
    OracleClob oracleClob = castAsType(clob, OracleClob.class);
    return adaptFlowPublisher(() -> oracleClob.publisherOracle(1L));
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the ReactiveJdbcAdapter API by publishing the result of writing
   * BLOB content with the behavior of
   * {@link OracleBlob#subscriberOracle(long, Flow.Subscriber)} adapted to
   * conform with the R2DBC standards.
   * </p>
   * @implNote The {@code OracleBlob} subscriber retains published byte arrays
   * after a call to {@code onNext} returns, until a {@code request} signal
   * follows. This implementation assumes that the {@code contentPublisher}
   * also retains any {@code ByteBuffer} emitted to {@code onNext}, so the
   * contents are always copied into a new byte array. In a later release,
   * avoiding the copy using {@link ByteBuffer#array()} can be worth
   * considering.
   *
   * @implNote The 21c {@code OracleBlob} subscriber violates Rule 2.7 of the
   * Reactive Streams Specification, which prohibits concurrent calls to
   * {@link Subscription#request(long)}. This can cause undefined behavior by
   * the {@code contentPublisher}. To work around this bug, this method
   * proxies the {@link Subscription} between the {@code contentPublisher}
   * and the {@code OracleBlob} subscriber. The proxy ensures that
   * {@code request} signals are delivered serially.
   */
  @Override
  public Publisher<Void> publishBlobWrite(
    Publisher<ByteBuffer> contentPublisher, Blob blob) {
    OracleBlob oracleBlob = castAsType(blob, OracleBlob.class);

    // This processor emits a terminal signal when all blob writing database
    // calls have completed
    DirectProcessor<Long> writeOutcomeProcessor = DirectProcessor.create();
    Flow.Subscriber<byte[]> blobSubscriber = getOrHandleSQLException(() ->
      oracleBlob.subscriberOracle(1L,
        toFlowSubscriber(writeOutcomeProcessor)));

    return adaptFlowPublisher(() -> {
      Flux.from(contentPublisher)
        .map(byteBuffer -> {
          // Don't mutate position/limit/mark
          ByteBuffer slice = byteBuffer.slice();
          byte[] byteArray = new byte[slice.remaining()];
          slice.get(byteArray);
          return byteArray;
        })
        .subscribe(new SerializedLobSubscriber<>(blobSubscriber));

      return toFlowPublisher(writeOutcomeProcessor.then());
    });
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the ReactiveJdbcAdapter API by publishing the result of writing
   * CLOB content with the behavior of
   * {@link OracleClob#subscriberOracle(long, Flow.Subscriber)} adapted to
   * conform with the R2DBC standards.
   * </p>
   *
   * @implNote The 21c {@code OracleClob} subscriber violates Rule 2.7 of the
   * Reactive Streams Specification, which prohibits concurrent calls to
   * {@link Subscription#request(long)}. This can cause undefined behavior by
   * the {@code contentPublisher}. To work around this bug, this method
   * proxies the {@link Subscription} between the {@code contentPublisher}
   * and the {@code OracleClob} subscriber. The proxy ensures that
   * {@code request} signals are delivered serially.
   */
  @Override
  public Publisher<Void> publishClobWrite(
    Publisher<? extends CharSequence> contentPublisher, Clob clob) {
    OracleClob oracleClob = castAsType(clob, OracleClob.class);

    // This processor emits a terminal signal when all clob writing database
    // calls have completed
    DirectProcessor<Long> writeOutcomeProcessor = DirectProcessor.create();
    Flow.Subscriber<String> clobSubscriber = getOrHandleSQLException(() ->
      oracleClob.subscriberOracle(1L,
        toFlowSubscriber(writeOutcomeProcessor)));

    return adaptFlowPublisher(() -> {
      Flux.from(contentPublisher)
        .map(CharSequence::toString)
        .subscribe(new SerializedLobSubscriber<>(clobSubscriber));

      return toFlowPublisher(writeOutcomeProcessor.then());
    });
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the ReactiveJdbcAdapter API by publishing the result of
   * releasing the resources of a BLOB, with the behavior of
   * {@link OracleBlob#freeAsyncOracle()} adapted to conform with R2DBC
   * standards.
   * </p>
   */
  @Override
  public Publisher<Void> publishBlobFree(Blob blob) throws R2dbcException {
    OracleBlob oracleBlob = castAsType(blob, OracleBlob.class);
    return adaptFlowPublisher(oracleBlob::freeAsyncOracle);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the ReactiveJdbcAdapter API by publishing the result of
   * releasing the resources of a CLOB, with the behavior of
   * {@link OracleClob#freeAsyncOracle()} adapted to conform with R2DBC
   * standards.
   * </p>
   */
  @Override
  public Publisher<Void> publishClobFree(Clob clob) throws R2dbcException {
    OracleClob oracleClob = castAsType(clob, OracleClob.class);
    return adaptFlowPublisher(oracleClob::freeAsyncOracle);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the ReactiveJdbcAdapter API by returning {@code true} if a
   * hardcoded set of classes that contains {@code javaType}, or a super type
   * of {@code javaType}.
   * </p>
   * @param javaType Type passed to setObject
   * @return {@code true} if supported, otherwise {@code false}
   */
  public boolean isSupportedBindType(Class<?> javaType) {
    return SUPPORTED_BIND_TYPES.contains(javaType)
      || SUPPORTED_BIND_TYPES.stream().anyMatch(
           supportedType -> supportedType.isAssignableFrom(javaType));
  }

  /**
   * {@inheritDoc}
   * <p>
   * <em>
   * This method executes a blocking database call when generated column
   * names are specified by a non-empty {@code generatedColumns} argument.
   * </em>
   * This is a known limitation stemming from the implementation of
   * {@link Connection#prepareStatement(String, String[])} in the 21.1 Oracle
   * JDBC Driver. This limitation will be resolved in a later release;
   * The Oracle JDBC Team is aware of the issue and is working on a fix.
   * </p>
   *
   * <p>
   * Implements the ReactiveJdbcAdapter API by publishing a statement
   * returned by either {@link OracleConnection#prepareStatement(String)} or
   * {@link OracleConnection#prepareStatement(String, int)}, or
   * {@link OracleConnection#prepareStatement(String, String[])}.
   * </p>
   */
  @Override
  public Publisher<PreparedStatement> publishPreparedStatement(
    String sql, String[] generatedColumns, Connection connection) {
    OracleConnection oracleConnection = unwrapOracleConnection(connection);
    try {
      final PreparedStatement preparedStatement;
      if (generatedColumns == null) {
        preparedStatement = oracleConnection.prepareCall(sql);
        //TODO preparedStatement = oracleConnection.prepareStatement(sql);
      }
      else if (generatedColumns.length == 0) {
        preparedStatement =
          oracleConnection.prepareStatement(sql, RETURN_GENERATED_KEYS);
      }
      else {
        // TODO: This executes a BLOCKING DATABASE CALL
        preparedStatement =
          oracleConnection.prepareStatement(sql, generatedColumns);
      }
      return Mono.just(preparedStatement);
    }
    catch (SQLException sqlException) {
      return Mono.error(toR2dbcException(sqlException));
    }
  }

  @Override
  public Publisher<CallableStatement> publishCallableStatement(
    String sql, Connection connection) {
    OracleConnection oracleConnection = unwrapOracleConnection(connection);
    return Mono.just(getOrHandleSQLException(() ->
      oracleConnection.prepareCall(sql)));
  }

  @Override
  public JdbcRow createOutParameterRow(CallableStatement callableStatement) {
    return new OutParameterRow(callableStatement);
  }

  /**
   * <p>
   * Returns a publisher that adapts the behavior of a Reactive Extensions
   * publisher to conform with the R2DBC standards. Subscribers of the returned
   * publisher are subscribed to the Reactive Streams publisher created by
   * {@code publisherSupplier}. There will be, at most, one invocation of {@code
   * publisherSupplier} used to create a single instance of a Reactive Extensions
   * publisher. All subscribers of the returned publisher are subscribed to
   * the single publisher instance created by {@code publisherSupplier}.
   * </p><p>
   * The returned publisher adapts the behavior implemented by the Reactive
   * Extensions publisher as follows:
   * <ul>
   *   <li>
   *    The supplied {@code java.util.concurrent.Flow.Publisher} is adapted to
   *    implement {@code org.reactivestreams.Publisher}.
   *   </li>
   *   <li>
   *     SQLExceptions emitted by the supplied publisher are converted into
   *     R2dbcExceptions.
   *   </li>
   *   <li>
   *     The publisher's creation is deferred. Publishers created by the
   *     Oracle JDBC Driver generally initiate execution when they are created,
   *     before a subscriber subscribes. The returned publisher defers
   *     execution which happens during publisher creation by invoking the
   *     specified {@code publisherSupplier} <i>after</i> a subscriber has
   *     subscribed.
   *   </li>
   * </ul>
   * </p>
   * @param publisherSupplier Invoked to supply a publisher when a subscriber
   *   subscribes.
   * @param <T> The type of item emitted by the publisher.
   * @return A publisher which adapts the supplied publisher.
   */
  private <T> Publisher<T> adaptFlowPublisher(
    ThrowingSupplier<Flow.Publisher<? extends T>> publisherSupplier) {
    return Flux.from(deferOnce(publisherSupplier))
      .onErrorMap(SQLException.class, OracleR2dbcExceptions::toR2dbcException);
  }

  /**
   * <p>
   * Returns a publisher that defers the creation of a single publisher that
   * is output from a {@code publisherSupplier}. The returned publisher will
   * invoke the {@code getOrThrow()} method of the {@code publisherSupplier} the
   * first time a subscriber subscribes.
   * </p><p>
   * The purpose of this method is to defer the creation of publishers returned
   * by Oracle JDBC's Reactive Extensions APIs that initiate execution
   * when the publisher is created. To meet the R2DBC goal of deferred
   * execution, this method is used to defer the publisher's creation.
   * </p><p>
   * Deferred publisher factory methods such as {@link Flux#defer(Supplier)}
   * invoke the publisher supplier for each subscriber. This factory method
   * does not invoke the supplier for each subscriber. This factory invokes
   * the supplier only when a subscriber subscribes for the first time. The
   * first subscriber and all subsequent subscribers are then subscribed to the
   * same publisher.
   * </p><p>
   * This implementation ensures that a deferred execution is not re-executed
   * for each subscriber. For instance,
   * {@link OraclePreparedStatement#executeAsyncOracle()} executes the
   * statement each time it is called. If {@link Flux#defer(Supplier)} is
   * called with a reference to this method, it would return a publisher that
   * executes the statement each time a subscriber subscribes. By only invoking
   * the supplier a single time, the publisher returned by this method
   * ensures that the statement is only executed one time, and that the result
   * of that single execution is emitted to each subscriber.
   * </p>
   * @param publisherSupplier Supplies a publisher, or throws an exception. A
   *                          thrown exception is emitted as an {@code onError}
   *                          signal to subscribers.
   * @param <T> The type emitted by the returned publisher
   * @return A publisher that defers creation of a supplied publisher until a
   * subscriber subscribes.
   */
  private static <T> Publisher<T> deferOnce(
    ThrowingSupplier<Flow.Publisher<? extends T>> publisherSupplier) {

    AtomicBoolean isSubscribed = new AtomicBoolean(false);
    CompletableFuture<Publisher<T>> publisherFuture = new CompletableFuture<>();

    return subscriber -> {
      Objects.requireNonNull(subscriber, "Subscriber is null");

      if (isSubscribed.compareAndSet(false, true)) {
        Publisher<T> publisher;
        try {
          publisher = toPublisher(getOrHandleSQLException(publisherSupplier));
        }
        catch (R2dbcException r2dbcException) {
          publisher = Mono.error(r2dbcException);
        }

        publisher.subscribe(subscriber);
        publisherFuture.complete(publisher);
      }
      else {
        publisherFuture.thenAccept(publisher ->
          publisher.subscribe(subscriber));
      }
    };
  }

  /**
   * Returns a {@code DataSource}
   * {@linkplain Wrapper#unwrap(Class) unwrapped} as an
   * {@code OracleDataSource}, or throws an {@code R2dbcException} if it does
   * not wrap or implement the Oracle JDBC interface.
   * @param dataSource A JDBC data source
   * @return An Oracle JDBC data source
   * @throws R2dbcException If an Oracle JDBC data source is not wrapped.
   */
  private OracleDataSource unwrapOracleDataSource(DataSource dataSource) {
    return getOrHandleSQLException(() ->
      dataSource.unwrap(OracleDataSource.class));
  }

  /**
   * Returns a {@code Connection}
   * {@linkplain Wrapper#unwrap(Class) unwrapped} as an
   * {@code OracleConnection}, or throws an {@code R2dbcException} if it does
   * not wrap or implement the Oracle JDBC interface.
   * @param connection A JDBC connection
   * @return An Oracle JDBC connection
   * @throws R2dbcException If an Oracle JDBC connection is not wrapped.
   */
  private OracleConnection unwrapOracleConnection(Connection connection) {
    return getOrHandleSQLException(() ->
      connection.unwrap(OracleConnection.class));
  }

  /**
   * Returns a {@code PreparedStatement}
   * {@linkplain Wrapper#unwrap(Class) unwrapped} as an
   * {@code OraclePreparedStatement}, or throws an {@code R2dbcException} if it
   * does not wrap or implement the Oracle JDBC interface.
   * @param preparedStatement A JDBC prepared statement
   * @return An Oracle JDBC prepared statement
   * @throws R2dbcException If an Oracle JDBC prepared statement is not wrapped.
   */
  private OraclePreparedStatement unwrapOraclePreparedStatement(
    PreparedStatement preparedStatement) {
    return getOrHandleSQLException(() ->
      preparedStatement.unwrap(OraclePreparedStatement.class));
  }

  /**
   * Returns a {@code ResultSet}
   * {@linkplain Wrapper#unwrap(Class) unwrapped} as an
   * {@code OracleResultSet}, or throws an {@code R2dbcException} if it does
   * not wrap or implement the Oracle JDBC interface.
   * @param resultSet A JDBC result set
   * @return An Oracle JDBC result set
   * @throws R2dbcException If an Oracle JDBC result set is not wrapped.
   */
  private OracleResultSet unwrapOracleResultSet(ResultSet resultSet) {
    return getOrHandleSQLException(() ->
      resultSet.unwrap(OracleResultSet.class));
  }

  /**
   * <p>
   * Returns an {@code object} cast as a specified {@code type}, or
   * throws an {@code R2dbcException} if it is not an instance of the type.
   * </p><p>
   * The adapter uses this method to cast standard JDBC typed parameters to
   * Oracle JDBC types, when the parameter type <i>is not</i> a
   * {@link java.sql.Wrapper}. The adapter should use
   * {@link Wrapper#unwrap(Class)} whenever it is possible to do so.
   * </p>
   * @param object An object to cast
   * @param type A type to cast as
   * @return The cast object
   * @throws R2dbcException If {@code object} is not an instance of {@code type}
   */
  private <T> T castAsType(Object object, Class<T> type) {
    if (type.isInstance(object)) {
      return type.cast(object);
    }
    else {
      throw OracleR2dbcExceptions.newNonTransientException(
        object.getClass() + " is not an instance of " + type, null);
    }
  }

  /**
   * Returns {@code true} if an {@code errorCode} indicates a failure to
   * convert a SQL type value into a Java type.
   * @param errorCode Error code of a {@code SQLException}
   * @return {@code true} if {@code errorCode} is a type conversion failure,
   * otherwise returns {@code false}
   */
  private static boolean isTypeConversionError(int errorCode) {
    // ORA-17004 is raised for an unsupported type conversion
    return errorCode == 17004;
  }

  private static final class OutParameterRow implements JdbcRow {

    /** CallableStatement that has been executed to return out parameters */
    final CallableStatement callableStatement;

    /**
     * Set to {@code false} after a mapping function has output a value for
     * this row as input, and it is no longer valid to access this row.
     */
    boolean isValid = true;

    private OutParameterRow(CallableStatement callableStatement) {
      this.callableStatement = callableStatement;
    }

    @Override
    public <T> T getObject(int index, Class<T> type) {
      if (! isValid) {
        throw new IllegalStateException(
          "A Row is not valid outside of a mapping function");
      }
      else {
        try {
          return callableStatement.getObject(1, type);
        }
        catch (SQLException sqlException) {
          if (isTypeConversionError(sqlException.getErrorCode()))
            throw new IllegalArgumentException(sqlException);
          else
            throw toR2dbcException(sqlException);
        }
      }
    }

    @Override
    public JdbcRow copy() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * A {@code JdbcRow} that delegates to an {@link OracleRow}. An instance of
   * this class adapts the behavior of {@code OracleRow} to conform with
   * R2DBC standards.
   */
  private static final class OracleJdbcRow implements JdbcRow {

    /** OracleRow wrapped by this JdbcRow */
    private final OracleRow oracleRow;

    /** Constructs a new row that delegates to {@code oracleRow} */
    private OracleJdbcRow(OracleRow oracleRow) {
      this.oracleRow = oracleRow;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implements the {@code JdbcRow} API by delegating to
     * {@link OracleRow#getObject(int, Class)} and throwing
     * {@link SQLException}s as {@link Throwable}s that conform with R2DBC
     * standards.
     * </p>
     */
    @Override
    public <U> U getObject(int index, Class<U> type) {
      try {
        return oracleRow.getObject(index + 1, type);
      }
      catch (SQLException sqlException) {
        // ORA-18711 is raised when outside of a row mapping function
        if (sqlException.getErrorCode() == 18711)
          throw new IllegalStateException(sqlException);
        else if (isTypeConversionError(sqlException.getErrorCode()))
          throw new IllegalArgumentException(sqlException);
        else
          throw toR2dbcException(sqlException);
      }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implements the {@code JdbcRow} API by delegating to
     * {@link OracleRow#clone()}.
     * </p>
     * @implNote This implementation will fail if the {@code OracleRow} is
     * emitted by a {@link OracleResultSet} that does not support
     * {@link java.sql.ResultSetMetaData}, such as one returned by
     * {@link OraclePreparedStatement#getReturnResultSet()}.
     */
    @Override
    public JdbcRow copy() {
      return new OracleJdbcRow(oracleRow.clone());
    }
  }

  /**
   * <p>
   * A {@code Subscriber} that serializes {@code Subscription} method calls
   * made by {@link OracleBlob} or {@link OracleClob} subscribers. The purpose
   * of this class is to work around Oracle JDBC Bug #32097526, in which the
   * Large Object (LOB) subscribers violate Rule 2.7 of the Reactive Streams
   * 1.0.3 Specification by invoking subscription methods concurrently. This
   * violation can lead to unspecified behavior from the upstream LOB content
   * {@code Publisher}.
   * </p><p>
   * This class serves as an intermediary between a LOB content publisher
   * upstream, and the LOB subscriber downstream. It presents itself as a
   * subscription to the LOB subscriber so that it can regulate it's
   * subscription method calls. Each subscription call is regulated by
   * acquiring a mutually exclusive lock before the call is forwarded to the
   * content publisher's subscription.
   * </p>
   *
   * @implNote This class is an {@code org.reactivestreams.Subscriber} and a
   * {@code java.util.concurrent.Flow.Subscription}. These APIs were chosen to
   * interface with R2DBC Blob/Clob publishers upstream, and with Reactive
   * Extensions downstream.
   * @param <T> The type of item subscribed to
   */
  private static class SerializedLobSubscriber<T>
    implements org.reactivestreams.Subscriber<T>, Flow.Subscription {

    /** The downstream OracleBlob/OracleClob subscriber */
    final Flow.Subscriber<T> lobSubscriber;

    /** Guards access to the upstream content publisher's subscription */
    final ReentrantLock signalLock = new ReentrantLock();

    /** The upstream content publisher's subscription */
    Subscription contentSubscription;

    /**
     * Constructs a new subscriber that regulates subscription calls from a
     * {@code lobSubscriber}. The {@code onSubscribe} method of the {@code
     * lobSubscriber} is invoked when the {@code onSubscribe} method of the
     * constructed subscriber is invoked.
     */
    SerializedLobSubscriber(Flow.Subscriber<T> lobSubscriber) {
      this.lobSubscriber = lobSubscriber;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Retains the {@code subscription} and presents itself as a subscription
     * to the LOB subscriber. Subscription calls from the LOB subscriber are
     * then serially forwarded to the {@code subscription}.
     * </p>
     */
    @Override
    public void onSubscribe(Subscription subscription) {
      contentSubscription = subscription;
      lobSubscriber.onSubscribe(this);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Regulates a request call from the {@code lobSubscriber} by first
     * blocking until any active {@code request} or {@code cancel} call has
     * completed, and then forwarding the request to the content publisher.
     * </p>
     */
    @Override
    public void request(long n) {
      signalLock.lock();
      try {
        contentSubscription.request(n);
      }
      finally {
        signalLock.unlock();
      }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Regulates a cancel call from the {@code lobSubscriber} by first
     * blocking until any active {@code request} or {@code cancel} call has
     * completed, and then forwarding the cancel to the content publisher.
     * </p>
     */
    @Override
    public void cancel() {
      signalLock.lock();
      try {
        contentSubscription.cancel();
      }
      finally {
        signalLock.unlock();
      }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the signal to the LOB subscriber without any regulation.
     * </p>
     */
    @Override
    public void onNext(T item) {
      lobSubscriber.onNext(item);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the signal to the LOB subscriber without any regulation.
     * </p>
     */
    @Override
    public void onError(Throwable throwable) {
      lobSubscriber.onError(throwable);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the signal to the LOB subscriber without any regulation.
     * </p>
     */
    @Override
    public void onComplete() {
      lobSubscriber.onComplete();
    }
  }
}
