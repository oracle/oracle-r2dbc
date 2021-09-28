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
import oracle.r2dbc.OracleR2dbcOptions;
import oracle.r2dbc.impl.OracleR2dbcExceptions.ThrowingSupplier;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.r2dbc.spi.ConnectionFactoryOptions.CONNECT_TIMEOUT;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.SSL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.fromJdbc;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.runJdbc;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.toR2dbcException;
import static org.reactivestreams.FlowAdapters.toFlowPublisher;
import static org.reactivestreams.FlowAdapters.toFlowSubscriber;
import static org.reactivestreams.FlowAdapters.toPublisher;

/**
 * <p>
 * Implementation of {@link ReactiveJdbcAdapter} for the Oracle JDBC Driver.
 * This adapter is compatible with the 21.3 release of Oracle JDBC. The
 * implementation adapts the behavior of the Reactive Extensions APIs of Oracle
 * JDBC to conform with the R2DBC standards. Typically, the adaption of Reactive
 * Extensions for R2DBC conformance requires the following:
 * </p><ul>
 *   <li>
 *     Interfacing with {@code org.reactivestreams}: All Reactive Extensions
 *     APIs interface with the {@link Flow} equivalents of the
 *     {@code org.reactivestreams} types.
 *   </li>
 *   <li>
 *     Deferred Execution: Most Reactive Extension APIs do not defer
 *     execution.
 *   </li>
 *   <li>
 *     Type Conversion: Most Reactive Extensions APIs do not emit or consume
 *     the same types as R2DBC SPIs.
 *   </li>
 *   <li>
 *     Thread Safety: An instance of this adapter guards access to a JDBC
 *     Connection without blocking a thread. Oracle JDBC implements thread
 *     safety by blocking threads, and this can cause deadlocks in common
 *     R2DBC programming scenarios. See the JavaDoc of
 *     {@link UsingConnectionSubscriber} for more details.
 *   </li>
 * </ul><p>
 * A instance of this class is obtained by invoking {@link #getInstance()}. A
 * new instance should be created each time a JDBC {@code Connection} is
 * created, and that instance should be used to execute database calls with
 * that {@code Connection} only.
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
    JDBC_CONNECTION_PROPERTY_OPTIONS = Set.of(

      // Support TNS_ADMIN (tnsnames.ora, ojdbc.properties).
      OracleR2dbcOptions.TNS_ADMIN,

      // Support wallet properties for TCPS/SSL/TLS
      OracleR2dbcOptions.TLS_WALLET_LOCATION,
      OracleR2dbcOptions.TLS_WALLET_PASSWORD,

      // Support keystore properties for TCPS/SSL/TLS
      OracleR2dbcOptions.TLS_KEYSTORE,
      OracleR2dbcOptions.TLS_KEYSTORE_TYPE,
      Option.sensitiveValueOf(
        OracleConnection
          .CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTOREPASSWORD),

      // Support truststore properties for TCPS/SSL/TLS
      OracleR2dbcOptions.TLS_TRUSTSTORE,
      OracleR2dbcOptions.TLS_TRUSTSTORE_TYPE,
      OracleR2dbcOptions.TLS_TRUSTSTORE_PASSWORD,

      // Support authentication services (RADIUS, KERBEROS, and TCPS)
      OracleR2dbcOptions.AUTHENTICATION_SERVICES,

      // Support fine grained configuration for TCPS/SSL/TLS
      OracleR2dbcOptions.TLS_CERTIFICATE_ALIAS,
      OracleR2dbcOptions.TLS_SERVER_DN_MATCH,
      OracleR2dbcOptions.TLS_SERVER_CERT_DN,
      OracleR2dbcOptions.TLS_VERSION,
      OracleR2dbcOptions.TLS_CIPHER_SUITES,
      OracleR2dbcOptions.TLS_KEYMANAGERFACTORY_ALGORITHM,
      OracleR2dbcOptions.TLS_TRUSTMANAGERFACTORY_ALGORITHM,
      OracleR2dbcOptions.SSL_CONTEXT_PROTOCOL,

      // Because of bug 32378754, the FAN support in the driver may cause a 10s
      // delay to connect. As a workaround the following property can be set
      // to false to disable FAN support in the driver.
      OracleR2dbcOptions.FAN_ENABLED,

      // Support statement cache configuration
      OracleR2dbcOptions.IMPLICIT_STATEMENT_CACHE_SIZE,

      // Support LOB prefetch size configuration. A large size is configured
      // by default to support cases where memory is available to store entire
      // LOB values. A non-default size may be configured when LOB values are
      // too large to be prefetched and must be streamed from Blob/Clob objects.
      OracleR2dbcOptions.DEFAULT_LOB_PREFETCH_SIZE,

      // Allow out-of-band (OOB) breaks to be disabled. Oracle JDBC uses OOB
      // breaks to interrupt a SQL call after a timeout expires. This option 
      // may need to be disabled when connecting to an 18.x database. Starting
      // in 19.x, the database can detect when it's running on a system where
      // OOB is not supported and automatically disable OOB. This automated 
      // detection is not impleneted in 18.x.
      OracleR2dbcOptions.DISABLE_OUT_OF_BAND_BREAK,

      // Allow the client-side ResultSet cache to be disabled. It is
      // necessary to do so when using the serializable transaction isolation
      // level in order to prevent phantom reads.
      OracleR2dbcOptions.ENABLE_QUERY_RESULT_CACHE
    );

  /** Guards access to a JDBC {@code Connection} created by this adapter */
  private final AsyncLock asyncLock = new AsyncLock();

  /**
   * Used to construct the instances of this class.
   */
  private OracleReactiveJdbcAdapter() { }

  /**
   * Returns an instance of this adapter.
   * @return An Oracle JDBC adapter
   */
  static OracleReactiveJdbcAdapter getInstance() {
    return new OracleReactiveJdbcAdapter();
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the ReactiveJdbcAdapter API by returning an instance of
   * {@link OracleDataSource} that implements the Reactive Extensions APIs for
   * creating connections.
   * </p>
   * <h3>Composing a JDBC URL</h3>
   * <p>
   * The {@code options} provided to this method are used to compose a URL
   * for the JDBC {@code DataSource}. Values for standard
   * {@link ConnectionFactoryOptions} of {@code HOST}, {@code PORT}, and
   * {@code DATABASE} are used to compose the JDBC URL with {@code DATABASE}
   * interpreted as a service name (not a system identifier (SID)):
   * </p><pre>
   *   jdbc:oracle:thin:@HOST:PORT/DATABASE
   * </pre><p>
   * Alternatively, the host, port, and service name may be specified using an
   * <a href="https://docs.oracle.com/en/database/oracle/oracle-database/21/netag/identifying-and-accessing-database.html#GUID-8D28E91B-CB72-4DC8-AEFC-F5D583626CF6"></a>
   * Oracle Net Descriptor</a>. The descriptor may be set as the value of an
   * {@link Option} having the name "descriptor". When the descriptor option is
   * present, the JDBC URL is composed as:
   * </p><pre>
   *   jdbc:oracle:thin:@(DESCRIPTION=...)
   * </pre><p>
   * When the "descriptor" option is provided, it is invalid to specify any
   * other options that might conflict with values also specified in the
   * descriptor. For instance, the descriptor element of
   * {@code (ADDRESSS=(HOST=...)(PORT=...)(PROTOCOL=...))} specifies values
   * that overlap with the standard {@code Option}s of {@code HOST}, {@code
   * PORT}, and {@code SSL}. An {@code IllegalArgumentException} is thrown
   * when the descriptor is provided with any overlapping {@code Option}s.
   * </p><p>
   * Note that the alias of a descriptor within a tnsnames.ora file may be
   * specified as the descriptor {@code Option} as well. Where "db1" is an
   * alias value set by the descriptor {@code Option}, a JDBC URL is composed
   * as:
   * </p><pre>
   *   jdbc:oracle:thin:@db1
   * </pre>
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
   * Non-sensitive options may be configured either programmatically using
   * {@link Option#valueOf(String)}, or by including name=value pairs
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
   *
   * @throws IllegalArgumentException If the {@code oracleNetDescriptor}
   * {@code Option} is provided with any other options that might have
   * conflicting values, such as {@link ConnectionFactoryOptions#HOST}.
   */
  @Override
  public DataSource createDataSource(ConnectionFactoryOptions options) {

    OracleDataSource oracleDataSource =
      fromJdbc(oracle.jdbc.pool.OracleDataSource::new);

    runJdbc(() -> oracleDataSource.setURL(composeJdbcUrl(options)));
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
   * @throws IllegalArgumentException If the {@code oracleNetDescriptor}
   * {@code Option} is provided with any other options that might have
   * conflicting values, such as {@link ConnectionFactoryOptions#HOST}.
   */
  private static String composeJdbcUrl(ConnectionFactoryOptions options) {
    Object descriptor = options.getValue(OracleR2dbcOptions.DESCRIPTOR);

    if (descriptor != null) {
      validateDescriptorOptions(options);
      return "jdbc:oracle:thin:@" + descriptor.toString();
    }
    else {
      Object host = options.getRequiredValue(HOST);
      Integer port = parseOptionValue(
        PORT, options, Integer.class, Integer::valueOf);
      Object serviceName = options.getValue(DATABASE);
      Boolean isTcps = parseOptionValue(
        SSL, options, Boolean.class, Boolean::valueOf);

      return String.format("jdbc:oracle:thin:@%s%s%s%s",
        Boolean.TRUE.equals(isTcps) ? "tcps:" : "",
        host,
        port != null ? (":" + port) : "",
        serviceName != null ? ("/" + serviceName) : "");
    }
  }

  /**
   * Validates {@code options} when the {@link OracleR2dbcOptions#DESCRIPTOR}
   * {@code Option} is present. It is invalid to specify any other options
   * having information that potentially conflicts with information in the
   * descriptor, such as {@link ConnectionFactoryOptions#HOST}.
   * @param options Options to validate
   * @throws IllegalArgumentException If {@code options} are invalid
   */
  private static void validateDescriptorOptions(
    ConnectionFactoryOptions options) {
    Option<?>[] conflictingOptions =
      Set.of(HOST, PORT, DATABASE, SSL)
        .stream()
        .filter(options::hasOption)
        .filter(option ->
          // Ignore options having a value that can be represented as a
          // zero-length String; It may be necessary to include a zero-length
          // host name in an R2DBC URL:
          // r2dbc:oracle://user:password@?oracleNetDescriptor=...
          ! options.getValue(option).toString().isEmpty())
        .toArray(Option[]::new);

    if (conflictingOptions.length != 0) {
      throw new IllegalArgumentException(OracleR2dbcOptions.DESCRIPTOR.name()
        + " Option has been specified with potentially conflicting Options: "
        + Arrays.toString(conflictingOptions));
    }
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

    Object user = options.getValue(USER);
    if (user != null)
      runJdbc(() -> oracleDataSource.setUser(user.toString()));

    Object password = options.getValue(PASSWORD);
    if (password != null) {
      runJdbc(() ->
        oracleDataSource.setPassword(password.toString()));
    }

    Duration timeout = parseOptionValue(
      CONNECT_TIMEOUT, options, Duration.class, Duration::parse);
    if (timeout != null) {
      runJdbc(() ->
        oracleDataSource.setLoginTimeout(
          Math.toIntExact(timeout.getSeconds())
            // Round up to nearest whole second
            + (timeout.getNano() == 0 ? 0 : 1)));
    }

  }

  /**
   * Configures an {@code oracleDataSource} with the values of extended R2DBC
   * {@code Options}. Extended options are those declared in
   * {@link OracleR2dbcOptions}. The values of these options are used to
   * configure the {@code oracleDataSource} as specified in the javadoc of
   * {@link #createDataSource(ConnectionFactoryOptions)}
   * @param oracleDataSource An data source to configure
   * @param options R2DBC options. Not null.
   */
  private static void configureExtendedOptions(
    OracleDataSource oracleDataSource, ConnectionFactoryOptions options) {

    // Handle the short form of the TNS_ADMIN option
    Object tnsAdmin = options.getValue(Option.valueOf("TNS_ADMIN"));
    if (tnsAdmin != null) {
      // Configure using the long form: oracle.net.tns_admin
      runJdbc(() ->
        oracleDataSource.setConnectionProperty(
          OracleConnection.CONNECTION_PROPERTY_TNS_ADMIN, tnsAdmin.toString()));
    }

    // Apply any JDBC connection property options
    for (Option<CharSequence> option : JDBC_CONNECTION_PROPERTY_OPTIONS) {
      // Using Object as the value type allows options to be set as types like
      // Boolean or Integer. These types make sense for numeric or boolean
      // connection property values, such as statement cache size, or enable x.
      Object value = options.getValue(option);
      if (value != null) {
        runJdbc(() ->
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
    Object value = options.getValue(option);

    if (value == null) {
      return null;
    }
    else if (type.isInstance(value)) {
      return type.cast(value);
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
   * this adapter requires by default. This method will not set a default
   * value for any connection property that has already been configured on the
   * {@code oracleDataSource}.
   * @param oracleDataSource A data source to configure
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
    runJdbc(() ->
      oracleDataSource.setConnectionProperty(enableJdbcSpecCompliance, "true"));

    // Cache PreparedStatements by default. The default value of the
    // OPEN_CURSORS parameter in the 21c and 19c databases is 50:
    // https://docs.oracle.com/en/database/oracle/oracle-database/21/refrn/OPEN_CURSORS.html#GUID-FAFD1247-06E5-4E64-917F-AEBD4703CF40
    // Assuming this default, then a default cache size of 25 will keep
    // each session at or below 50% of it's cursor capacity, which seems
    // reasonable.
    setPropertyIfAbsent(oracleDataSource,
      OracleConnection.CONNECTION_PROPERTY_IMPLICIT_STATEMENT_CACHE_SIZE, "25");

    // Prefetch LOB values by default. The database's maximum supported
    // prefetch size, 1GB, is configured by default. This is done so that
    // Row.get(...) can map LOB values into ByteBuffer/String without a
    // blocking database call. If the entire value is prefetched, then JDBC
    // won't need to fetch the remainder from the database when the entire is
    // value requested as a ByteBuffer or String.
    setPropertyIfAbsent(oracleDataSource,
      OracleConnection.CONNECTION_PROPERTY_DEFAULT_LOB_PREFETCH_SIZE,
      "1048576");

    // TODO: Disable the result set cache? This is needed to support the
    //  SERIALIZABLE isolation level, which requires result set caching to be
    //  disabled.
  }

  /**
   * Sets a JDBC connection {@code property} to a provided {@code value} if an
   * {@code oracleDataSource} has not already been configured with a
   * {@code value} for that {@code property}. This method is used to set
   * default values for properties that may otherwise be configured with user
   * defined values.
   * @param oracleDataSource DataSource to configure. Not null.
   * @param property Name of property to set. Not null.
   * @param value Value of {@code property} to set. Not null.
   */
  private static void setPropertyIfAbsent(
    OracleDataSource oracleDataSource, String property, String value) {

    runJdbc(() -> {
      String userValue = oracleDataSource.getConnectionProperty(property);

      // Don't override a value set by user code
      if (userValue == null)
        oracleDataSource.setConnectionProperty(property, value);
    });
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
    DataSource dataSource, Executor executor) {
    OracleDataSource oracleDataSource = unwrapOracleDataSource(dataSource);
    return Mono.from(adaptFlowPublisher(() ->
        oracleDataSource
          .createConnectionBuilder()
          .executorOracle(executor)
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

    return adaptFlowPublisher(
      oraclePreparedStatement::executeAsyncOracle);
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

    return adaptFlowPublisher(
      oraclePreparedStatement::executeBatchAsyncOracle);
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
    ResultSet resultSet, Function<JdbcReadable, T> rowMappingFunction) {

    OracleResultSet oracleResultSet = unwrapOracleResultSet(resultSet);

    return adaptFlowPublisher(() ->
      oracleResultSet.publisherOracle(oracleRow ->
        rowMappingFunction.apply(new OracleJdbcReadable(oracleRow))));
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
    Flow.Subscriber<byte[]> blobSubscriber = fromJdbc(() ->
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
    Flow.Subscriber<String> clobSubscriber = fromJdbc(() ->
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
    return usingConnection(Flux.from(deferOnce(publisherSupplier))
      .onErrorMap(SQLException.class, OracleR2dbcExceptions::toR2dbcException));
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
          publisher = toPublisher(fromJdbc(publisherSupplier));
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
    return fromJdbc(() ->
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
    return fromJdbc(() ->
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
    return fromJdbc(() ->
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
    return fromJdbc(() ->
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

  /**
   * Returns a {@code Publisher} that proxies signals to and from a
   * provided {@code publisher} in order to guards access to the JDBC
   * {@code Connection} associated with this adapter. Invocations of
   * {@link Publisher#subscribe(Subscriber)} and
   * {@link Subscription#request(long)} will only occur when the JDBC connection
   * is not being used by another thread or another publisher.
   *
   * @param publisher A publisher that uses the JDBC connection
   * @return A Publisher that
   */
  private <T> Publisher<T> usingConnection(Publisher<T> publisher) {

    return subscriber ->
      asyncLock.lock(() ->
        publisher.subscribe(
          new UsingConnectionSubscriber<>(subscriber)));

  }

  /**
   * <p>
   * A {@code Subscriber} that uses the {@link #asyncLock} to ensure that
   * threads do not become blocked when contending for this adapter's JDBC
   * {@code Connection}. Any time a {@code Subscriber} subscribes to a
   * {@code Publisher} that uses the JDBC {@code Connection}, an instance of
   * {@code UsingConnectionSubscriber} should be created in order to proxy
   * signals between that {@code Publisher} and the downstream
   * {@code Subscriber}.
   * </p>
   *
   * <h3>Problem Overview</h3>
   * <p>
   * {@code UsingConnectionSubscriber} solves a problem with how Oracle JDBC
   * implements thread safety. When an asynchronous database call is initiated
   * with a {@code Connection}, that {@code Connection} is locked until
   * the call completes. When a {@code Connection} is locked, any thread that
   * invokes a method of that {@code Connection} or any object created by that
   * {@code Connection} will become blocked. This can lead to a deadlock where
   * all threads in a pool have become blocked until the database call
   * completes, and JDBC can not complete the database call until a thread
   * becomes unblocked.
   * </p><p>
   * As a simplified example, consider what would happen with the code below if
   * the Executor had a pool of 1 thread:
   * </p><pre>
   * List<Flow.Publisher<Boolean>> publishers = new ArrayList<>();
   * executor.execute(() -> {
   *   try {
   *     publishers.add(connection.prepareStatement("SELECT 0 FROM dual")
   *       .unwrap(OraclePreparedStatement.class)
   *       .executeAsyncOracle());
   *
   *     publishers.add(connection.prepareStatement("SELECT 1 FROM dual")
   *       .unwrap(OraclePreparedStatement.class)
   *       .executeAsyncOracle());
   *   }
   *   catch (SQLException sqlException) {
   *     sqlException.printStackTrace();
   *   }
   * });
   * </pre><p>
   * After the first call to {@code executeAsyncOracle}, the connection is
   * locked, and so when the second call to {@code executeAsyncOracle} is
   * made, the executor thread is blocked. If Oracle JDBC is configured to use
   * this same executor, which has a pool of just one thread, then no thread
   * is left to handle the response from the database for the first call to
   * {@code executeAsyncOracle}. With no thread available to handle the
   * response, the call is never completed and the connection is never
   * unlocked, so the code above results in a deadlock.
   * </p><p>
   * While the code above presents a somewhat obvious scenario, it is more
   * common for deadlocks to occur in less obvious ways. Consider this code
   * example which uses Project Reactor and R2DBC:
   * </p><pre>
   * Flux.usingWhen(
   *   connectionFactory.create(),
   *   connection ->
   *     Flux.usingWhen(
   *       Mono.from(connection.beginTransaction())
   *         .thenReturn(connection),
   *       connection ->
   *         connection.createStatement("INSERT INTO deadlock VALUES(?)")
   *           .bind(0, 0)
   *           .execute(),
   *       Connection::commitTransaction),
   *   Connection::close)
   *   .hasElements();
   * </pre><p>
   * The hasElements() operator transforms the sequence into a single boolean
   * value. When an {@code onNext} signal delivers this value, the subscriber
   * emits a {@code cancel} signal to the upstream publisher as the
   * subscriber does not require any additional values. This cancel signal
   * triggers a subscription to both the commitTransaction() publisher and to
   * the close() publisher. The commitTransaction() publisher subscribed to
   * first, and this has the Oracle JDBC connection locked until that
   * database call completes. The close() publisher is subscribed to immediately
   * afterwards, and this has the thread become blocked. As there is no
   * thread left to handle the result of the commit, the connection never
   * becomes unlocked.
   * </p>
   *
   * <h3>Guarding Access to the JDBC Connection</h3>
   * <p>
   * Access to the JDBC Connection must be guarded such that no thread will
   * attempt to use it when an asynchronous database call is in-flight. The
   * potential for an in-flight call exists whenever there is a pending signal
   * from the upstream {@code Publisher}. Instances of
   * {@code UsingConnectionSubscriber} acquire the {@link #asyncLock}
   * before requesting a signal from the publisher, and release the
   * {@code asyncLock} once that signal is received. This ensures that no other
   * thread will be able to acquire the {@code asyncLock} when a pending signal
   * is potentially pending upon an asynchronous database call.
   * </p><p>
   * An {@code onSubscribe} signal is pending between an invocation of
   * {@link Publisher#subscribe(Subscriber)} and an invocation of
   * {@link Subscriber#onSubscribe(Subscription)}. Accordingly, the
   * {@link #asyncLock} <i>MUST</i> be acquired before invoking
   * {@code subscribe} with an instance of {@code UsingConnectionSubscriber}.
   * When that instance receives an {@code onSubscribe} signal, it will release
   * the {@code asyncLock}.
   * </p><p>
   * An {@code onNext} signal is pending between an invocation of
   * {@link Subscription#request(long)} and a number of invocations of
   * {@link Subscriber#onNext(Object)} equal to the number of
   * values requested. Accordingly, instances of
   * {@code UsingConnectionSubscriber} acquire the {@link #asyncLock} before
   * emitting a {@code request} signal, and release the {@code asyncLock} when
   * a corresponding number of {@code onNext} signals have been received.
   * </p><p>
   * When a {@code cancel} signal is emitted to the upstream {@code Publisher},
   * that publisher will not emit any further signals to the downstream
   * {@code Subscriber}. If an instance {@code UsingConnectionSubscriber}
   * has acquired the {@link #asyncLock} for a pending {@code onNext} signal,
   * then it will defer sending a {@code cancel} signal until the pending
   * {@code onNext} signal has been received. Deferring cancellation until the
   * the publisher invokes {@code onNext} ensures that the cancellation happens
   * after any pending database call, and before any subsequent database calls
   * that would obtain additional values for {@code onNext}.
   * </p>
   */
  private final class UsingConnectionSubscriber<T>
    implements Subscription, Subscriber<T> {

    /**
     * Value of {@link #demand} after a {@code cancel} signal has been received
     * from the downstream subscriber, but before a pending {@code onNext}
     * signal has been received.
     */
    private static final long CANCEL_PENDING = -1;

    /**
     * Value of {@link #demand} after a {@code cancel} signal has been received
     * from the downstream subscriber, and after any pending {@code onNext}
     * signal has been received, and after the {@code cancel} signal has been
     * emitted to the upstream publisher.
     */
    private static final long CANCEL_COMPLETE = -2;

    /** Downstream subscriber that requests values from database calls. */
    private final Subscriber<T> downstream;

    /**
     * Subscription from an upstream publisher that emits values from database
     * calls.
     */
    private Subscription upstream;

    /**
     * Unfilled demand from {@code request} signals. When the value is a
     * positive number, it is equal to the number of pending {@code onNext}
     * signals. When a {@code cancel} signal is received from downstream, the
     * value is set to either {@link #CANCEL_PENDING} or
     * {@link #CANCEL_COMPLETE}.
     * if an {@code
     * onNext}
     * signal is
     * pending, or it is set to {@link #CANCEL_COMPLETE} if no {@code onNext}
     * signal is pending.
     *
     */
    private final AtomicLong demand = new AtomicLong(0L);

    private UsingConnectionSubscriber(Subscriber<T> downstream) {
      this.downstream = downstream;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
      asyncLock.unlock();
      upstream = subscription;
      downstream.onSubscribe(this);
    }

    @Override
    public void request(long n) {
      long currentDemand = demand.getAndUpdate(current ->
        current < 0L
          ? current
          : Long.MAX_VALUE - current < n
            ? Long.MAX_VALUE
            : current + n);

      // If no onNext signals are currently pending, then acquire the lock
      // before signalling the request. Otherwise, if signals are already pending,
      // then lock is already acquired (and it won't be released by onNext
      // now that the demand value has been increased). If the current demand
      // is less than zero, then this subscription is cancelled, so don't
      // signal the request at all.
      if (currentDemand == 0)
        asyncLock.lock(() -> upstream.request(n));
      else if (currentDemand > 0)
        upstream.request(n);
    }

    @Override
    public void onNext(T value) {

      long currentDemand = demand.getAndUpdate(current ->
        current == Long.MAX_VALUE
          ? current
          : current == CANCEL_PENDING
            ? CANCEL_COMPLETE
            : current - 1L);

      // Send the cancel signal now if it was pending upon this onNext signal
      if (currentDemand == CANCEL_PENDING) {
        upstream.cancel();
        asyncLock.unlock();
      }
      else if (currentDemand > 0L){

        // Unlock if this was the last pending onNext signal. Note that even if
        // request(long) has been invoked after getAndUpdate returned, it will
        // not have acquired the the lock yet. The lock first needs to be
        // unlocked here.
        if (currentDemand == 1)
          asyncLock.unlock();

        downstream.onNext(value);
      }

      // Don't emit anything if this subscription has been cancelled.
    }

    @Override
    public void cancel() {
      long currentDemand = demand.getAndUpdate(current ->
        current > 0 || current == CANCEL_PENDING
          ? CANCEL_PENDING
          : CANCEL_COMPLETE);

      // Send the cancel signal now if no onNext signals are pending.
      if (currentDemand == 0)
        upstream.cancel();

    }

    @Override
    public void onError(Throwable error) {
      terminate();
      downstream.onError(error);
    }

    @Override
    public void onComplete() {
      terminate();
      downstream.onComplete();
    }

    /**
     * Terminates upon receiving {@code onComplete} or {@code onError}.
     * Termination has this subscriber release the {@link #asyncLock} if it
     * is currently being held. The {@link #demand} is updated so that no
     * future request signals will have this subscriber acquire the lock again.
     */
    private void terminate() {
      long currentDemand = demand.getAndSet(CANCEL_COMPLETE);

      if (currentDemand > 0 || currentDemand == CANCEL_PENDING)
        asyncLock.unlock();
    }
  }

  /**
   * <p>
   * A lock that is acquired asynchronously. Acquiring threads invoke
   * {@link #lock(Runnable)} with a {@code Runnable} that will access a
   * guarded resource. The {@code Runnable} <i>MUST</i> ensure that a single
   * invocation of {@link #unlock()} will occur after its {@code run()} method
   * has been invoked. The call to {@code unlock} may occur asynchronously on
   * a thread other than the one invoking {@code run}.
   * </p><p>
   * An instance of this lock is used to guard access to the Oracle JDBC
   * Connection, without blocking threads that contend for it.
   * </p>
   */
  private static final class AsyncLock {

    /**
     * Count that is incremented for invocation of {@link #lock(Runnable)},
     * and is decremented by each invocation of {@link #unlock()}. This lock
     * is unlocked when the count is 0.
     */
    private final AtomicInteger waitCount = new AtomicInteger(0);

    /**
     * Dequeue of {@code Runnable} callbacks enqueued each time an invocation of
     * {@link #lock(Runnable)} is not able to acquire this lock. The head of
     * this dequeue is dequeued and executed by an invocation of
     * {@link #unlock()}.
     */
    private final ConcurrentLinkedDeque<Runnable> waitQueue =
      new ConcurrentLinkedDeque<>();

    /**
     * Returns a {@code Publisher} that emits {@code onComplete} when
     * this lock is acquired.
     * @return
     */
    void lock(Runnable callback) {
      assert waitCount.get() >= 0 : "Wait count is less than 0: " + waitCount;

      // Acquire this lock and invoke the callback immediately, if possible
      if (waitCount.compareAndSet(0, 1)) {
        callback.run();
      }
      else {
        // Enqueue the callback to be invoked asynchronously when this
        // lock is unlocked
        waitQueue.addLast(callback);

        // Another thread may have unlocked this lock while this thread was
        // enqueueing the callback. Dequeue and execute the head of the deque
        // if this is the case.
        if (0 == waitCount.getAndIncrement())
          waitQueue.removeFirst().run();
      }

    }

    void unlock() {
      assert waitCount.get() > 0 : "Wait count is less than 1: " + waitCount;

      // Decrement the count. Assuming that lock was called before this
      // method, the count is guaranteed to be 1 or greater. If it greater
      // than 1 after being decremented, then another invocation of lock has
      // enqueued a callback
      if (0 != waitCount.decrementAndGet())
        waitQueue.removeFirst().run();
    }
  }

  /**
   * A {@code JdbcRow} that delegates to an {@link OracleRow}. An instance of
   * this class adapts the behavior of {@code OracleRow} to conform with
   * R2DBC standards.
   */
  private static final class OracleJdbcReadable implements JdbcReadable {

    /** OracleRow wrapped by this JdbcRow */
    private final OracleRow oracleRow;

    /** Constructs a new row that delegates to {@code oracleRow} */
    private OracleJdbcReadable(OracleRow oracleRow) {
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
