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
package oracle.r2dbc;

import io.r2dbc.spi.Option;
import oracle.jdbc.OracleConnection;
import org.reactivestreams.Publisher;

import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

/**
 * Extended {@link Option}s supported by the Oracle R2DBC Driver.
 */
public final class OracleR2dbcOptions {

  private OracleR2dbcOptions() {}

  /**
   * Extended {@code Option} that specifies an Oracle Net Connect Descriptor
   * of the form "(DESCRIPTION=...)". If {@link #TNS_ADMIN} is specified,
   * then the value of this {@code Option} may be set to a tnsnames.ora
   * alias.
   */
  public static final Option<CharSequence> DESCRIPTOR;

  /**
   * Extended {@code Option} that specifies an {@link Executor} for executing
   * asynchronous tasks. This {@code Option} can not be configured in the
   * query section of an R2DBC URL, it must be configured programmatically,
   * as in:
   * <pre>
   * Executor myExecutor = getMyExecutor();
   *
   * ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
   *   .option(ConnectionFactoryOptions.DRIVER, "oracle")
   *   .option(OracleR2dbcOptions.EXECUTOR, myExecutor)
   *   ...
   *   .build();
   * </pre>
   * If this option is not configured, then Oracle R2DBC will use
   * {@code ForkJoinPool}'s
   * {@linkplain ForkJoinPool#commonPool() common pool} by default.
   */
  public static final Option<Executor> EXECUTOR;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_TNS_ADMIN}
   */
  public static final Option<CharSequence> TNS_ADMIN;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_WALLET_LOCATION}
   */
  public static final Option<CharSequence> TLS_WALLET_LOCATION;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_WALLET_PASSWORD}
   */
  public static final Option<CharSequence> TLS_WALLET_PASSWORD;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORE}
   */
  public static final Option<CharSequence> TLS_KEYSTORE;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORETYPE}
   */
  public static final Option<CharSequence> TLS_KEYSTORE_TYPE;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTOREPASSWORD}
   */
  public static final Option<CharSequence> TLS_KEYSTORE_PASSWORD;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTORE}
   */
  public static final Option<CharSequence> TLS_TRUSTSTORE;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTORETYPE}
   */
  public static final Option<CharSequence> TLS_TRUSTSTORE_TYPE;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTOREPASSWORD}
   */
  public static final Option<CharSequence> TLS_TRUSTSTORE_PASSWORD;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_SERVICES}
   */
  public static final Option<CharSequence> AUTHENTICATION_SERVICES;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_SSL_CERTIFICATE_ALIAS}
   */
  public static final Option<CharSequence> TLS_CERTIFICATE_ALIAS;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_SSL_SERVER_DN_MATCH}
   */
  public static final Option<CharSequence> TLS_SERVER_DN_MATCH;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_SSL_SERVER_CERT_DN}
   */
  public static final Option<CharSequence> TLS_SERVER_CERT_DN;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_SSL_VERSION}
   */
  public static final Option<CharSequence> TLS_VERSION;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_SSL_CIPHER_SUITES}
   */
  public static final Option<CharSequence> TLS_CIPHER_SUITES;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_SSL_KEYMANAGERFACTORY_ALGORITHM}
   */
  public static final Option<CharSequence> TLS_KEYMANAGERFACTORY_ALGORITHM;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_SSL_TRUSTMANAGERFACTORY_ALGORITHM}
   */
  public static final Option<CharSequence> TLS_TRUSTMANAGERFACTORY_ALGORITHM;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_SSL_CONTEXT_PROTOCOL}
   */
  public static final Option<CharSequence> SSL_CONTEXT_PROTOCOL;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_FAN_ENABLED}
   */
  public static final Option<CharSequence> FAN_ENABLED;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_IMPLICIT_STATEMENT_CACHE_SIZE}
   */
  public static final Option<CharSequence> IMPLICIT_STATEMENT_CACHE_SIZE;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_DEFAULT_LOB_PREFETCH_SIZE}
   */
  public static final Option<CharSequence> DEFAULT_LOB_PREFETCH_SIZE;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_NET_DISABLE_OUT_OF_BAND_BREAK}
   */
  public static final Option<CharSequence> DISABLE_OUT_OF_BAND_BREAK;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_ENABLE_QUERY_RESULT_CACHE}
   */
  public static final Option<CharSequence> ENABLE_QUERY_RESULT_CACHE;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_VSESSION_TERMINAL}
   */
  public static final Option<CharSequence> VSESSION_TERMINAL;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_VSESSION_MACHINE}
   */
  public static final Option<CharSequence> VSESSION_MACHINE;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_VSESSION_OSUSER}
   */
  public static final Option<CharSequence> VSESSION_OSUSER;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_VSESSION_PROGRAM}
   */
  public static final Option<CharSequence> VSESSION_PROGRAM;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_VSESSION_PROCESS}
   */
  public static final Option<CharSequence> VSESSION_PROCESS;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_TIMEZONE_AS_REGION}
   */
  public static final Option<CharSequence> TIMEZONE_AS_REGION;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_LDAP_SECURITY_AUTHENTICATION}
   */
  public static final Option<CharSequence> LDAP_SECURITY_AUTHENTICATION;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_LDAP_SECURITY_PRINCIPAL}
   */
  public static final Option<CharSequence> LDAP_SECURITY_PRINCIPAL;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_LDAP_SECURITY_CREDENTIALS}
   */
  public static final Option<CharSequence> LDAP_SECURITY_CREDENTIALS;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_JNDI_LDAP_CONNECT_TIMEOUT}
   */
  public static final Option<CharSequence> LDAP_CONNECT_TIMEOUT;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_JNDI_LDAP_READ_TIMEOUT}
   */
  public static final Option<CharSequence> LDAP_READ_TIMEOUT;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_LDAP_SSL_WALLET_LOCATION}
   */
  public static final Option<CharSequence> LDAP_WALLET_LOCATION;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_LDAP_SSL_WALLET_PASSWORD}
   */
  public static final Option<CharSequence> LDAP_WALLET_PASSWORD;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_LDAP_SSL_KEYSTORE_TYPE}
   */
  public static final Option<CharSequence> LDAP_KEYSTORE_TYPE;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_LDAP_SSL_KEYSTORE}
   */
  public static final Option<CharSequence> LDAP_KEYSTORE;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_LDAP_SSL_KEYSTORE_PASSWORD}
   */
  public static final Option<CharSequence> LDAP_KEYSTORE_PASSWORD;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_LDAP_SSL_TRUSTSTORE_TYPE}
   */
  public static final Option<CharSequence> LDAP_TRUSTSTORE_TYPE;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_LDAP_SSL_TRUSTSTORE}
   */
  public static final Option<CharSequence> LDAP_TRUSTSTORE;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_LDAP_SSL_TRUSTSTORE_PASSWORD}
   */
  public static final Option<CharSequence> LDAP_TRUSTSTORE_PASSWORD;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_LDAP_SSL_CIPHER_SUITES}
   */
  public static final Option<CharSequence> LDAP_CIPHER_SUITES;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_LDAP_SSL_VERSIONS}
   */
  public static final Option<CharSequence> LDAP_VERSIONS;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_LDAP_SSL_KEYMANAGER_FACTORY_ALGORITHM}
   */
  public static final Option<CharSequence> LDAP_KEYMANAGER_FACTORY_ALGORITHM;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_LDAP_SSL_TRUSTMANAGER_FACTORY_ALGORITHM}
   */
  public static final Option<CharSequence> LDAP_TRUSTMANAGER_FACTORY_ALGORITHM;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_LDAP_SSL_CONTEXT_PROTOCOL}
   */
  public static final Option<CharSequence> LDAP_CONTEXT_PROTOCOL;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_NET_CHECKSUM_LEVEL}
   */
  public static final Option<CharSequence> NET_CHECKSUM_LEVEL;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_NET_CHECKSUM_TYPES}
   */
  public static final Option<CharSequence> NET_CHECKSUM_TYPES;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_NET_ENCRYPTION_LEVEL}
   */
  public static final Option<CharSequence> NET_ENCRYPTION_LEVEL;

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_NET_ENCRYPTION_TYPES}
   */
  public static final Option<CharSequence> NET_ENCRYPTION_TYPES;


  /** The unmodifiable set of all extended options */
  private static final Set<Option<?>> OPTIONS = Set.of(
    DESCRIPTOR = Option.valueOf("oracle.r2dbc.descriptor"),
    EXECUTOR = Option.valueOf("oracle.r2dbc.executor"),
    TNS_ADMIN = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_TNS_ADMIN),
    TLS_WALLET_LOCATION = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_WALLET_LOCATION),
    TLS_WALLET_PASSWORD = Option.sensitiveValueOf(
      OracleConnection.CONNECTION_PROPERTY_WALLET_PASSWORD),
    TLS_KEYSTORE = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORE),
    TLS_KEYSTORE_TYPE = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORETYPE),
    TLS_KEYSTORE_PASSWORD = Option.sensitiveValueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTOREPASSWORD),
    TLS_TRUSTSTORE = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTORE),
    TLS_TRUSTSTORE_TYPE = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTORETYPE),
    TLS_TRUSTSTORE_PASSWORD = Option.sensitiveValueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTOREPASSWORD),
    AUTHENTICATION_SERVICES = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_SERVICES),
    TLS_CERTIFICATE_ALIAS = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_SSL_CERTIFICATE_ALIAS),
    TLS_SERVER_DN_MATCH = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_SSL_SERVER_DN_MATCH),
    TLS_SERVER_CERT_DN = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_SSL_SERVER_CERT_DN),
    TLS_VERSION = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_SSL_VERSION),
    TLS_CIPHER_SUITES = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_SSL_CIPHER_SUITES),
    TLS_KEYMANAGERFACTORY_ALGORITHM = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_SSL_KEYMANAGERFACTORY_ALGORITHM),
    TLS_TRUSTMANAGERFACTORY_ALGORITHM = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_SSL_TRUSTMANAGERFACTORY_ALGORITHM),
    SSL_CONTEXT_PROTOCOL = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_SSL_CONTEXT_PROTOCOL),
    FAN_ENABLED = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_FAN_ENABLED),
    IMPLICIT_STATEMENT_CACHE_SIZE = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_IMPLICIT_STATEMENT_CACHE_SIZE),
    DEFAULT_LOB_PREFETCH_SIZE = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_DEFAULT_LOB_PREFETCH_SIZE),
    DISABLE_OUT_OF_BAND_BREAK = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_NET_DISABLE_OUT_OF_BAND_BREAK),
    ENABLE_QUERY_RESULT_CACHE = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_ENABLE_QUERY_RESULT_CACHE),
    VSESSION_TERMINAL = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_VSESSION_TERMINAL),
    VSESSION_MACHINE = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_VSESSION_MACHINE),
    VSESSION_OSUSER = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_VSESSION_OSUSER),
    VSESSION_PROGRAM = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_VSESSION_PROGRAM),
    VSESSION_PROCESS = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_VSESSION_PROCESS),
    TIMEZONE_AS_REGION = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_TIMEZONE_AS_REGION),
    LDAP_SECURITY_AUTHENTICATION = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_LDAP_SECURITY_AUTHENTICATION),
    LDAP_SECURITY_PRINCIPAL = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_LDAP_SECURITY_PRINCIPAL),
    LDAP_SECURITY_CREDENTIALS = Option.sensitiveValueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_LDAP_SECURITY_CREDENTIALS),
    LDAP_CONNECT_TIMEOUT = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_JNDI_LDAP_CONNECT_TIMEOUT),
    LDAP_READ_TIMEOUT = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_JNDI_LDAP_READ_TIMEOUT),
    LDAP_WALLET_LOCATION = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_LDAP_SSL_WALLET_LOCATION),
    LDAP_WALLET_PASSWORD = Option.sensitiveValueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_LDAP_SSL_WALLET_PASSWORD),
    LDAP_KEYSTORE_TYPE = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_LDAP_SSL_KEYSTORE_TYPE),
    LDAP_KEYSTORE = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_LDAP_SSL_KEYSTORE),
    LDAP_KEYSTORE_PASSWORD = Option.sensitiveValueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_LDAP_SSL_KEYSTORE_PASSWORD),
    LDAP_TRUSTSTORE_TYPE = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_LDAP_SSL_TRUSTSTORE_TYPE),
    LDAP_TRUSTSTORE = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_LDAP_SSL_TRUSTSTORE),
    LDAP_TRUSTSTORE_PASSWORD = Option.sensitiveValueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_LDAP_SSL_TRUSTSTORE_PASSWORD),
    LDAP_CIPHER_SUITES = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_LDAP_SSL_CIPHER_SUITES),
    LDAP_VERSIONS = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_LDAP_SSL_VERSIONS),
    LDAP_KEYMANAGER_FACTORY_ALGORITHM = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_LDAP_SSL_KEYMANAGER_FACTORY_ALGORITHM),
    LDAP_TRUSTMANAGER_FACTORY_ALGORITHM = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_LDAP_SSL_TRUSTMANAGER_FACTORY_ALGORITHM),
    LDAP_CONTEXT_PROTOCOL = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_LDAP_SSL_CONTEXT_PROTOCOL),
    NET_CHECKSUM_LEVEL = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_NET_CHECKSUM_LEVEL),
    NET_CHECKSUM_TYPES = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_NET_CHECKSUM_TYPES),
    NET_ENCRYPTION_LEVEL = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_NET_ENCRYPTION_LEVEL),
    NET_ENCRYPTION_TYPES = Option.valueOf(
      OracleConnection.CONNECTION_PROPERTY_THIN_NET_ENCRYPTION_TYPES)
  );

  /**
   * Returns the set of all extended options supported by Oracle R2DBC. The
   * returned set contains each {@code Option} constant declared by
   * {@code OracleR2dbcOptions}. The {@code Set} returned by this method is
   * unmodifiable, as described in the class level JavaDoc of {@link Set}.
   * @return The set of all options. Not null. Not mutable.
   */
  public static Set<Option<?>> options() {
    return OPTIONS;
  }

  /**
   * <p>
   * Casts an <code>Option&lt;T&gt;</code> to
   * <code>Option&lt;Supplier&lt;T&gt;&gt;</code>. For instance, if an
   * <code>Option&lt;CharSequence&gt;</code> is passed to this method, it is
   * returned as an
   * <code>Option&lt;Supplier&lt;CharSequence&gt;&gt;</code>.
   * </p><p>
   * This method can used when configuring an <code>Option</code> with values
   * from a <code>Supplier</code>:
   * <pre>{@code
   * void configurePassword(ConnectionFactoryOptions.Builder optionsBuilder) {
   *   optionsBuilder.option(supplied(PASSWORD), () -> getPassword());
   * }
   *
   * CharSequence getPassword() {
   *   // ... return a database password ...
   * }
   * }</pre>
   * </p><p>
   * It is not strictly necessary to use this method when configuring an
   * <code>Option</code> with a value from a <code>Supplier</code>. This method
   * is offered for code readability and convenience.
   * </p>
   */
  public static <T> Option<Supplier<T>> supplied(Option<T> option) {
    @SuppressWarnings("unchecked")
    Option<Supplier<T>> supplierOption = (Option<Supplier<T>>)option;
    return supplierOption;
  }

  /**
   * <p>
   * Casts an <code>Option&lt;T&gt;</code> to
   * <code>Option&lt;Publisher&lt;T&gt;&gt;</code>. For instance, if an
   * <code>Option&lt;CharSequence&gt;</code> is passed to this method, it
   * is returned as an
   * <code>Option&lt;Publisher&lt;CharSequence&gt;&gt;</code>.
   * </p><p>
   * This method can used when configuring an <code>Option</code> with values
   * from a <code>Publisher</code>:
   * <pre>{@code
   * void configurePassword(ConnectionFactoryOptions.Builder optionsBuilder) {
   *   optionsBuilder.option(published(PASSWORD), getPasswordPublisher());
   * }
   *
   * Publisher<CharSequence> getPasswordPublisher() {
   *   // ... publish a database password ...
   * }
   * }</pre>
   * </p><p>
   * It is not strictly necessary to use this method when configuring an
   * <code>Option</code> with a value from a <code>Publisher</code>. This method
   * is offered for code readability and convenience.
   * </p>
   */
  public static <T> Option<Publisher<T>> published(Option<T> option) {
    @SuppressWarnings("unchecked")
    Option<Publisher<T>> publisherOption = (Option<Publisher<T>>)option;
    return publisherOption;
  }

}
