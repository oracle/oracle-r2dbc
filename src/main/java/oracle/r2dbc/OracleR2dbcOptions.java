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

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

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
  public static final Option<CharSequence> DESCRIPTOR =
    Option.valueOf("oracle.r2dbc.descriptor");

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
  public static final Option<Executor> EXECUTOR =
    Option.valueOf("oracle.r2dbc.executor");

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_TNS_ADMIN}
   */
  public static final Option<CharSequence> TNS_ADMIN =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_TNS_ADMIN);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_WALLET_LOCATION}
   */
  public static final Option<CharSequence> TLS_WALLET_LOCATION =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_WALLET_LOCATION);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_WALLET_PASSWORD}
   */
  public static final Option<CharSequence> TLS_WALLET_PASSWORD =
    Option.sensitiveValueOf(OracleConnection.CONNECTION_PROPERTY_WALLET_PASSWORD);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORE}
   */
  public static final Option<CharSequence> TLS_KEYSTORE =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORE);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORETYPE}
   */
  public static final Option<CharSequence> TLS_KEYSTORE_TYPE =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORETYPE);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTOREPASSWORD}
   */
  public static final Option<CharSequence> TLS_KEYSTORE_PASSWORD =
    Option.sensitiveValueOf(OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTOREPASSWORD);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTORE}
   */
  public static final Option<CharSequence> TLS_TRUSTSTORE =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTORE);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTORETYPE}
   */
  public static final Option<CharSequence> TLS_TRUSTSTORE_TYPE =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTORETYPE);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTOREPASSWORD}
   */
  public static final Option<CharSequence> TLS_TRUSTSTORE_PASSWORD =
    Option.sensitiveValueOf(OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTOREPASSWORD);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_SERVICES}
   */
  public static final Option<CharSequence> AUTHENTICATION_SERVICES =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_SERVICES);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_SSL_CERTIFICATE_ALIAS}
   */
  public static final Option<CharSequence> TLS_CERTIFICATE_ALIAS =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_THIN_SSL_CERTIFICATE_ALIAS);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_SSL_SERVER_DN_MATCH}
   */
  public static final Option<CharSequence> TLS_SERVER_DN_MATCH =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_THIN_SSL_SERVER_DN_MATCH);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_SSL_SERVER_CERT_DN}
   */
  public static final Option<CharSequence> TLS_SERVER_CERT_DN =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_THIN_SSL_SERVER_CERT_DN);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_SSL_VERSION}
   */
  public static final Option<CharSequence> TLS_VERSION =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_THIN_SSL_VERSION);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_SSL_CIPHER_SUITES}
   */
  public static final Option<CharSequence> TLS_CIPHER_SUITES =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_THIN_SSL_CIPHER_SUITES);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_SSL_KEYMANAGERFACTORY_ALGORITHM}
   */
  public static final Option<CharSequence> TLS_KEYMANAGERFACTORY_ALGORITHM =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_THIN_SSL_KEYMANAGERFACTORY_ALGORITHM);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_SSL_TRUSTMANAGERFACTORY_ALGORITHM}
   */
  public static final Option<CharSequence> TLS_TRUSTMANAGERFACTORY_ALGORITHM =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_THIN_SSL_TRUSTMANAGERFACTORY_ALGORITHM);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_SSL_CONTEXT_PROTOCOL}
   */
  public static final Option<CharSequence> SSL_CONTEXT_PROTOCOL =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_SSL_CONTEXT_PROTOCOL);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_FAN_ENABLED}
   */
  public static final Option<CharSequence> FAN_ENABLED =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_FAN_ENABLED);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_IMPLICIT_STATEMENT_CACHE_SIZE}
   */
  public static final Option<CharSequence> IMPLICIT_STATEMENT_CACHE_SIZE =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_IMPLICIT_STATEMENT_CACHE_SIZE);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_DEFAULT_LOB_PREFETCH_SIZE}
   */
  public static final Option<CharSequence> DEFAULT_LOB_PREFETCH_SIZE =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_DEFAULT_LOB_PREFETCH_SIZE);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_THIN_NET_DISABLE_OUT_OF_BAND_BREAK}
   */
  public static final Option<CharSequence> DISABLE_OUT_OF_BAND_BREAK =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_THIN_NET_DISABLE_OUT_OF_BAND_BREAK);

  /**
   * Configures the Oracle JDBC Connection used by Oracle R2DBC as specified by:
   * {@link OracleConnection#CONNECTION_PROPERTY_ENABLE_QUERY_RESULT_CACHE}
   */
  public static final Option<CharSequence> ENABLE_QUERY_RESULT_CACHE =
    Option.valueOf(OracleConnection.CONNECTION_PROPERTY_ENABLE_QUERY_RESULT_CACHE);
}
