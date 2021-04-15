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

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;

import java.util.ServiceLoader;

import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;

/**
 * <p>
 * Implementation of the {@link ConnectionFactoryProvider} SPI for the Oracle
 * Database. This provider
 * {@linkplain #supports(ConnectionFactoryOptions) supports}
 * {@link ConnectionFactoryOptions} that specify {@code DRIVER} as the value
 * {@code "oracle"}.
 * </p><p>
 * The Oracle R2DBC Driver JAR identifies this class as a service provider for
 * the R2DBC SPI. Identification is specified with a
 * provider-configuration-file included under {@code META-INF/services}. A
 * {@link ServiceLoader} uses the provider-configuration-file to locate this
 * class.
 * </p>
 *
 * @author  harayuanwang, michael-a-mcmahon
 * @since   0.1.0
 */
public final class OracleConnectionFactoryProviderImpl
  implements ConnectionFactoryProvider {

  /**
   * Identifier of the Oracle R2DBC Driver that can appear in a R2DBC URL or
   * connection factory options.
   */
  private static final String DRIVER_IDENTIFIER = "oracle";

  /**
   * <p>
   * Constructs a new connection factory provider.
   * </p><p>
   * This public no-arg constructor is explicitly declared in conformance
   * with the discovery mechanism described in
   * <a href="https://r2dbc.io/spec/0.8.2.RELEASE/spec/html/#connections.factory.discovery">
   * Section 5.2 of the 0.8.2 R2DBC Specification
   * </a>
   * </p><p>
   * <i>
   *   This constructor is not supported for general application programming.
   * </i>
   * Application programmers should use the
   * {@link io.r2dbc.spi.ConnectionFactories} APIs to obtain an instance of
   * the Oracle R2DBC {@link ConnectionFactory}.
   * </p>
   */
  public OracleConnectionFactoryProviderImpl() { }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning a new
   * {@code ConnectionFactory} that applies values specified by the {@code
   * options} parameter, when opening connections to an Oracle Database.
   * </p>
   *
   * @throws IllegalStateException If any option required by
   * {@link OracleConnectionFactoryImpl} is not specified by {@code options}.
   *
   * @throws IllegalArgumentException If the {@code oracleNetDescriptor}
   * {@code Option} is provided with any other options that might have
   * conflicting values, such as {@link ConnectionFactoryOptions#HOST}.
   */
  @Override
  public ConnectionFactory create(ConnectionFactoryOptions options) {
    assert supports(options) : "Options are not supported: " + options;
    requireNonNull(options, "options must not be null.");
    return new OracleConnectionFactoryImpl(options);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning {@code true} if the {@code
   * options} parameter specifies the value of {@code DRIVER} as "oracle".
   * </p>
   */
  @Override
  public boolean supports(ConnectionFactoryOptions options) {
    requireNonNull(options, "options must not be null.");
    return DRIVER_IDENTIFIER.equals(options.getValue(DRIVER));
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning the identifier of the Oracle
   * R2DBC Driver, which is "oracle".
   * </p>
   */
  @Override
  public String getDriver() {
    return DRIVER_IDENTIFIER;
  }

}
