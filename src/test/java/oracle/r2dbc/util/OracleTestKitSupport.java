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

package oracle.r2dbc.util;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import oracle.r2dbc.impl.OracleConnectionFactoryProviderImpl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

/**
 * Initializes a {@link ConnectionFactory} with options specified
 * by {@link DatabaseConfigExtension}.
 *
 * @author  harayuanwang
 * @since   0.1.0
 */
public abstract class OracleTestKitSupport {
  @RegisterExtension
  protected static final DatabaseConfigExtension CONFIG =
    new DatabaseConfigExtension();

  protected static ConnectionFactory connectionFactory;

  @BeforeAll
  static void beforeAll() {
    ConnectionFactoryOptions options = builder().build();
    connectionFactory = ConnectionFactories.get(options);
  }

  static ConnectionFactoryOptions.Builder builder() {
    return ConnectionFactoryOptions.builder()
      .option(DRIVER, new OracleConnectionFactoryProviderImpl().getDriver())
      .option(DATABASE, CONFIG.getServiceName())
      .option(HOST, CONFIG.getHost())
      .option(PORT, CONFIG.getPort())
      .option(PASSWORD, CONFIG.getPassword())
      .option(USER, CONFIG.getUsername());
  }
}

/*
   MODIFIED    (MM/DD/YY)
    harayuanwang    05/12/20 - Creation
 */
