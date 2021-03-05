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

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.junit.jupiter.api.Test;

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
}
