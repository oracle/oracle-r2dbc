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
import io.r2dbc.spi.ConnectionFactoryOptions;
import oracle.r2dbc.OracleR2dbcOptions;
import reactor.core.publisher.Mono;

import static oracle.r2dbc.samples.DatabaseConfig.HOST;
import static oracle.r2dbc.samples.DatabaseConfig.PORT;
import static oracle.r2dbc.samples.DatabaseConfig.SERVICE_NAME;
import static oracle.r2dbc.samples.DatabaseConfig.USER;
import static oracle.r2dbc.samples.DatabaseConfig.PASSWORD;

/**
 * This code example shows how to use TNS descriptor URLs with Oracle R2DBC.
 * The TNS descriptor has the form:
 * <pre>
 *   (DESCRIPTION=...)
 * </pre>
 * The full syntax of the TNS descriptor is described in the
 * <a href=https://docs.oracle.com/en/database/oracle/oracle-database/21/netag/identifying-and-accessing-database.html#GUID-8D28E91B-CB72-4DC8-AEFC-F5D583626CF6>
 *   Oracle Net Services Administrator's Guide
 * </a>
 */
public class DescriptorURL {

  /**
   * A TNS descriptor specifying the HOST, PORT, and SERVICE_NAME read from
   * {@link DatabaseConfig}. These values can be configured in a
   * config.properties file of the current directory.
   */
  private static final String DESCRIPTOR = "(DESCRIPTION=" +
    "(ADDRESS=(HOST="+HOST+")(PORT="+PORT+")(PROTOCOL=tcp))" +
    "(CONNECT_DATA=(SERVICE_NAME="+SERVICE_NAME+")))";

  public static void main(String[] args) {
    // A descriptor may appear in the query section of an R2DBC URL:
    String r2dbcUrl = "r2dbc:oracle://?oracle.r2dbc.descriptor="+DESCRIPTOR;
    Mono.from(ConnectionFactories.get(ConnectionFactoryOptions.parse(r2dbcUrl)
      .mutate()
      .option(ConnectionFactoryOptions.USER, USER)
      .option(ConnectionFactoryOptions.PASSWORD, PASSWORD)
      .build())
      .create())
      .flatMapMany(connection ->
        Mono.from(connection.createStatement(
          "SELECT 'Connected with TNS descriptor' FROM sys.dual")
          .execute())
          .flatMapMany(result ->
            result.map(row -> row.get(0, String.class)))
          .concatWith(Mono.from(connection.close()).cast(String.class)))
      .toStream()
      .forEach(System.out::println);

    // A descriptor may also be specified as an Option
    Mono.from(ConnectionFactories.get(ConnectionFactoryOptions.builder()
      .option(ConnectionFactoryOptions.DRIVER, "oracle")
      .option(OracleR2dbcOptions.DESCRIPTOR, DESCRIPTOR)
      .option(ConnectionFactoryOptions.USER, USER)
      .option(ConnectionFactoryOptions.PASSWORD, PASSWORD)
      .build())
      .create())
      .flatMapMany(connection ->
        Mono.from(connection.createStatement(
          "SELECT 'Connected with TNS descriptor' FROM sys.dual")
          .execute())
          .flatMapMany(result ->
            result.map((row, metadata) -> row.get(0, String.class)))
          .concatWith(Mono.from(connection.close()).cast(String.class)))
      .toStream()
      .forEach(System.out::println);
  }
}
