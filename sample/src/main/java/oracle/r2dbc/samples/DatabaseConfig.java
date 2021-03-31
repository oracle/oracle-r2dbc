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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * <p>
 * Configuration for connecting code samples to an Oracle Database instance.
 * </p><p>
 * The configuration is read from a properties file in the current directory
 * by default, or from a file specified as
 * <code>-DCONFIG_FILE=/path/to/your/config.properties</code>
 * </p>
 */
public class DatabaseConfig {

  /** Path to a configuration file: config.properties */
  private static final Path CONFIG_PATH =
    Path.of(System.getProperty("CONFIG_FILE", "config.properties"));

  /** Configuration that is read from a file at {@link #CONFIG_PATH} */
  private static final Properties CONFIG;
  static {
    try (var fileStream = Files.newInputStream(CONFIG_PATH)) {
      CONFIG = new Properties();
      CONFIG.load(fileStream);
    }
    catch (IOException readFailure) {
      throw new UncheckedIOException(readFailure);
    }
  }

  /** Host name where an Oracle Database instance is running */
  static final String HOST = CONFIG.getProperty("HOST");

  /** Port number where an Oracle Database instance is listening */
  static final int PORT = Integer.parseInt(CONFIG.getProperty("PORT"));

  /** Service name of an Oracle Database */
  static final String SERVICE_NAME = CONFIG.getProperty("SERVICE_NAME");

  /** User name that connects to an Oracle Database */
  static final String USER = CONFIG.getProperty("USER");

  /** Password of the user that connects to an Oracle Database */
  static final String PASSWORD = CONFIG.getProperty("PASSWORD");

  /** The file system path of a wallet directory */
  static final String WALLET_LOCATION =
    CONFIG.getProperty("WALLET_LOCATION");
}
