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

import oracle.jdbc.pool.OracleDataSource;
import oracle.r2dbc.DatabaseConfig;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

/**
 *  JUnit Extension to establish an Oracle Database connection configuration
 *  during integration tests.
 *
 *  @author  harayuanwang
 *  @since   0.1.0
 */
public final class DatabaseConfigExtension implements BeforeAllCallback {
  private JdbcOperations jdbcOperations;
  private OracleDataSource dataSource;

  @Override
  public void beforeAll(ExtensionContext extensionContext)
    throws IOException, SQLException {
    initialize();
  }

  private void initialize() throws IOException, SQLException {
    dataSource = new OracleDataSource();
    dataSource.setURL(String.format("jdbc:oracle:thin:@%s:%d/%s",
      DatabaseConfig.host(), DatabaseConfig.port(),
      DatabaseConfig.serviceName()));
    dataSource.setUser(DatabaseConfig.user());
    dataSource.setPassword(DatabaseConfig.password());
    this.jdbcOperations = new JdbcTemplate(dataSource);
  }

  public JdbcOperations getJDBCOperations() {
    return this.jdbcOperations;
  }

  public String getUsername() {
    return DatabaseConfig.user();
  }

  public String getPassword() {
    return DatabaseConfig.password();
  }

  public String getServiceName() {
    return DatabaseConfig.serviceName();
  }

  public String getHost() {
    return DatabaseConfig.host();
  }

  public int getPort() {
    return DatabaseConfig.port();
  }
}

/*
   MODIFIED    (MM/DD/YY)
    harayuanwang    05/12/20 - Creation
 */
