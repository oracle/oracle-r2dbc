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

import java.sql.DatabaseMetaData;
import io.r2dbc.spi.ConnectionMetadata;

import static oracle.r2dbc.impl.OracleR2dbcExceptions.fromJdbc;


/**
 * <p>
 * Implementation of the {@link ConnectionMetadata} SPI for Oracle Database.
 * </p><p>
 * Instances of this class supply metadata values of a JDBC
 * {@link DatabaseMetaData} object.
 * </p>
 *
 * @author  harayuanwang
 * @since   0.1.0
 */
final class OracleConnectionMetadataImpl implements ConnectionMetadata {

  /** Metadata from a JDBC connection */
  private final DatabaseMetaData dbMetaData;

  /**
   * Constructs a new instance that supplies metadata from the specified
   * {@code dbMetaData}.
   * @param dbMetaData Metadata to supply.
   */
  OracleConnectionMetadataImpl(DatabaseMetaData dbMetaData) {
    this.dbMetaData = dbMetaData;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning the database product name
   * supplied by the JDBC {@code DatabaseMetaData}.
   * </p>
   */
  @Override
  public String getDatabaseProductName() {
    return fromJdbc(dbMetaData::getDatabaseProductName);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning the database product version
   * supplied by the JDBC {@code DatabaseMetaData}.
   * </p>
   */
  @Override
  public String getDatabaseVersion() {
    return fromJdbc(dbMetaData::getDatabaseProductVersion);
  }
}