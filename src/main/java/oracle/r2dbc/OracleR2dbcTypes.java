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

import io.r2dbc.spi.Type;
import oracle.sql.json.OracleJsonObject;

import java.nio.ByteBuffer;
import java.sql.RowId;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;

/**
 * SQL types supported by Oracle Database that are not defined as standard types
 * by {@link io.r2dbc.spi.R2dbcType}.
 */
public final class OracleR2dbcTypes {

  private OracleR2dbcTypes() {}

  /**
   * A 64-bit, double-precision floating-point number data type.
   */
  public static final Type BINARY_DOUBLE =
    new TypeImpl(Double.class, "BINARY_DOUBLE");

  /**
   * A 32-bit, single-precision floating-point number data type.
   */
  public static final Type BINARY_FLOAT =
    new TypeImpl(Float.class, "BINARY_FLOAT");

  /**
   * Stores a period of time in days, hours, minutes, and seconds.
   */
  public static final Type INTERVAL_DAY_TO_SECOND =
    new TypeImpl(Duration.class, "INTERVAL DAY TO SECOND");

  /**
   * Stores a period of time in years and months.
   */
  public static final Type INTERVAL_YEAR_TO_MONTH =
    new TypeImpl(Period.class, "INTERVAL YEAR TO MONTH");

  /**
   * Stores a JSON value.
   */
  public static final Type JSON =
    new TypeImpl(OracleJsonObject.class, "JSON");

  /**
   * Character data of variable length up to 2 gigabytes.
   */
  public static final Type LONG =
    new TypeImpl(String.class, "LONG");

  /**
   * Raw binary data of variable length up to 2 gigabytes.
   */
  public static final Type LONG_RAW =
    new TypeImpl(ByteBuffer.class, "LONG RAW");

  /**
   * Base 64 string representing the unique address of a row in its table.
   */
  public static final Type ROWID =
    new TypeImpl(RowId.class, "ROWID");

  /**
   * Timestamp that is converted to the database's timezone when stored, and
   * converted to the local timezone (the session timezone) when retrieved.
   */
  public static final Type TIMESTAMP_WITH_LOCAL_TIME_ZONE =
    new TypeImpl(LocalDateTime.class, "TIMESTAMP WITH LOCAL TIME ZONE");

  /**
   * Implementation of the {@link Type} SPI.
   */
  private static final class TypeImpl implements Type {

    /**
     * The Java Language mapping of this SQL type.
     */
    private final Class<?> javaType;

    /**
     * The name of this SQL type, as it would appear in a DDL expression.
     */
    private final String sqlName;

    /**
     * Constructs a {@code Type} having a {@code javaType} mapping and
     * {@code sqlName}.
     * @param javaType Java type
     * @param sqlName SQL type name
     */
    TypeImpl(Class<?> javaType, String sqlName) {
      this.javaType = javaType;
      this.sqlName = sqlName;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implements the R2DBC SPI method by returning the default Java type
     * mapping for values of this SQL type. The Java type returned by this
     * method is the type of {@code Object} returned by {@code Row.get
     * (String/int)} when accessing a value of this SQL type.
     * </p>
     */
    @Override
    public Class<?> getJavaType() {
      return javaType;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implements the R2DBC SPI method by returning the name of this SQL type.
     * The name returned by this method is recognized in expressions of a SQL
     * command, for instance: A column definition of a {@code CREATE TABLE}
     * command.
     * </p>
     *
     * @return
     */
    @Override
    public String getName() {
      return sqlName;
    }

    /**
     * Returns the name of this type.
     * @return Type name
     */
    @Override
    public String toString() {
      return getName();
    }
  }

}
