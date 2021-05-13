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

import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Type;
import oracle.jdbc.OracleType;
import oracle.sql.json.OracleJsonObject;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.RowId;
import java.sql.SQLType;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Map.entry;

/**
 * SQL types supported by Oracle Database that are not defined as standard types
 * by {@link io.r2dbc.spi.R2dbcType}.
 */
final class OracleR2dbcTypes {

  private OracleR2dbcTypes() {}

  /**
   * A 64-bit, double-precision floating-point number data type.
   */
  static final Type BINARY_DOUBLE = new TypeImpl(Double.class, "BINARY_DOUBLE");

  /**
   * A 32-bit, single-precision floating-point number data type.
   */
  static final Type BINARY_FLOAT = new TypeImpl(Float.class, "BINARY_FLOAT");

  /**
   * A Binary Large Object (BLOB) as implemented by Oracle Database. The default
   * Java type mapping is {@link io.r2dbc.spi.Blob} rather than
   * {@link java.nio.ByteBuffer}, which is the mapping of the standard
   * {@link io.r2dbc.spi.R2dbcType#BLOB}.
   */
  static final Type BLOB = new TypeImpl(io.r2dbc.spi.Blob.class, "BLOB");

  /**
   * A Character Large Object (BLOB) as implemented by Oracle Database. The
   * default Java type mapping is {@link io.r2dbc.spi.Clob} rather than
   * {@link String}, which is the mapping of the standard
   * {@link io.r2dbc.spi.R2dbcType#CLOB}.
   */
  static final Type CLOB = new TypeImpl(io.r2dbc.spi.Clob.class, "CLOB");

  /**
   * Stores a period of time in days, hours, minutes, and seconds.
   */
  static final Type INTERVAL_DAY_TO_SECOND =
    new TypeImpl(Duration.class, "INTERVAL DAY TO SECOND");

  /**
   * Stores a period of time in years and months.
   */
  static final Type INTERVAL_YEAR_TO_MONTH =
    new TypeImpl(Period.class, "INTERVAL YEAR TO MONTH");

  /**
   * Character data of variable length up to 2 gigabytes.
   */
  static final Type LONG = new TypeImpl(String.class, "LONG");

  /**
   * Raw binary data of variable length up to 2 gigabytes.
   */
  static final Type LONG_RAW = new TypeImpl(ByteBuffer.class, "LONG RAW");

  /**
   * A National Character Large Object (NCLOB) as implemented by Oracle
   * Database. The default Java type mapping is {@link io.r2dbc.spi.Clob}
   * rather than {@link String}, which is the mapping of the standard
   * {@link io.r2dbc.spi.R2dbcType#NCLOB}.
   */
  static final Type NCLOB = new TypeImpl(io.r2dbc.spi.Clob.class, "NCLOB");

  /**
   * Base 64 string representing the unique address of a row in its table.
   */
  static final Type ROW_ID = new TypeImpl(RowId.class, "ROWID");

  /**
   * Timestamp that is converted to the database's timezone when stored, and
   * converted to the local timezone (the session timezone) when retrieved.
   */
  static final Type TIMESTAMP_WITH_LOCAL_TIME_ZONE =
    new TypeImpl(LocalDateTime.class, "TIMESTAMP WITH LOCAL TIME ZONE");

  /**
   * Mapping of JDBC's {@link SQLType} to R2DBC {@link io.r2dbc.spi.Type}s.
   */
  private static final Map<SQLType, Type> JDBC_TO_R2DBC_TYPE_MAP =
    Map.ofEntries(
      entry(JDBCType.BIGINT, R2dbcType.BIGINT),
      entry(JDBCType.BINARY, R2dbcType.BINARY),
      entry(OracleType.BINARY_DOUBLE, OracleR2dbcTypes.BINARY_DOUBLE),
      entry(OracleType.BINARY_FLOAT, OracleR2dbcTypes.BINARY_FLOAT),
      entry(JDBCType.BLOB, OracleR2dbcTypes.BLOB),
      entry(JDBCType.BOOLEAN, R2dbcType.BOOLEAN),
      entry(JDBCType.CHAR, R2dbcType.CHAR),
      entry(JDBCType.CLOB, OracleR2dbcTypes.CLOB),
      entry(JDBCType.ARRAY, R2dbcType.COLLECTION),
      entry(JDBCType.DATE, R2dbcType.DATE),
      entry(JDBCType.DECIMAL, R2dbcType.DECIMAL),
      entry(JDBCType.DOUBLE, R2dbcType.DOUBLE),
      entry(JDBCType.FLOAT, R2dbcType.FLOAT),
      entry(JDBCType.INTEGER, R2dbcType.INTEGER),
      entry(
        OracleType.INTERVAL_DAY_TO_SECOND,
        OracleR2dbcTypes.INTERVAL_DAY_TO_SECOND),
      entry(
        OracleType.INTERVAL_YEAR_TO_MONTH,
        OracleR2dbcTypes.INTERVAL_YEAR_TO_MONTH),
      entry(JDBCType.LONGVARBINARY, OracleR2dbcTypes.LONG_RAW),
      entry(JDBCType.LONGVARCHAR, OracleR2dbcTypes.LONG),
      entry(JDBCType.NCHAR, R2dbcType.NCHAR),
      entry(JDBCType.NCLOB, OracleR2dbcTypes.NCLOB),
      entry(JDBCType.NUMERIC, R2dbcType.NUMERIC),
      entry(JDBCType.NVARCHAR, R2dbcType.NVARCHAR),
      entry(JDBCType.REAL, R2dbcType.REAL),
      entry(JDBCType.ROWID, OracleR2dbcTypes.ROW_ID),
      entry(JDBCType.SMALLINT, R2dbcType.SMALLINT),
      entry(JDBCType.TIME, R2dbcType.TIME),
      entry(JDBCType.TIME_WITH_TIMEZONE, R2dbcType.TIME_WITH_TIME_ZONE),
      entry(JDBCType.TIMESTAMP, R2dbcType.TIMESTAMP),
      entry(
        OracleType.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        OracleR2dbcTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
      entry(
        JDBCType.TIMESTAMP_WITH_TIMEZONE,
        new Type() { // This is a placeholder
          @Override
          public Class<?> getJavaType() {
            return OffsetDateTime.class;
          }

          @Override
          public String getName() {
            return "TIMESTAMP WITH TIME ZONE";
          }
        }),
      // TODO: Replace above with:
      // R2dbcType.TIMESTAMP_WITH_TIME_ZONE),
      // Needs Fix:
      // https://github.com/r2dbc/r2dbc-spi/commit/a86562421a312df2d8a3ae187553bf6c2b291aad

      entry(JDBCType.TINYINT, R2dbcType.TINYINT),
      entry(JDBCType.VARBINARY, R2dbcType.VARBINARY),
      entry(JDBCType.VARCHAR, R2dbcType.VARCHAR)
    );

  /**
   * Mapping of R2DBC {@link io.r2dbc.spi.Type}s to JDBC's {@link SQLType}.
   */
  private static final Map<Type, SQLType> R2DBC_TO_JDBC_TYPE_MAP =
    // Swap R2DBC key and JDBC value
    JDBC_TO_R2DBC_TYPE_MAP.entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

  /**
   * Mapping of Java classes to JDBC's {@link SQLType}.
   */
  private static final Map<Class<?>, SQLType> JAVA_TO_SQL_TYPE_MAP =
    Map.ofEntries(
      // Standard mappings listed in the R2DBC Specification
      entry(String.class, JDBCType.VARCHAR),
      entry(Boolean.class, JDBCType.BOOLEAN),
      entry(ByteBuffer.class, JDBCType.VARBINARY),
      entry(Integer.class, JDBCType.INTEGER),
      entry(Byte.class, JDBCType.TINYINT),
      entry(Short.class, JDBCType.SMALLINT),
      entry(Long.class, JDBCType.BIGINT),
      entry(BigDecimal.class, JDBCType.NUMERIC),
      entry(Float.class, JDBCType.REAL),
      entry(Double.class, JDBCType.DOUBLE),
      entry(LocalDate.class, JDBCType.DATE),
      entry(LocalTime.class, JDBCType.TIME),
      entry(OffsetTime.class, JDBCType.TIME_WITH_TIMEZONE),
      entry(LocalDateTime.class, JDBCType.TIMESTAMP),
      entry(OffsetDateTime.class, JDBCType.TIMESTAMP_WITH_TIMEZONE),
      entry(io.r2dbc.spi.Blob.class, JDBCType.BLOB),
      entry(io.r2dbc.spi.Clob.class, JDBCType.CLOB),

      // Extended mappings supported by Oracle
      entry(Duration.class, OracleType.INTERVAL_DAY_TO_SECOND),
      entry(Period.class, OracleType.INTERVAL_YEAR_TO_MONTH),
      entry(RowId.class, OracleType.ROWID),
      entry(OracleJsonObject.class, OracleType.JSON)

    );

  /**
   * Returns the R2DBC {@code Type} identifying the same SQL type as an JDBC
   * {@code SQLType}, or {@code null} if no R2DBC {@code Type} is known to
   * identify same SQL type as the {@code jdbcType}.
   * @param jdbcType A JDBC SQL type
   * @return An R2DBC SQL type
   */
  static Type toR2dbcType(SQLType jdbcType) {
    return JDBC_TO_R2DBC_TYPE_MAP.get(jdbcType);
  }

  /**
   * Returns the JDBC {@code SQLType} identifying the same SQL type as an
   * R2DBC {@code Type}, or {@code null} if no JDBC {@code SQLType} is known to
   * identify same SQL type as the {@code r2dbcType}.
   * @param r2dbcType An R2DBC SQL type
   * @return A JDBC SQL type
   */
  static SQLType toJdbcType(Type r2dbcType) {
    return r2dbcType instanceof Type.InferredType
      ? toJdbcType(r2dbcType.getJavaType())
      : R2DBC_TO_JDBC_TYPE_MAP.get(r2dbcType);
  }

  /**
   * <p>
   * Returns the JDBC {@code SQLType} identifying the default SQL type
   * mapping for a {@code javaType}, or {@code null} if
   * {@code javaType} has no SQL type mapping.
   * </p><p>
   * The type returned by this method is derived from the the R2DBC
   * Specification's SQL to Java type mappings. Where the specification
   * defines a Java type that maps to a single SQL type, this method returns
   * that SQL type. Where the specification defines a Java type that maps to
   * multiple SQL types, the return value of this method is as follows:
   * <ul>
   *   <li>String -> VARCHAR</li>
   *   <li>ByteBuffer -> VARBINARY</li>
   * </ul>
   * This method returns non-standard SQL types supported by Oracle
   * Database for the following Java types:
   * <ul>
   *   <li>Double -> BINARY_DOUBLE</li>
   *   <li>Float -> BINARY_FLOAT</li>
   *   <li>Duration -> INTERVAL DAY TO SECOND</li>
   *   <li>Period -> INTERVAL YEAR TO MONTH</li>
   *   <li>{@link RowId} -> ROWID</li>
   *   <li>{@link OracleJsonObject} -> JSON</li>
   * </ul>
   * @param javaType Java type to map
   * @return SQL type mapping for the {@code javaType}
   */
  static SQLType toJdbcType(Class<?> javaType) {
    SQLType sqlType = JAVA_TO_SQL_TYPE_MAP.get(javaType);

    if (sqlType != null) {
      return sqlType;
    }
    else {
      // Search for a mapping of the object's super-type
      return JAVA_TO_SQL_TYPE_MAP.entrySet()
        .stream()
        .filter(entry -> entry.getKey().isAssignableFrom(javaType))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse(null);
    }
  }

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
  }

}
