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
import oracle.r2dbc.OracleR2dbcTypes;
import oracle.sql.json.OracleJsonObject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.RowId;
import java.sql.SQLException;
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
 * Defines the SQL to Java type mappings used by Oracle R2DBC.
 */
final class SqlTypeMap {

  /**
   * Mapping of JDBC's {@link SQLType} to R2DBC {@link io.r2dbc.spi.Type}s.
   */
  private static final Map<SQLType, Type> JDBC_TO_R2DBC_TYPE_MAP =
    Map.ofEntries(
      entry(JDBCType.BIGINT, R2dbcType.BIGINT),
      entry(JDBCType.BINARY, R2dbcType.BINARY),
      entry(OracleType.BINARY_DOUBLE, OracleR2dbcTypes.BINARY_DOUBLE),
      entry(OracleType.BINARY_FLOAT, OracleR2dbcTypes.BINARY_FLOAT),
      entry(JDBCType.BLOB, R2dbcType.BLOB),
      entry(JDBCType.BOOLEAN, R2dbcType.BOOLEAN),
      entry(JDBCType.CHAR, R2dbcType.CHAR),
      entry(JDBCType.CLOB, R2dbcType.CLOB),
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
      entry(JDBCType.NCLOB, R2dbcType.NCLOB),
      entry(JDBCType.NUMERIC, R2dbcType.NUMERIC),
      entry(JDBCType.NVARCHAR, R2dbcType.NVARCHAR),
      entry(JDBCType.REAL, R2dbcType.REAL),
      entry(JDBCType.ROWID, OracleR2dbcTypes.ROWID),
      entry(JDBCType.SMALLINT, R2dbcType.SMALLINT),
      entry(JDBCType.TIME, R2dbcType.TIME),
      entry(JDBCType.TIME_WITH_TIMEZONE, R2dbcType.TIME_WITH_TIME_ZONE),
      entry(JDBCType.TIMESTAMP, R2dbcType.TIMESTAMP),
      entry(
        OracleType.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        OracleR2dbcTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
      entry(
        // Map the OracleType value, which has a different type code than the
        // JDBCType value.
        OracleType.TIMESTAMP_WITH_TIME_ZONE,
        R2dbcType.TIMESTAMP_WITH_TIME_ZONE),
      entry(JDBCType.TINYINT, R2dbcType.TINYINT),
      entry(JDBCType.VARBINARY, R2dbcType.VARBINARY),
      entry(JDBCType.VARCHAR, R2dbcType.VARCHAR)
    );

  /**
   * Mapping of R2DBC {@link Type}s to JDBC's {@link SQLType}.
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

      // JDBC 4.3 mappings not included in R2DBC Specification. Types like
      // java.sql.Blob/Clob/NClob/Array can be accessed from Row.get(...)
      // and then used as bind values, so these types are included.
      // TODO: JDBC 4.3 lists a "Java class" to JAVA_OBJECT mapping. It's not
      //  clear what that is for, or if Oracle JDBC supports it. That mapping
      //  is not included here.
      entry(byte[].class, JDBCType.VARBINARY),
      entry(BigInteger.class, JDBCType.BIGINT),
      entry(java.sql.Date.class, JDBCType.DATE),
      entry(java.sql.Time.class, JDBCType.TIME),
      entry(java.sql.Timestamp.class, JDBCType.TIMESTAMP),
      entry(java.sql.Array.class, JDBCType.ARRAY),
      entry(java.sql.Blob.class, JDBCType.BLOB),
      entry(java.sql.Clob.class, JDBCType.CLOB),
      entry(java.sql.Struct.class, JDBCType.STRUCT),
      entry(java.sql.Ref.class, JDBCType.REF),
      entry(java.net.URL.class, JDBCType.DATALINK),
      entry(java.sql.RowId.class, JDBCType.ROWID),
      entry(java.sql.NClob.class, JDBCType.NCLOB),
      entry(java.sql.SQLXML.class, JDBCType.SQLXML),
      entry(java.util.Calendar.class, JDBCType.TIMESTAMP),
      entry(java.util.Date.class, JDBCType.TIMESTAMP),

      // Extended mappings supported by Oracle
      entry(Duration.class, OracleType.INTERVAL_DAY_TO_SECOND),
      entry(Period.class, OracleType.INTERVAL_YEAR_TO_MONTH),
      entry(OracleJsonObject.class, OracleType.JSON)

    );

  /**
   * Returns the R2DBC {@code Type} identifying the same SQL type as an JDBC
   * {@code SQLType}, or {@code null} if no R2DBC {@code Type} is known to
   * identify same SQL type as the {@code jdbcType}.
   * @param jdbcType A JDBC SQL type
   * @return An R2DBC SQL type. May be null.
   */
  static Type toR2dbcType(SQLType jdbcType) {
    return JDBC_TO_R2DBC_TYPE_MAP.get(jdbcType);
  }

  /**
   * Returns the R2DBC {@code Type} identifying the same SQL type as a JDBC
   * type number, or {@code null} if no R2DBC {@code Type} is known to
   * identify same SQL type as the {@code jdbcTypeNumber}.
   * @param jdbcTypeNumber A JDBC type number
   * @return An R2DBC SQL type. May be null.
   */
  static Type toR2dbcType(int jdbcTypeNumber) {

    // Search for a JDBCType with a matching type number
    for (JDBCType jdbcType : JDBCType.values()) {
      Integer vendorTypeNumber = jdbcType.getVendorTypeNumber();

      if (vendorTypeNumber != null && vendorTypeNumber == jdbcTypeNumber)
        return toR2dbcType(jdbcType);
    }

    // If no JDBCType matches, search for a matching OracleType
    try {
      OracleType oracleType =
        oracle.jdbc.OracleType.toOracleType(jdbcTypeNumber);

      if (oracleType != null)
        return toR2dbcType(oracleType);
      else
        return null;
    }
    catch (SQLException typeNotFound) {
      // toOracleType is specified to throw SQLException if no match is found
      return null;
    }
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
   *   <li>{@link String} -> VARCHAR</li>
   *   <li>{@link ByteBuffer} -> VARBINARY</li>
   * </ul>
   * This method returns non-standard SQL types supported by Oracle
   * Database for the following Java types:
   * <ul>
   *   <li>{@link Double} -> BINARY_DOUBLE</li>
   *   <li>{@link Float} -> BINARY_FLOAT</li>
   *   <li>{@link Duration} -> INTERVAL DAY TO SECOND</li>
   *   <li>{@link Period} -> INTERVAL YEAR TO MONTH</li>
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
}
