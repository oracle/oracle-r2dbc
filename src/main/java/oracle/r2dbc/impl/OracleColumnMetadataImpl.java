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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.util.Map;
import java.util.stream.Collectors;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Type;
import oracle.jdbc.OracleType;
import oracle.jdbc.OracleTypes;

import static java.util.Map.entry;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.getOrHandleSQLException;

/**
 * <p>
 * Implementation of the {@link ColumnMetadata} SPI for Oracle Database.
 * Instances of this class supply metadata values that are derived from a JDBC
 * {@link ResultSetMetaData} object.
 * </p><p>
 * For many SQL types, instances of this class supply metadata values that
 * are the same as JDBC's metadata values. For other SQL types, JDBC and
 * R2DBC specifications have different standards, such as the
 * {@linkplain #getJavaType() Java type mapping}. Instances of this class
 * override JDBC's metadata values when necessary to conform with R2DBC
 * standards.
 * </p><p>
 * This class implements {@link #getNativeTypeMetadata()} to return an
 * instance of {@link SQLType}.
 * </p>
 *
 * @author  harayuanwang, michael-a-mcmahon
 * @since   0.1.0
 */
final class OracleColumnMetadataImpl implements ColumnMetadata {

  /**
   * The number of characters needed to represent an {@link OffsetDateTime}
   * with 9 digits of a fractional second precision. This is the value
   * returned by {@link #getPrecision()} for column types that map to
   * {@code OffsetDateTime}. The 35 character length format is specified by
   * {@link OffsetDateTime#toString()} and
   * {@link OffsetDateTime#parse(CharSequence)} as:
   * <pre>
   * uuuu-MM-ddTHH:mm:ss.SSSSSSSSS+HH:mm
   * </pre>
   */
  private static final int OFFSET_DATE_TIME_PRECISION = 35;

  /**
   * The number of characters needed to represent a {@link LocalDateTime}
   * with 9 digits of a fractional second precision. This is the value
   * returned by {@link #getPrecision()} for column types that map to
   * {@code LocalDateTime}. The 29 character length format is specified by
   * {@link LocalDateTime#toString()} and
   * {@link LocalDateTime#parse(CharSequence)} as:
   * <pre>
   * uuuu-MM-ddTHH:mm:ss.SSSSSSSSS
   * </pre>
   */
  private static final int LOCAL_DATE_TIME_PRECISION = 29;

  /**
   * The number of characters needed to represent a {@link LocalDate}. This is
   * the value returned by {@link #getPrecision()} for column types that map to
   * {@code LocalDate}. The 10 character length format is specified by
   * {@link LocalDate#toString()} and {@link LocalDate#parse(CharSequence)} as:
   * <pre>
   * uuuu-MM-dd
   * </pre>
   */
  private static final int LOCAL_DATE_PRECISION = 10;

  /**
   * The number of characters needed to represent a {@link Period}. This is
   * the value returned by {@link #getPrecision()} for column types that map to
   * {@code Period}. The 10 character length format is specified by
   * {@link Period#toString()} and {@link Period#parse(CharSequence)} as:
   * <pre>
   * PnYnMnD
   * </pre>
   * {@code INTERVAL YEAR TO MONTH} values store a year with a maximum precision
   * of 9 digits, and doesn't store a day. Where 'd' represents a base 10
   * digit, the longest character representation of {@code INTERVAL YEAR TO
   * MONTH} is:
   * <pre>
   * PdddddddddYddM0D
   * </pre>
   */
  private static final int PERIOD_PRECISION = 16;

  /**
   * The number of characters needed to represent a {@link Duration}. This is
   * the value returned by {@link #getPrecision()} for column types that map to
   * {@code Duration}. The 10 character length format is specified by
   * {@link Duration#toString()} and {@link Duration#parse(CharSequence)} as:
   * <pre>
   * PTnHnMnS
   * </pre>
   * {@code INTERVAL DAY TO SECOND} values store a day with a maximum precision
   * of 9 digits, an hour between 0 and 23, a minute between 0 and 59, and a
   * second between 0 and 59.999999999. {@link Duration#toString()} will
   * convert days into hours, and 999,999,999 days = 23,999,999,976 hours.
   * Where 'd' represents a base 10 digit, the longest character
   * representation of {@code INTERVAL DAY TO SECOND} is:
   * <pre>
   * PTdddddddddddHddMdd.dddddddddS
   * </pre>
   */
  private static final int DURATION_PRECISION = 30;

  /**
   * The number of characters needed to represent a {@code ROW ID} as a
   * {@link String} of base 64 characters. This is the value returned by
   * {@link #getPrecision()} for {@code ROW ID} columns. The 18 character
   * length format is specified by the
   * <a href="https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/ROWIDTOCHAR.html#GUID-67998E5B-376A-45B5-B20B-1A87E5D370C1">
   * ROWIDTOCHAR function
   * </a>
   */
  private static final int ROW_ID_PRECISION = 18;

  // TODO: Move maps to OracleR2dbcType?
  /**
   * Mapping of R2DBC {@link io.r2dbc.spi.Type}s to JDBC's {@link SQLType}.
   */
  private static final Map<SQLType, Type> JDBC_TO_R2DBC_TYPE_MAP =
    Map.ofEntries(
      entry(JDBCType.BIGINT, R2dbcType.BIGINT),
      entry(JDBCType.BINARY, R2dbcType.BINARY),
      entry(OracleType.BINARY_DOUBLE, OracleR2dbcType.BINARY_DOUBLE),
      entry(OracleType.BINARY_FLOAT, OracleR2dbcType.BINARY_FLOAT),
      entry(JDBCType.BLOB, OracleR2dbcType.BLOB),
      entry(JDBCType.BOOLEAN, R2dbcType.BOOLEAN),
      entry(JDBCType.CHAR, R2dbcType.CHAR),
      entry(JDBCType.CLOB, OracleR2dbcType.CLOB),
      entry(JDBCType.ARRAY, R2dbcType.COLLECTION),
      entry(JDBCType.DATE, R2dbcType.DATE),
      entry(JDBCType.DECIMAL, R2dbcType.DECIMAL),
      entry(JDBCType.DOUBLE, R2dbcType.DOUBLE),
      entry(JDBCType.FLOAT, R2dbcType.FLOAT),
      entry(JDBCType.INTEGER, R2dbcType.INTEGER),
      entry(
        OracleType.INTERVAL_DAY_TO_SECOND,
        OracleR2dbcType.INTERVAL_DAY_TO_SECOND),
      entry(
        OracleType.INTERVAL_YEAR_TO_MONTH,
        OracleR2dbcType.INTERVAL_YEAR_TO_MONTH),
      entry(JDBCType.LONGVARBINARY, OracleR2dbcType.LONG_RAW),
      entry(JDBCType.LONGVARCHAR, OracleR2dbcType.LONG),
      entry(JDBCType.NCHAR, R2dbcType.NCHAR),
      entry(JDBCType.NCLOB, OracleR2dbcType.NCLOB),
      entry(JDBCType.NUMERIC, R2dbcType.NUMERIC),
      entry(JDBCType.NVARCHAR, R2dbcType.NVARCHAR),
      entry(JDBCType.REAL, R2dbcType.REAL),
      entry(JDBCType.ROWID, OracleR2dbcType.ROW_ID),
      entry(JDBCType.SMALLINT, R2dbcType.SMALLINT),
      entry(JDBCType.TIME, R2dbcType.TIME),
      entry(JDBCType.TIME_WITH_TIMEZONE, R2dbcType.TIME_WITH_TIME_ZONE),
      entry(JDBCType.TIMESTAMP, R2dbcType.TIMESTAMP),
      entry(
        OracleType.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        OracleR2dbcType.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
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
      // TODO Define Oracle Specific types
      // TODO Map character types to NCHAR types by default?
    );

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
    entry(Duration.class, oracle.jdbc.OracleType.INTERVAL_DAY_TO_SECOND),
    entry(Period.class, oracle.jdbc.OracleType.INTERVAL_YEAR_TO_MONTH),
    entry(RowId.class, oracle.jdbc.OracleType.ROWID)

  );

  // Swap Class key and SQLType value
  private static final Map<SQLType, Class<?>> SQL_TO_JAVA_TYPE_MAP =
    JAVA_TO_SQL_TYPE_MAP.entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

  /**
   * The column's R2DBC SQL type.
   */
  private final Type sqlType;

  /**
   * The column's JDBC SQL type.
   */
  private final SQLType jdbcSqlType;

  /**
   * The column's Java type mapping, as returned by {@link Row#get(int)} and
   * {@link Row#get(String)}
   */
  private final Class<?> javaType;

  /**
   * The column's name, as specified in the SQL statement that returned the
   * column.
   */
  private final String name;

  /**
   * The column's nullability, indicating if can have null values or not. It
   * may indicate that it is unknown whether null values are possible.
   */
  private final Nullability nullability;

  /**
   * The column's precision, which indicates the maximum number of significant
   * digits for a numeric type, the maximum number characters for a character
   * type, the maximum number of characters used to represent a datetime type,
   * or null if precision is not applicable to the column's type.
   */
  private final Integer precision;

  /**
   * The column's scale, which indicates the maximum amount of digits to the
   * right of the decimal point for numeric types, or null if scale is not
   * applicable to the column's type.
   */
  private final Integer scale;

  /**
   * Constructs a new instance that supplies column metadata values as
   * specified by parameters to this constructor.
   * @param r2dbcSqlType The column's SQL type.
   * @param javaType The column's Java type mapping. Not null.
   * @param name The column's name, as specified in the SQL statement that
 *             returned the column. Not null.
   * @param nullability The column's nullability. Not the Java Language
   *          {@code null} value. May be any value of {@link Nullability}.
   * @param precision The column's precision, or null if precision is not
*                  applicable to the column's type.
   * @param scale The column's scale, or null if scale is not applicable to
   *              the column's type.
   */
  private OracleColumnMetadataImpl(
    SQLType sqlType, String name, Nullability nullability, Integer precision,
    Integer scale) {
    this.jdbcSqlType = sqlType;
    this.name = name;
    this.nullability = nullability;
    this.precision = precision;
    this.scale = scale;

    this.sqlType = JDBC_TO_R2DBC_TYPE_MAP.get(jdbcSqlType);
    if (this.sqlType == null) // TODO: OracleR2dbcType.UNKNOWN? Future proof
      throw new IllegalStateException("Unrecognized SQL type:" + sqlType);

    this.javaType = this.sqlType.getJavaType();
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning either the Java type
   * described by a JDBC {@code ResultSetMetaData} object or a type that
   * conforms to R2DBC standards.
   * </p>
   */
  @Override
  public Class<?> getJavaType() {
    return javaType;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning the column name described by
   * a JDBC {@code ResultSetMetaData} object.
   * </p>
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning the {@link SQLType} of a
   * column. The returned {@code SQLType} corresponds to the numeric SQL type
   * code described by a JDBC {@code ResultSetMetaData} object.
   * </p>
   * @return The SQL type for a column. Not null.
   */
  @Override
  public SQLType getNativeTypeMetadata() {
    return jdbcSqlType;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning the nullability described by
   * a JDBC {@code ResultSetMetaData} object.
   * </p>
   */
  @Override
  public Nullability getNullability() {
    return nullability;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning the precision described by a
   * JDBC {@code ResultSetMetaData} object. For datetime column types, this
   * implementation returns the length in bytes required to represent the
   * value, assuming the maximum allowed precision of the fractional seconds
   * component, when the value is represented as UTF-8 encoded characters
   * returned by invoking {@link #toString()} on an instance of the datetime
   * type's {@linkplain #getJavaType() default Java type mapping}.
   * </p>
   */
  @Override
  public Integer getPrecision() {
    return precision;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning the scale supplied from a JDBC
   * {@code ResultSetMetaData} object. For datetime column types, this
   * implementation returns the number of decimal digits in the fractional
   * seconds component.
   * </p>
   */
  @Override
  public Integer getScale() {
    return scale;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning the R2DBC {@code Type}.
   * </p>
   * @return
   */
  @Override
  public Type getType() {
    return sqlType;
  }

  /**
   * <p>
   * Returns a new instance of {@code ColumnMetaData} with metadata values
   * derived from a JDBC {@code ResultSetMetaData} object. The returned
   * {@code ColumnMetaData} supplies values for the column at the specified
   * 0-based {@code index}.
   * </p><p>
   * This method handles cases where JDBC and R2DBC standards diverge,
   * returning a metadata object that conforms with R2DBC standards.
   * </p>
   * @param resultSetMetaData ResultSet metadata from JDBC
   * @param index 0-based column index
   * @return {@code ColumnMetaData} for the parameer at the specified  index
   */
  static OracleColumnMetadataImpl fromResultSetMetaData(
    ResultSetMetaData resultSetMetaData, int index) {
    int jdbcIndex = index + 1;
    return getOrHandleSQLException(() -> fromJdbc(
      getSqlType(resultSetMetaData.getColumnType(jdbcIndex)),
      resultSetMetaData.getColumnName(jdbcIndex),
      getNullability(resultSetMetaData.isNullable(jdbcIndex)),
      resultSetMetaData.getPrecision(jdbcIndex),
      resultSetMetaData.getScale(jdbcIndex)));
  }

  static OracleColumnMetadataImpl create(String name, SQLType type) {
    return new OracleColumnMetadataImpl(
      type, name, Nullability.NULLABLE, null, null);
  }

  /**
   * <p>
   * Returns the JDBC {@code SQLType} corresponding to an {@code r2dbcType};
   * Returns {@link JDBCType#VARCHAR} for {@link R2dbcType#VARCHAR},
   * {@link JDBCType#DATE} for {@link R2dbcType#DATE}, etc.
   * </p>
   * @param r2dbcType An R2DBC type
   * @return A JDBC type, or {@code null} if {@code r2dbcType} is not
   * recognized by Oracle R2DBC.
   */
  static SQLType r2dbcToSQLType(Type r2dbcType) {
    return R2DBC_TO_JDBC_TYPE_MAP.get(r2dbcType);
  }

  static SQLType javaToSQLType(Class<?> javaType) {
    SQLType sqlType = JAVA_TO_SQL_TYPE_MAP.get(javaType);

    if (sqlType != null) {
      return sqlType;
    }
    else {
      // Attempt to find a supported super-type of the object
      return JAVA_TO_SQL_TYPE_MAP.entrySet()
        .stream()
        .filter(entry -> entry.getKey().isAssignableFrom(javaType))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElseThrow(() ->
          new IllegalArgumentException("Unsupported Java type:" + javaType));
    }
  }

  private static OracleColumnMetadataImpl fromJdbc(
    SQLType type, String name, Nullability nullability, int precision,
    int scale) {

    switch (type.getVendorTypeNumber()) {
      case Types.BLOB:
        return createBlobMetaData(name, nullability);
      case Types.CLOB:
      case Types.NCLOB:
        return createClobMetaData(type, name, nullability);
      case Types.DATE:
        return createDateMetaData(name, nullability);
      case Types.TIMESTAMP:
      case OracleTypes.TIMESTAMPLTZ:
        return createTimestampMetadata(type, name, nullability, scale);
      case Types.TIMESTAMP_WITH_TIMEZONE:
      case OracleTypes.TIMESTAMPTZ:
        return createTimestampWithTimeZoneMetadata(name, nullability, scale) ;
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return createBinaryMetaData(type, name, nullability, precision);
      case OracleTypes.JSON:
        return createJsonMetaData(name, nullability);
      default:
        // Return metadata with the same values as JDBC.
        return new OracleColumnMetadataImpl(type, name, nullability,
          // JDBC returns 0 when precision or scale is not applicable
          precision == 0 ? null : precision,
          scale == 0 ? null : scale);
    }
  }

  /**
   * Override JDBC's java.sql.Blob mapping to use io.r2dbc.spi.Blob,
   * and to use an appropriate precision value
   * (OracleResultSetMetaData.getPrecision(int) returns -1 for BLOB)
   * Not supporting the ByteBuffer type mapping guideline. See
   * OracleRowImpl.get(int/String)
   * Note: The actual precision is (4GB x Database-Block-Size), which
   *  exceeds Integer.MAX_VALUE. R2DBC SPI uses the Integer type for
   *  precision values, so using the null value is used for now.
   */
  private static OracleColumnMetadataImpl createBlobMetaData(
    String name, Nullability nullability) {
    return new OracleColumnMetadataImpl(
      JDBCType.BLOB, name, nullability, null, null);
  }

  /**
   * Override JDBC's java.sql.Clob and java.sql.NClob mapping to use
   * io.r2dbc.spi.Clob, and to use an appropriate precision value
   * (OracleResultSetMetaData.getPrecision(int) returns -1 for CLOB)
   * Not supporting the String type mapping guideline. See
   * OracleRowImpl.get(int/String)
   * Note: The actual precision is (4GB x Database-Block-Size), which
   *  exceeds Integer.MAX_VALUE. R2DBC SPI uses the Integer type for
   *  precision values, so using the null value is used for now.
   */
  private static OracleColumnMetadataImpl createClobMetaData(
    SQLType sqlType, String name, Nullability nullability) {
    return new OracleColumnMetadataImpl(
      sqlType, name, nullability, null, null);
  }

  /** Override JDBC's java.sql.Date type mapping to use LocalDate */
  private static OracleColumnMetadataImpl createDateMetaData(
    String name, Nullability nullability) {
    return new OracleColumnMetadataImpl(
      JDBCType.DATE, name, nullability,
      LOCAL_DATE_PRECISION, null);
  }

  /**
   * Override JDBC's java.sql.Timestamp type mapping, and Oracle JDBC's
   * oracle.sql.TIMESTAMPLTZ type mapping to use LocalDateTime, and to
   * use an appropriate precision value. Note that the Oracle type
   * named "DATE" is equivalent to a TIMESTAMP with 0 digits of fractional
   * second precision, so OracleResultSetMetaData.getColumnType(int)
   * returns TIMESTAMP for Oracle's "DATE" columns.
   */
  private static OracleColumnMetadataImpl createTimestampMetadata(
    SQLType type, String name, Nullability nullability, int scale) {
    return new OracleColumnMetadataImpl(
      type, name, nullability, LOCAL_DATE_TIME_PRECISION, scale);
  }

  /**
   * The JDBC 4.3 Specification does not specify a TIMESTAMP WITH
   * TIMEZONE mapping. Override JDBC's type mapping, and Oracle JDBC's
   * oracle.sql.TIMESTAMPLTZ type mapping to use OffsetDateTime, and to
   * use an appropriate precision value.
   */
  private static OracleColumnMetadataImpl createTimestampWithTimeZoneMetadata(
    String name, Nullability nullability, int scale) {
    return new OracleColumnMetadataImpl(
      JDBCType.TIMESTAMP_WITH_TIMEZONE, name,
      nullability, OFFSET_DATE_TIME_PRECISION, scale);
  }

  /**
   * Override JDBC's byte[] type mapping to use ByteBuffer. Work around
   * OracleResultSetMetaData implementation of getPrecision(int), which
   * returns 0 for RAW type columns, but implements
   * getColumnDisplaySize(int) method to return the RAW column's max
   * length in bytes.
   * TODO: Push this logic into the OracleReactiveJdbcAdapter?
   */
  private static OracleColumnMetadataImpl createBinaryMetaData(
    SQLType sqlType, String name, Nullability nullability, int precision) {
    return new OracleColumnMetadataImpl(
      sqlType, name, nullability, precision, null);
  }

  /**
   * Override Oracle JDBC's oracle.sql.INTERVALYM type mapping to use
   * Period
   */
  private static OracleColumnMetadataImpl createIntervalYearToMonthMetaData(
    String name, Nullability nullability, int precision) {
    return new OracleColumnMetadataImpl(
      oracle.jdbc.OracleType.INTERVAL_YEAR_TO_MONTH, name, nullability,
      precision, null);
  }

  /**
   * Override Oracle JDBC's oracle.sql.INTERVALDS type mapping to use
   * Duration
   */
  private static OracleColumnMetadataImpl createIntervalDayToSecondMetaData(
    String name, Nullability nullability, int precision) {
    return new OracleColumnMetadataImpl(
      oracle.jdbc.OracleType.INTERVAL_DAY_TO_SECOND, name, nullability,
      precision, null);
  }

  /**
   * Override JDBC's javax.json.JsonValue type mapping to use
   *  OracleJsonValue. Set the precision as null.
   *  OracleResultSetMetaData.getPrecision(int) returns -1 for JSON
   *  TODO: Or require the R2DBC application to have javax.json API on
   *    classpath? Need to make a decision before 1.0.0 release
   */
  private static OracleColumnMetadataImpl createJsonMetaData(
    String name, Nullability nullability) {
    return new OracleColumnMetadataImpl(
      oracle.jdbc.OracleType.JSON, name, nullability, null, null);
  }

  /**
   * <p>
   * Returns a {@link SQLType} that having the type number returned by
   * {@link ResultSetMetaData#getColumnType(int)} at a 1-based column
   * index specified as {@code jdbcIndex}
   * </p><p>
   * A {@link JDBCType} is returned if one is found to have a matching type
   * number. Otherwise, an {@link OracleR2dbcType} is returned if one is found to
   * have a matching type number. Otherwise, {@link JDBCType#OTHER} is
   * returned if no {@link SQLType} is found to have a matching type number.
   * </p>
   * @param resultSetMetaData JDBC metadata
   * @param jdbcIndex 1-based index of a column
   * @return A {@link SQLType} having the type number of the specified column.
   */
  private static SQLType getSqlType(int typeNumber) {

    for (JDBCType jdbcType : JDBCType.values()) {
      Integer vendorTypeNumber = jdbcType.getVendorTypeNumber();
      if (vendorTypeNumber != null && vendorTypeNumber == typeNumber)
        return jdbcType;
    }

    try {
      oracle.jdbc.OracleType oracleType =
        oracle.jdbc.OracleType.toOracleType(typeNumber);
      return oracleType != null ? oracleType : JDBCType.OTHER;
    }
    catch (SQLException typeNotFound) {
      return JDBCType.OTHER;
    }
  }

  /**
   * Returns the {@code Nullability} of a {@code ResultSetMetaData} column at
   * the 1-based index specified by {@code jdbcIndex}. This method maps the
   * {@code int} constant returned by {@link ResultSetMetaData#isNullable(int)}
   * to the equivalent {@link Nullability} enum value.
   * @param resultSetMetaData Indicates nullability for a row of columns.
   * @param jdbcIndex 1-based index of a column
   * @return The nullability of the specified column.
   * @throws R2dbcException If a database access error occurs
   */
  private static Nullability getNullability(int jdbcNullability) {
    switch (jdbcNullability) {
      case ResultSetMetaData.columnNoNulls:
        return Nullability.NON_NULL;
      case ResultSetMetaData.columnNullable:
        return Nullability.NULLABLE;
      case ResultSetMetaData.columnNullableUnknown:
      default:
        return Nullability.UNKNOWN;
    }
  }

  /**
   * Returns the {@code Class} identified by {@code className}.
   * @param className A class name. May be null.
   * @return {@code Class} identified by {@code className}, or
   * {@code Object.class} if {@code className} is {@code null}
   * @throws IllegalStateException If the identified {@code Class} can not be
   * found.
   */
  private static Class<?> getClass(String className) {
    try {
      return className == null ? Object.class : Class.forName(className);
    }
    catch (ClassNotFoundException columnClassNotFound) {
      throw new IllegalStateException(
        "Could not find Java class for ColumnMetaData",
        columnClassNotFound);
    }
  }

}