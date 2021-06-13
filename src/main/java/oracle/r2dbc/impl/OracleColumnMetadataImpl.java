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

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Type;
import oracle.jdbc.OracleTypes;
import oracle.r2dbc.OracleR2dbcTypes;

import static oracle.r2dbc.impl.OracleR2dbcExceptions.getOrHandleSQLException;
import static oracle.r2dbc.impl.SqlTypeMap.toJdbcType;
import static oracle.r2dbc.impl.SqlTypeMap.toR2dbcType;

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
   * The column's R2DBC SQL type.
   */
  private final Type r2dbcType;

  /**
   * The column's JDBC SQL type.
   */
  private final SQLType jdbcType;

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
   * @param jdbcType The column's SQL type.
   * @param name The column's name, as specified in the SQL statement that
 *             returned the column. Not null.
   * @param nullability The column's nullability. Not the Java Language
   *          {@code null} value. May be any value of {@link Nullability}.
   * @param precision The column's precision, or null if precision is not
*                  applicable to the column's type.
   * @param scale The column's scale, or null if scale is not applicable to
   *              the column's type.
   * @throws IllegalStateException If the {@code jdbcType} is not recognized
   */
  private OracleColumnMetadataImpl(
    SQLType jdbcType, String name, Nullability nullability, Integer precision,
    Integer scale) {
    this.jdbcType = jdbcType;
    this.name = name;
    this.nullability = nullability;
    this.precision = precision;
    this.scale = scale;

    this.r2dbcType = toR2dbcType(jdbcType);
    if (this.r2dbcType == null)
      throw new IllegalStateException("Unrecognized SQL type:" + jdbcType);

    this.javaType = r2dbcType.getJavaType();
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
    return jdbcType;
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
    return r2dbcType;
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
    SQLType jdbcType = getSqlType(getOrHandleSQLException(() ->
      resultSetMetaData.getColumnType(jdbcIndex)));

    return getOrHandleSQLException(() -> fromJdbc(jdbcType,
      resultSetMetaData.getColumnName(jdbcIndex),
      getNullability(resultSetMetaData.isNullable(jdbcIndex)),
      jdbcType.equals(JDBCType.VARBINARY) // Oracle JDBC implementation
        ? resultSetMetaData.getColumnDisplaySize(jdbcIndex)
        : resultSetMetaData.getPrecision(jdbcIndex),
      resultSetMetaData.getScale(jdbcIndex)));
  }

  /**
   * Creates {@code ColumnMetadata} for an out parameter. The returned
   * {@code ColumnMetaData} has a specified {@code name} and {@code type}.
   * The Java type mapping of the returned metadata is obtained by invoking
   * {@link Type#getJavaType()} on {@code type}. The nullablity is
   * {@link Nullability#NULLABLE}, as all PL/SQL out parameters may be {@code
   * null}.
   * @param name Column name
   * @param type Column type
   * @return Column metadata having {@code name} and {@code type}
   */
  static OracleColumnMetadataImpl createParameterMetadata(
    String name, Type type) {
    return new OracleColumnMetadataImpl(
      toJdbcType(type), name, Nullability.NULLABLE, null, null);
  }

  /**
   * Creates {@code ColumnMetadata} from values of a JDBC
   * {@code ResultSetMetaDataObject}.
   * @param type SQL type of a column. Not null.
   * @param name Name of a column. Not null.
   * @param nullability Nullability of a column. Not null.
   * @param precision Precision of a column, or {@code 0} if precision is not
   * applicable to the column type.
   * @param scale Scale of a column, or {@code 0} if scale is not applicable
   * to the column type.
   * @return {@code ColumnMetadata} derived from JDBC values. Not null.
   */
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
   * number. Otherwise, an {@link OracleR2dbcTypes} is returned if one is found to
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

}