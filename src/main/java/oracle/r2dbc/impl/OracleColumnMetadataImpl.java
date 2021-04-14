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

import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Period;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Type;
import oracle.jdbc.OracleType;
import oracle.jdbc.OracleTypes;
import oracle.sql.json.OracleJsonObject;

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
   * The column's SQL type.
   */
  private final SQLType sqlType;

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
   * @param sqlType The column's SQL type.
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
    SQLType sqlType, Class<?> javaType, String name, Nullability nullability,
    Integer precision, Integer scale) {
    this.sqlType = sqlType;
    this.javaType = javaType;
    this.name = name;
    this.nullability = nullability;
    this.precision = precision;
    this.scale = scale;
  }

  /**
   * <p>
   * Returns a new instance of {@code ColumnMetaData} with metadata values
   * derived from a JDBC {@code ResultSetMetaData} object. The returned
   * metadata object supplies values for the column at the specified
   * 0-based {@code index}.
   * </p><p>
   * This method handles cases where JDBC and R2DBC standards diverge,
   * returning a metadata object that conforms with R2DBC standards.
   * </p>
   *
   * @param resultSetMetaData Column metadata from JDBC
   * @param index 0-based column index
   * @return Metadata for the column at the specified index
   */
  static OracleColumnMetadataImpl fromResultSetMetaData(
    ResultSetMetaData resultSetMetaData, int index) {
    int jdbcIndex = index + 1;
    int sqlTypeCode = getOrHandleSQLException(() ->
      resultSetMetaData.getColumnType(jdbcIndex));

    switch (sqlTypeCode) {

      case Types.BLOB:
        // Override JDBC's java.sql.Blob mapping to use io.r2dbc.spi.Blob,
        // and to use an appropriate precision value
        // (OracleResultSetMetaData.getPrecision(int) returns -1 for BLOB)
        // Not supporting the ByteBuffer type mapping guideline. See
        // OracleRowImpl.get(int/String)
        // Note: The actual precision is (4GB x Database-Block-Size), which
        //  exceeds Integer.MAX_VALUE. R2DBC SPI uses the Integer type for
        //  precision values, so using the null value is used for now.
        return newMetadata(Blob.class, null, resultSetMetaData, jdbcIndex);

      case Types.CLOB:
      case Types.NCLOB:
        // Override JDBC's java.sql.Clob and java.sql.NClob mapping to use
        // io.r2dbc.spi.Clob, and to use an appropriate precision value
        // (OracleResultSetMetaData.getPrecision(int) returns -1 for CLOB)
        // Not supporting the String type mapping guideline. See
        // OracleRowImpl.get(int/String)
        // Note: The actual precision is (4GB x Database-Block-Size), which
        //  exceeds Integer.MAX_VALUE. R2DBC SPI uses the Integer type for
        //  precision values, so using the null value is used for now.
        return newMetadata(Clob.class, null, resultSetMetaData, jdbcIndex);

      case Types.DATE:
        // Override JDBC's java.sql.Date type mapping to use LocalDate
        return newMetadata(LocalDate.class, resultSetMetaData, jdbcIndex);

      case Types.TIMESTAMP:
      case OracleTypes.TIMESTAMPLTZ:
        // Override JDBC's java.sql.Timestamp type mapping, and Oracle JDBC's
        // oracle.sql.TIMESTAMPLTZ type mapping to use LocalDateTime, and to
        // use an appropriate precision value. Note that the Oracle type
        // named "DATE" is equivalent to a TIMESTAMP with 0 digits of fractional
        // second precision, so OracleResultSetMetaData.getColumnType(int)
        // returns TIMESTAMP for Oracle's "DATE" columns.
        return newMetadata(LocalDateTime.class, LOCAL_DATE_TIME_PRECISION,
          resultSetMetaData, jdbcIndex);

      case Types.TIMESTAMP_WITH_TIMEZONE:
      case OracleTypes.TIMESTAMPTZ:
        // The JDBC 4.3 Specification does not specify a TIMESTAMP WITH
        // TIMEZONE mapping. Override JDBC's type mapping, and Oracle JDBC's
        // oracle.sql.TIMESTAMPLTZ type mapping to use OffsetDateTime, and to
        // use an appropriate precision value.
        return newMetadata(OffsetDateTime.class,
          JDBCType.TIMESTAMP_WITH_TIMEZONE, OFFSET_DATE_TIME_PRECISION,
          resultSetMetaData, jdbcIndex);

      case Types.VARBINARY:
        // Override JDBC's byte[] type mapping to use ByteBuffer. Work around
        // OracleResultSetMetaData implementation of getPrecision(int), which
        // returns 0 for RAW type columns, but implements
        // getColumnDisplaySize(int) method to return the RAW column's max
        // length in bytes.
        // TODO: Push this logic into the OracleReactiveJdbcAdapter?
        return newMetadata(ByteBuffer.class,
          getOrHandleSQLException(() ->
              resultSetMetaData.getColumnDisplaySize(jdbcIndex)),
          resultSetMetaData, jdbcIndex);

      case Types.BINARY:
      case Types.LONGVARBINARY:
        // Override JDBC's byte[] type mapping to use ByteBuffer. Work around
        // OracleResultSetMetaData implementation of getPrecision(int), which
        // returns 0 for RAW type columns. The getColumnDisplaySize(int) method
        // returns the RAW column's max length in bytes.
        return newMetadata(ByteBuffer.class, resultSetMetaData, jdbcIndex);

      case OracleTypes.INTERVALYM:
        // Override Oracle JDBC's oracle.sql.INTERVALYM type mapping to use
        // Period
        return newMetadata(Period.class, resultSetMetaData, jdbcIndex);

      case OracleTypes.INTERVALDS:
        // Override Oracle JDBC's oracle.sql.INTERVALDS type mapping to use
        // Duration
        return newMetadata(Duration.class, resultSetMetaData, jdbcIndex);

      case Types.ROWID:
        // Override JDBC's java.sql.RowId type mapping to use String
        return newMetadata(String.class, resultSetMetaData, jdbcIndex);

      case OracleTypes.JSON:
        // Override JDBC's javax.json.JsonValue type mapping to use
        // OracleJsonValue. Set the precision as null.
        // OracleResultSetMetaData.getPrecision(int) returns -1 for JSON
        // TODO: Or require the R2DBC application to have javax.json API on
        //  classpath? Need to make a decision before 1.0.0 release
        return newMetadata(
          OracleJsonObject.class, null, resultSetMetaData, jdbcIndex);

      default:
        // Derive metadata values directly from JDBC
        return newMetadata(resultSetMetaData, jdbcIndex);
    }
  }

  /**
   * Returns a new metadata object that supplies values that are directly
   * mapped from JDBC metadata as follows:
   * <ul>
   *   <li>
   *     {@link ColumnMetadata#getJavaType()} returns the class named by
   *     {@link ResultSetMetaData#getColumnClassName(int)}
   *   </li>
   *   <li>
   *     {@link ColumnMetadata#getName()} returns the same value as
   *     {@link ResultSetMetaData#getColumnLabel(int)}
   *   </li>
   *   <li>
   *     {@link ColumnMetadata#getNullability()} returns a
   *     {@link Nullability} enum value equivalent to the {@code int} constant
   *     returned by {@link ResultSetMetaData#isNullable(int)}
   *   </li>
   *   <li>
   *     {@link ColumnMetadata#getPrecision()} returns an {@code Integer} with
   *     the same {@code int} value returned by
   *     {@link ResultSetMetaData#getPrecision(int)} if the JDBC API indicates
   *     that precision is applicable by returning a non-zero value, or returns
   *     {@code null} if the JDBC API indicates that precision is not applicable
   *     by returning {@code 0}.
   *   </li>
   *   <li>
   *     {@link ColumnMetadata#getScale()} returns an {@code Integer} with
   *     the same {@code int} value returned by
   *     {@link ResultSetMetaData#getScale(int)} if the JDBC API indicates
   *     that scale is applicable by returning a non-zero value, or returns
   *     {@code null} if the JDBC API indicates that scale is not applicable
   *     by returning {@code 0}.
   *   </li>
   * </ul>
   * @param resultSetMetaData JDBC metadata
   * @param jdbcIndex 1-based column index
   * @return Metadata for the specified column
   */
  private static OracleColumnMetadataImpl newMetadata(
    ResultSetMetaData resultSetMetaData, int jdbcIndex)
    throws R2dbcException {
    return newMetadata(
      getJavaType(resultSetMetaData, jdbcIndex),
      resultSetMetaData,
      jdbcIndex);
  }

  /**
   * Returns a new metadata object with {@link ColumnMetadata#getJavaType()}
   * returning the specified {@code javaType}, and with all other values
   * directly mapped from JDBC metadata as follows:
   * <ul>
   *   <li>
   *     {@link ColumnMetadata#getName()} returns the same value as
   *     {@link ResultSetMetaData#getColumnLabel(int)}
   *   </li>
   *   <li>
   *     {@link ColumnMetadata#getNullability()} returns a
   *     {@link Nullability} enum value equivalent to the {@code int} constant
   *     returned by {@link ResultSetMetaData#isNullable(int)}
   *   </li>
   *   <li>
   *     {@link ColumnMetadata#getPrecision()} returns an {@code Integer} with
   *     the same {@code int} value returned by
   *     {@link ResultSetMetaData#getPrecision(int)} if the JDBC API indicates
   *     that precision is applicable by returning a non-zero value, or returns
   *     {@code null} if the JDBC API indicates that precision is not applicable
   *     by returning {@code 0}.
   *   </li>
   *   <li>
   *     {@link ColumnMetadata#getScale()} returns an {@code Integer} with
   *     the same {@code int} value returned by
   *     {@link ResultSetMetaData#getScale(int)} if the JDBC API indicates
   *     that scale is applicable by returning a non-zero value, or returns
   *     {@code null} if the JDBC API indicates that scale is not applicable
   *     by returning {@code 0}.
   *   </li>
   *   <li>
   *     {@link ColumnMetadata#getNativeTypeMetadata()} returns a
   *     {@link SQLType} having the same type number as the {@code int}
   *     value returned by {@link ResultSetMetaData#getColumnType(int)}.
   *   </li>
   * </ul>
   * @param javaType Java type mapping
   * @param resultSetMetaData JDBC metadata
   * @param jdbcIndex 1-based column index
   * @return Metadata for the specified column
   */
  private static OracleColumnMetadataImpl newMetadata(
    Class<?> javaType, ResultSetMetaData resultSetMetaData, int jdbcIndex) {
    return getOrHandleSQLException(() -> {
      int precision = resultSetMetaData.getPrecision(jdbcIndex);
      int scale = resultSetMetaData.getScale(jdbcIndex);
      return new OracleColumnMetadataImpl(
        getSqlType(resultSetMetaData, jdbcIndex),
        javaType,
        resultSetMetaData.getColumnLabel(jdbcIndex),
        getNullability(resultSetMetaData, jdbcIndex),
        precision == 0 ? null : precision,
        scale == 0 ? null : scale);
    });
  }

  /**
   * Returns a new metadata object with {@link ColumnMetadata#getJavaType()}
   * returning the specified {@code javaType}, with
   * {@link ColumnMetadata#getPrecision()} returning the specified
   * {@code precision}, and with all other values directly mapped from JDBC
   * metadata as follows:
   * <ul>
   *   <li>
   *     {@link ColumnMetadata#getName()} returns the same value as
   *     {@link ResultSetMetaData#getColumnLabel(int)}
   *   </li>
   *   <li>
   *     {@link ColumnMetadata#getNullability()} returns a
   *     {@link Nullability} enum value equivalent to the {@code int} constant
   *     returned by {@link ResultSetMetaData#isNullable(int)}
   *   </li>
   *   <li>
   *     {@link ColumnMetadata#getScale()} returns an {@code Integer} with
   *     the same {@code int} value returned by
   *     {@link ResultSetMetaData#getScale(int)} if the JDBC API indicates
   *     that scale is applicable by returning a non-zero value, or returns
   *     {@code null} if the JDBC API indicates that scale is not applicable
   *     by returning {@code 0}.
   *   </li>
   *   <li>
   *     {@link ColumnMetadata#getNativeTypeMetadata()} returns a
   *     {@link SQLType} having the same type number as the {@code int}
   *     value returned by {@link ResultSetMetaData#getColumnType(int)}.
   *   </li>
   * </ul>
   * @param javaType Java type mapping
   * @param precision The column's precision, or null if precision is not
   *                  applicable.
   * @param resultSetMetaData JDBC metadata
   * @param jdbcIndex 1-based column index
   * @return Metadata for the specified column
   */
  private static OracleColumnMetadataImpl newMetadata(
    Class<?> javaType, Integer precision, ResultSetMetaData resultSetMetaData,
    int jdbcIndex) {
    return getOrHandleSQLException(() -> {
      int scale = resultSetMetaData.getScale(jdbcIndex);
      return new OracleColumnMetadataImpl(
        getSqlType(resultSetMetaData, jdbcIndex), javaType,
        resultSetMetaData.getColumnLabel(jdbcIndex),
        getNullability(resultSetMetaData, jdbcIndex),
        precision, scale == 0 ? null : scale);
    });
  }


  /**
   * Returns a new metadata object with {@link ColumnMetadata#getJavaType()}
   * returning the specified {@code javaType}, with
   * {@link ColumnMetadata#getPrecision()} returning the specified
   * {@code precision}, with {@link ColumnMetadata#getNativeTypeMetadata()}
   * returning the specified {@code sqlType}, and with all other values
   * directly mapped from JDBC metadata as follows:
   * <ul>
   *   <li>
   *     {@link ColumnMetadata#getName()} returns the same value as
   *     {@link ResultSetMetaData#getColumnLabel(int)}
   *   </li>
   *   <li>
   *     {@link ColumnMetadata#getNullability()} returns a
   *     {@link Nullability} enum value equivalent to the {@code int} constant
   *     returned by {@link ResultSetMetaData#isNullable(int)}
   *   </li>
   *   <li>
   *     {@link ColumnMetadata#getScale()} returns an {@code Integer} with
   *     the same {@code int} value returned by
   *     {@link ResultSetMetaData#getScale(int)} if the JDBC API indicates
   *     that scale is applicable by returning a non-zero value, or returns
   *     {@code null} if the JDBC API indicates that scale is not applicable
   *     by returning {@code 0}.
   *   </li>
   * </ul>
   * @param javaType Java type mapping
   * @param precision The column's precision, or null if precision is not
   *                  applicable.
   * @param resultSetMetaData JDBC metadata
   * @param jdbcIndex 1-based column index
   * @return Metadata for the specified column
   */
  private static OracleColumnMetadataImpl newMetadata(
    Class<?> javaType, SQLType sqlType, Integer precision,
    ResultSetMetaData resultSetMetaData, int jdbcIndex) {
    return getOrHandleSQLException(() -> {
      int scale = resultSetMetaData.getScale(jdbcIndex);
      return new OracleColumnMetadataImpl(sqlType, javaType,
        resultSetMetaData.getColumnLabel(jdbcIndex),
        getNullability(resultSetMetaData, jdbcIndex),
        precision, scale == 0 ? null : scale);
    });
  }

  /**
   * Returns the Java {@code Class} type mapping of a {@code ResultSetMetaData}
   * column at the 1-based index specified by {@code jdbcIndex}. This method
   * maps the class name {@code String} returned by
   * {@link ResultSetMetaData#getColumnClassName(int)} to the {@code Class}
   * object that is associated to that name.
   * @param resultSetMetaData Indicates the Java type mapping for a row of
   *   columns.
   * @param jdbcIndex 1-based index of a column
   * @return The Java type of the specified column.
   * @throws R2dbcException If a database access error occurs
   * @throws io.r2dbc.spi.R2dbcNonTransientException If the class loader can not
   * find a class with the specified {@code className}
   */
  private static Class<?> getJavaType(
    ResultSetMetaData resultSetMetaData, int jdbcIndex) {
    try {
      String className = getOrHandleSQLException(() ->
        resultSetMetaData.getColumnClassName(jdbcIndex));
      return className == null ? Object.class : Class.forName(className);
    }
    catch (ClassNotFoundException columnClassNotFound) {
      throw OracleR2dbcExceptions.newNonTransientException(
        "Could not find the Java class type for column at index: "
          + (jdbcIndex - 1),
        columnClassNotFound);
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
  private static Nullability getNullability(
    ResultSetMetaData resultSetMetaData, int jdbcIndex) {
    int nullability = getOrHandleSQLException(() ->
      resultSetMetaData.isNullable(jdbcIndex));

    switch (nullability) {
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
    return sqlType;
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
   * Implements the R2DBC SPI method by returning the R2DBC {@code Type} scale
   * supplied from a
   * JDBC
   *
   * </p>
   * @return
   */
  @Override
  public Type getType() {
    return null; // TODO
  }

  /**
   * <p>
   * Returns a {@link SQLType} that having the type number returned by
   * {@link ResultSetMetaData#getColumnType(int)} at a 1-based column
   * index specified as {@code jdbcIndex}
   * </p><p>
   * A {@link JDBCType} is returned if one is found to have a matching type
   * number. Otherwise, an {@link OracleType} is returned if one is found to
   * have a matching type number. Otherwise, {@link JDBCType#OTHER} is
   * returned if no {@link SQLType} is found to have a matching type number.
   * </p>
   * @param resultSetMetaData JDBC metadata
   * @param jdbcIndex 1-based index of a column
   * @return A {@link SQLType} having the type number of the specified column.
   */
  private static SQLType getSqlType(ResultSetMetaData resultSetMetaData,
                                    int jdbcIndex) {
    int typeNumber = getOrHandleSQLException(() ->
      resultSetMetaData.getColumnType(jdbcIndex));

    for (JDBCType jdbcType : JDBCType.values()) {
      Integer vendorTypeNumber = jdbcType.getVendorTypeNumber();
      if (vendorTypeNumber != null && vendorTypeNumber == typeNumber)
        return jdbcType;
    }

    try {
      OracleType oracleType = OracleType.toOracleType(typeNumber);
      return oracleType != null ? oracleType : JDBCType.OTHER;
    }
    catch (SQLException typeNotFound) {
      return JDBCType.OTHER;
    }
  }

}