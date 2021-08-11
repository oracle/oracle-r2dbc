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

import java.sql.ResultSetMetaData;
import java.sql.SQLType;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Objects;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.OutParameterMetadata;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.ReadableMetadata;
import io.r2dbc.spi.Type;
import oracle.r2dbc.OracleR2dbcTypes;

import static oracle.r2dbc.impl.OracleR2dbcExceptions.fromJdbc;
import static oracle.r2dbc.impl.SqlTypeMap.toJdbcType;
import static oracle.r2dbc.impl.SqlTypeMap.toR2dbcType;

/**
 * <p>
 * Implementation of the {@link ReadableMetadata} SPI for Oracle Database.
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
class OracleReadableMetadataImpl implements ReadableMetadata {

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
   * The SQL type of the readable value
   */
  private final Type type;

  /**
   * The name of the readable value. The name may be defined in the SQL
   * statement that returned the value, using an alias for instance.
   */
  private final String name;

  /**
   * The nullability of the readable value, indicating if can have null
   * values or not. It may indicate that it is unknown whether null values
   * are possible.
   */
  private final Nullability nullability;

  /**
   * The precision of the readable value, which indicates the maximum number of
   * significant digits for a numeric type, the maximum number characters for
   * a character type, the maximum number of characters used to represent a
   * datetime type, or null if precision is not applicable to the data type
   * of the value.
   */
  private final Integer precision;

  /**
   * The scale of the readable value, which indicates the maximum amount of
   * digits to the right of the decimal point for numeric types, or null if
   * scale is not applicable to the data type of the value.
   */
  private final Integer scale;

  /**
   * Constructs a new instance that supplies metadata of a readable value as
   * specified by parameters to this constructor.
   * @param type The column's SQL type. Not null.
   * @param name The column's name, as specified in the SQL statement that
 *             returned the column. Not null.
   * @param nullability The column's nullability. Not the Java Language
   *          {@code null} value. May be any value of {@link Nullability}.
   * @param precision The column's precision, or null if precision is not
*                  applicable to the column's type.
   * @param scale The column's scale, or null if scale is not applicable to
   *              the column's type.
   */
  private OracleReadableMetadataImpl(
    Type type, String name, Nullability nullability, Integer precision,
    Integer scale) {
    this.type = type;
    this.name = name;
    this.nullability = nullability;
    this.precision = precision;
    this.scale = scale;
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
    return type.getJavaType();
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
    return toJdbcType(type);
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
    return type;
  }


  /**
   * Creates {@code ColumnMetadata} for an out parameter. The returned
   * {@code ColumnMetaData} has a specified {@code name} and {@code type}.
   * The Java type mapping of the returned metadata is obtained by invoking
   * {@link Type#getJavaType()} on {@code type}. The nullability is
   * {@link Nullability#NULLABLE}, as all PL/SQL out parameters may be {@code
   * null}.
   * @param name Column name
   * @param type Column type
   * @return Column metadata having {@code name} and {@code type}
   */
  static OutParameterMetadata createParameterMetadata(
    String name, Type type) {
    return new OracleOutParameterMetadataImpl(
      type, name, Nullability.NULLABLE, null, null);
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
   * @return {@code ColumnMetaData} for the parameter at the specified  index
   */
  static ColumnMetadata createColumnMetadata(
    ResultSetMetaData resultSetMetaData, int index) {

    int jdbcIndex = index + 1;

    Type type = toR2dbcType(fromJdbc(() ->
      resultSetMetaData.getColumnType(jdbcIndex)));

    String name = fromJdbc(() ->
      resultSetMetaData.getColumnName(jdbcIndex));

    Nullability nullability = getNullability(fromJdbc(() ->
      resultSetMetaData.isNullable(jdbcIndex)));

    if (type == R2dbcType.BLOB || type == R2dbcType.CLOB
      || type == R2dbcType.NCLOB) {
      // For LOB types, use null as the precision. The actual maximum length
      // is (4GB x database-block-size), which can not be stored as an Integer
      return new OracleColumnMetadataImpl(type, name, nullability, null, null);
    }
    else if (type == R2dbcType.DATE) {
      // For the DATE type, use the length of LocalDate.toString() as the
      // precision
      return new OracleColumnMetadataImpl(type, name, nullability,
        LOCAL_DATE_PRECISION, null);
    }
    else if (type == R2dbcType.TIMESTAMP
      || type == OracleR2dbcTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      // For the TIMESTAMP types, use the length of LocalDateTime.toString() as
      // the precision. Use the scale from JDBC, even if it's 0 because a
      // TIMESTAMP may 0 decimal digits.
      return new OracleColumnMetadataImpl(type, name, nullability,
        LOCAL_DATE_TIME_PRECISION,
        fromJdbc(() -> resultSetMetaData.getScale(jdbcIndex)));
    }
    else if (type == R2dbcType.TIMESTAMP_WITH_TIME_ZONE) {
      // For the TIMESTAMP WITH TIMEZONE types, use the length of
      // OffsetDateTime.toString() as the precision. Use the scale from JDBC,
      // even if it's 0 because a  TIMESTAMP may 0 decimal digits.
      return new OracleColumnMetadataImpl(type, name, nullability,
        OFFSET_DATE_TIME_PRECISION,
        fromJdbc(() -> resultSetMetaData.getScale(jdbcIndex)));
    }
    else if (type == R2dbcType.VARBINARY) {
      // Oracle JDBC implements getColumnDisplaySize to return the
      // maximum length for a RAW column. It does not implement
      // getPrecision(int) to return the correct value
      return new OracleColumnMetadataImpl(type, name, nullability,
        fromJdbc(() ->
          resultSetMetaData.getColumnDisplaySize(jdbcIndex)),
        null);
    }
    // For all other types, metadata values are taken directly from JDBC.
    else {
      int precision = fromJdbc(() ->
        resultSetMetaData.getPrecision(jdbcIndex));

      int scale = fromJdbc(() ->
        resultSetMetaData.getScale(jdbcIndex));

      // If the JDBC type number was not recognized, then create a Type having
      // the SQL type name and Java class name identified by ResultSetMetaData.
      if (type == null) {
        type = fromJdbc(() ->
          new TypeImpl(resultSetMetaData.getColumnTypeName(jdbcIndex),
            tryGetClass(resultSetMetaData.getColumnClassName(jdbcIndex))));
      }

      return new OracleColumnMetadataImpl(
        type, name, nullability,
        // The getPrecision and getScale methods return 0 for types where
        // precision and scale are not applicable.
        precision == 0 ? null : precision,
        scale == 0 ? null : scale);
    }
  }

  /**
   * Maps the {@code int} constant returned by
   * {@link ResultSetMetaData#isNullable(int)} to the equivalent
   * {@link Nullability} enum value.
   * @param jdbcNullability JDBC nullability value
   * @return R2DBC {@code Nullability}
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
   * Returns the {@code Class} identified by {@code className}, or returns
   * {@code Object.class} if the identified class can not be located.
   * @param className Name of Class to return
   * @return The named Class if it can be located, otherwise returns
   * {@code Object.class}
   */
  private static Class<?> tryGetClass(String className) {
      try {
        return className == null
          ? Object.class
          : Class.forName(className);
      }
      catch (ClassNotFoundException classNotFoundException) {
        return Object.class;
      }
  }

  /**
   * Implementation of the {@link ColumnMetadata} interface of the
   * R2DBC SPI. This class inherits the implementation of
   * {@link ReadableMetadata} from {@link OracleReadableMetadataImpl}. This
   * class does not currently declare any additional methods, but it might do
   * so if a future SPI update defines any for the {@code ColumnMetadata}
   * interface.
   */
  private static final class OracleColumnMetadataImpl
    extends OracleReadableMetadataImpl implements ColumnMetadata {

    /**
     * Constructs a new instance that supplies metadata of a column.
     *
     * @param type The column's SQL type. Not null.
     * @param name The column's name, as specified in the SQL statement that
     *   returned the column. Not null.
     * @param nullability The column's nullability. Not the Java Language {@code
     *   null} value. May be any value of {@link Nullability}.
     * @param precision The column's precision, or null if precision is not
     *   applicable to the column's type.
     * @param scale The column's scale, or null if scale is not applicable to the
     *   column's type.
     * @throws IllegalStateException If the {@code jdbcType} is not recognized
     */
    private OracleColumnMetadataImpl(
      Type type, String name, Nullability nullability, Integer precision,
      Integer scale) {
      super(type, name, nullability, precision, scale);
    }
  }

  /**
   * Implementation of the {@link OutParameterMetadata} interface of the
   * R2DBC SPI. This class inherits the implementation of
   * {@link ReadableMetadata} from {@link OracleReadableMetadataImpl}. This
   * class does not currently declare any additional methods, but it might do
   * so if a future SPI update defines any for the {@code OutParameterMetadata}
   * interface.
   */
  private static final class OracleOutParameterMetadataImpl
    extends OracleReadableMetadataImpl implements OutParameterMetadata {

    /**
     * Constructs a new instance that supplies metadata of an out parameter.
     *
     * @param type The out parameter's SQL type. Not null.
     * @param name The out parameter's name, as specified in the SQL statement
     * that returned the out parameter. Not null.
     * @param nullability The out parameter's nullability. Not the Java Language
     * {@code null} value. May be any value of {@link Nullability}.
     * @param precision The out parameter's precision, or null if precision is
     * not applicable to the out parameter's type.
     * @param scale The out parameter's scale, or null if scale is not applicable to
     */
    private OracleOutParameterMetadataImpl(
      Type type, String name, Nullability nullability, Integer precision,
      Integer scale) {
      super(type, name, nullability, precision, scale);
    }
  }

  /**
   * Implementation of the R2DBC {@link Type} interface. Instances of this class
   * are constructed when the metadata of a {@code Readable} value identifies a
   * SQL type not recognized by {@link SqlTypeMap}. The instance is
   * constructed with the SQL type name and Java type mapping identified by
   * an instance of {@link ResultSetMetaData}.
   */
  private static final class TypeImpl implements Type {

    /** Name of this SQL type */
    private final String name;

    /** Java type mapping for this SQL type */
    private final Class<?> javaType;

    private TypeImpl(String name, Class<?> javaType) {
      this.name = name;
      this.javaType = javaType;
    }

    @Override
    public Class<?> getJavaType() {
      return javaType;
    }

    @Override
    public String getName() {
      return name;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overrides {@code equals} so that two instances of this class are equal
     * if they have the same SQL type name and Java type mapping.
     * </p>
     */
    @Override
    public boolean equals(Object object) {
      if (object == this) {
        return true;
      }
      else if (object instanceof TypeImpl) {
        TypeImpl typeImpl = (TypeImpl)object;
        return typeImpl.name.equals(name)
          && typeImpl.javaType.equals(javaType);
      }
      else {
        return false;
      }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overrides {@code hashCode} so that two instances of this class have
     * the same hash code if they have the same SQL type name and Java type
     * mapping.
     * </p>
     */
    @Override
    public int hashCode() {
      return Objects.hash(name, javaType);
    }
  }

}