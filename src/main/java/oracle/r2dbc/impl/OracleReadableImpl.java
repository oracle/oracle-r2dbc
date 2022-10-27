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

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.OutParametersMetadata;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Type;
import oracle.jdbc.OracleArray;
import oracle.jdbc.OracleConnection;
import oracle.r2dbc.OracleR2dbcTypes;
import oracle.r2dbc.impl.ReactiveJdbcAdapter.JdbcReadable;
import oracle.r2dbc.impl.ReadablesMetadata.OutParametersMetadataImpl;
import oracle.r2dbc.impl.ReadablesMetadata.RowMetadataImpl;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntFunction;

import static java.lang.String.format;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.fromJdbc;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.runJdbc;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.toR2dbcException;

/**
 * <p>
 * Implementation of the {@link io.r2dbc.spi.Readable} SPI for Oracle Database.
 * </p><p>
 * Instances of this class supply the values of a {@link JdbcReadable}. The
 * {@code JdbcReadable} may represent a single row of data, or a set of out
 * parameter values.
 * </p>
 * @author  harayuanwang, michael-a-mcmahon
 * @since   0.1.0
 */
class OracleReadableImpl implements io.r2dbc.spi.Readable {


  /** The JDBC connection that created this readable */
  private final java.sql.Connection jdbcConnection;

  /** This values of this {@code Readable}. Values are supplied by a JDBC Driver */
  private final JdbcReadable jdbcReadable;

  /** Metadata of the values of this {@code Readable}. */
  private final ReadablesMetadata<?> readablesMetadata;

  /** Adapts JDBC Driver APIs into Reactive Streams APIs */
  private final ReactiveJdbcAdapter adapter;

  /**
   * A collection of results that depend on the JDBC statement which created
   * this readable to remain open until all results are consumed.
   */
  private final DependentCounter dependentCounter;

  /**
   * <p>
   * Constructs a new {@code Readable} that supplies values of a
   * {@code jdbcReadable} and obtains metadata of the values from
   * {@code resultMetadata}.
   * </p>
   * @param jdbcConnection JDBC connection that created the
   *   {@code jdbcReadable}. Not null.
   * @param jdbcReadable Readable values from a JDBC Driver. Not null.
   * @param readablesMetadata Metadata of each value. Not null.
   * @param adapter Adapts JDBC calls into reactive streams. Not null.
   */
  private OracleReadableImpl(
    java.sql.Connection jdbcConnection,  DependentCounter dependentCounter,
    JdbcReadable jdbcReadable, ReadablesMetadata<?> readablesMetadata,
    ReactiveJdbcAdapter adapter) {
    this.jdbcConnection = jdbcConnection;
    this.dependentCounter = dependentCounter;
    this.jdbcReadable = jdbcReadable;
    this.readablesMetadata = readablesMetadata;
    this.adapter = adapter;
  }

  /**
   * <p>
   * Creates a new {@code Row} that supplies column values and metadata from the
   * provided {@code jdbcReadable} and {@code metadata}. The metadata
   * object is used to determine the default type mapping of column values.
   * </p>
   * @param jdbcConnection JDBC connection that created the
   *   {@code jdbcReadable}. Not null.
   * @param jdbcReadable Row data from the Oracle JDBC Driver. Not null.
   * @param metadata Meta-data for the specified row. Not null.
   * @param adapter Adapts JDBC calls into reactive streams. Not null.
   * @return A {@code Row} backed by the {@code jdbcReadable} and
   *   {@code metadata}. Not null.
   */
  static Row createRow(
    java.sql.Connection jdbcConnection, DependentCounter dependentCounter,
    JdbcReadable jdbcReadable, RowMetadataImpl metadata,
    ReactiveJdbcAdapter adapter) {
    return new RowImpl(
      jdbcConnection, dependentCounter, jdbcReadable, metadata, adapter);
  }

  /**
   * <p>
   * Creates a new {@code OutParameters} that supplies values and metadata from
   * the provided {@code jdbcReadable} and {@code rowMetadata}. The metadata
   * object is used to determine the default type mapping of column values.
   * </p>
   * @param jdbcConnection JDBC connection that created the
   *   {@code jdbcReadable}. Not null.
   * @param jdbcReadable Row data from the Oracle JDBC Driver. Not null.
   * @param metadata Meta-data for the specified row. Not null.
   * @param adapter Adapts JDBC calls into reactive streams. Not null.
   * @return An {@code OutParameters} backed by the {@code jdbcReadable} and
   *   {@code metadata}. Not null.
   */
  static OutParameters createOutParameters(
    java.sql.Connection jdbcConnection, DependentCounter dependentCounter,
    JdbcReadable jdbcReadable, OutParametersMetadataImpl metadata,
    ReactiveJdbcAdapter adapter) {
    return new OutParametersImpl(
      jdbcConnection, dependentCounter, jdbcReadable, metadata, adapter);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method with the {@code JdbcReadable} that backs
   * this {@code Readable} converting the value identified by {@code index}
   * into the specified {@code type}.
   * </p>
   * @throws IllegalArgumentException {@inheritDoc}
   * @throws IllegalArgumentException If conversion to the specified
   * {@code type} is not supported.
   */
  @Override
  public <T> T get(int index, Class<T> type) {
    requireNonNull(type, "type must not be null");
    requireValidIndex(index);
    return convert(index, type);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method with the {@code JdbcReadable} that backs
   * this {@code Readable} converting the value identified by
   * {@code name} into the specified {@code type}.
   * </p><p>
   * This method uses a case-insensitive column name match. If more than one
   * column has a matching name, this method returns the value of the
   * matching column with the lowest index.
   * </p>
   * @throws IllegalArgumentException {@inheritDoc}
   * @throws IllegalArgumentException If conversion to the specified
   * {@code type} is not supported.
   * @throws IllegalArgumentException If no value has a matching {@code name}
   */
  @Override
  public <T> T get(String name, Class<T> type) {
    requireNonNull(name, "name must not be null");
    requireNonNull(type, "type must not be null");
    return convert(indexOf(name), type);
  }

  /**
   * Returns the 0-based index of the value identified by {@code name}. This
   * method implements a case-insensitive name match. If more than one
   * value has a matching name, this method returns lowest of all indexes that
   * match.
   * @param name The name of a value. Not null.
   * @return The index of the named value within this {@code Readable}
   * @throws NoSuchElementException If no column has a matching name.
   */
  private int indexOf(String name) {
    int columnIndex = readablesMetadata.getColumnIndex(name);
    if (columnIndex != -1)
      return columnIndex;
    else
      throw new NoSuchElementException("Unrecognized name: " + name);
  }

  /**
   * <p>
   * Converts the value at the 0-based {@code index} of this {@code Readable}
   * into the specified {@code type}.
   * </p><p>
   * This method implements conversions to Java types that are not supported by
   * JDBC drivers, but are supported by R2DBC drivers. If the {@code type} is
   * {@code Object.class}, then the value is converted to the default Java
   * type mapping for its SQL type.
   * </p>
   *
   * @param index 0-based index of a value
   * @param type Java type that the value is converted to. Not null.
   * @param <T> Java type that the value is converted to
   * @return The converted column value. May be null.
   * @throws R2dbcException If the conversion is not supported.
   */
  private <T> T convert(int index, Class<T> type) {
    final Object value;

    if (ByteBuffer.class.equals(type)) {
      value = getByteBuffer(index);
    }
    else if (io.r2dbc.spi.Blob.class.equals(type)) {
      value = getBlob(index);
    }
    else if (io.r2dbc.spi.Clob.class.equals(type)) {
      value = getClob(index);
    }
    else if (LocalDateTime.class.equals(type)) {
      value = getLocalDateTime(index);
    }
    else if (Result.class.equals(type)) {
      value = getResult(index);
    }
    else if (type.isArray()
      && R2dbcType.COLLECTION.equals(readablesMetadata.get(index).getType())) {
      // Note that a byte[] would be a valid mapping for a RAW column, so this
      // branch is only taken if the target type is an array, and the column
      // type is a SQL ARRAY (ie: COLLECTION).
      value = getJavaArray(index, type.getComponentType());
    }
    else if (Object.class.equals(type)) {
      // Use the default type mapping if Object.class has been specified.
      // This method is invoked recursively with the default mapping, so long
      // as Object.class is not also the default mapping.
      Class<?> defaultType = readablesMetadata.get(index).getJavaType();
      value = Object.class.equals(defaultType)
        ? jdbcReadable.getObject(index, Object.class)
        : convert(index, defaultType);
    }
    else {
      value = jdbcReadable.getObject(index, type);
    }

    return type.cast(value);
  }

  /**
   * <p>
   * Converts the value of a column at the specified {@code index} to a
   * {@code ByteBuffer}.
   * </p><p>
   * A JDBC driver is not required to support {@code ByteBuffer} conversions
   * for any SQL type, so this method is necessary to implement the
   * conversion to {@code ByteBuffer} from a type that is supported by JDBC.
   * </p>
   * @param index 0 based column index
   * @return A column value as a {@code ByteBuffer}, or null if the column
   * value is NULL.
   */
  private ByteBuffer getByteBuffer(int index) {
    byte[] columnValue = jdbcReadable.getObject(index, byte[].class);
    return columnValue == null ? null : ByteBuffer.wrap(columnValue);
  }

  /**
   * <p>
   * Converts the value of a column at the specified {@code index} to a
   * {@code Blob}.
   * </p><p>
   * A JDBC driver is not required to support {@code io.r2dbc.spi.Blob}
   * conversions for any SQL type, so this method is necessary to implement the
   * conversion to {@code Blob} from a type that is supported by JDBC.
   * </p>
   * @param index 0 based column index
   * @return A column value as a {@code Blob}, or null if the column
   * value is NULL.
   */
  private Blob getBlob(int index) {
    java.sql.Blob jdbcBlob = jdbcReadable.getObject(index, java.sql.Blob.class);
    return jdbcBlob == null
      ? null
      : OracleLargeObjects.createBlob(
          adapter.publishBlobRead(jdbcBlob),
          adapter.publishBlobFree(jdbcBlob));
  }

  /**
   * <p>
   * Converts the value of a column at the specified {@code index} to a
   * {@code Clob}.
   * </p><p>
   * A JDBC driver is not required to support {@code io.r2dbc.spi.Clob}
   * conversions for any SQL type, so this method is necessary to implement the
   * conversion to {@code Clob} from a type that is supported by JDBC.
   * </p>
   * @param index 0 based column index
   * @return A column value as a {@code Clob}, or null if the column
   * value is NULL.
   */
  private Clob getClob(int index) {
    // Convert to a JDBC NClob or Clob, depending on the column type
    Type type = readablesMetadata.get(index).getType();
    final java.sql.Clob jdbcClob;
    if (R2dbcType.NCLOB.equals(type) || R2dbcType.NVARCHAR.equals(type)
      || R2dbcType.NCHAR.equals(type)) {
      jdbcClob = jdbcReadable.getObject(index, java.sql.NClob.class);
    }
    else {
      jdbcClob = jdbcReadable.getObject(index, java.sql.Clob.class);
    }

    return jdbcClob == null
      ? null
      : OracleLargeObjects.createClob(
          adapter.publishClobRead(jdbcClob),
          adapter.publishClobFree(jdbcClob));
  }

  /**
   * <p>
   * Converts the value of a column at the specified {@code index} to a
   * {@code LocalDateTime}.
   * </p><p>
   * A JDBC driver is not required to support {@code LocalDateTime} conversions
   * for any SQL type. The Oracle JDBC driver is known to support {@code
   * LocalDateTime} conversions for DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE.
   * </p><p>
   * The 21.1 Oracle JDBC Driver does not implement a correct conversion for
   * TIMESTAMP WITH LOCAL TIME ZONE; The driver returns a value in the database
   * timezone rather than the session time zone. A correct conversion is
   * implemented for {@code java.sql.Timestamp}, so this method is implemented
   * to convert that into a {@code LocalDateTime}.
   * </p>
   * @param index 0 based column index
   * @return A column value as a {@code Clob}, or null if the column
   * value is NULL.
   */
  private LocalDateTime getLocalDateTime(int index) {
    if (OracleR2dbcTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE
          .equals(readablesMetadata.get(index).getType())) {
      // TODO: Remove this when Oracle JDBC implements a correct conversion
      Timestamp timestamp = jdbcReadable.getObject(index, Timestamp.class);
      return timestamp == null ? null : timestamp.toLocalDateTime();
    }
    else {
      return jdbcReadable.getObject(index, LocalDateTime.class);
    }
  }

  /**
   * <p>
   * Converts the value of a column at the specified {@code index} to a Java
   * array.
   * </p><p>
   * JDBC drivers are not required to support conversion to Java arrays for any
   * SQL type. However, an R2DBC driver must support conversions to Java arrays
   * for ARRAY values. This method is implemented to handle that case.
   * </p>
   * @param index 0 based column index
   * @param javaType Type of elements stored in the array. Not null.
   * @return A Java array, or {@code null} if the column value is null.
   */
  private Object getJavaArray(int index, Class<?> javaType) {
    OracleArray oracleArray = jdbcReadable.getObject(index, OracleArray.class);

    return oracleArray == null
      ? null
      : convertOracleArray(oracleArray, javaType);
  }

  /**
   * Converts an {@code OracleArray} from Oracle JDBC into the Java array
   * variant of the specified {@code javaType}. If the {@code javaType} is
   * {@code Object.class}, this method converts to the R2DBC standard mapping
   * for the SQL type of the ARRAY elements.
   * @param oracleArray Array from Oracle JDBC. Not null.
   * @param javaType Type to convert to. Not null.
   * @return The converted array.
   */
  private Object convertOracleArray(
    OracleArray oracleArray, Class<?> javaType) {
    try {
      // Convert to primitive array types using API extensions on OracleArray
      if (javaType.isPrimitive())
        return convertPrimitiveArray(oracleArray, javaType);

      // Check if Row.get(int/String) was called without a Class argument.
      // In this case, the default mapping is declared as Object[] in
      // SqlTypeMap, and this method gets called with Object.class. A default
      // mapping is used in this case.
      Class<?> convertedType = getArrayTypeMapping(oracleArray, javaType);

      // Attempt to have Oracle JDBC convert to the desired type
      Object[] javaArray =
        (Object[]) oracleArray.getArray(
          Map.of(oracleArray.getBaseTypeName(), convertedType));

      // Oracle JDBC may ignore the Map argument in many cases, and just
      // maps values to their default JDBC type. The convertArray method
      // will correct this by converting the array to the desired type.
      return convertArray(javaArray, convertedType);
    }
    catch (SQLException sqlException) {
      throw toR2dbcException(sqlException);
    }
    finally {
      runJdbc(oracleArray::free);
    }
  }

  /**
   * Returns the Java array type that an ARRAY will be mapped to. This method
   * is used to determine a default type mapping when {@link #get(int)} or
   * {@link #get(String)} are called. In this case, the {@code javaType}
   * argument is expected to be {@code Object.class} and this method returns
   * a default mapping based on the element type of the ARRAY. Otherwise, if
   * the {@code javaType} is something more specific, this method just returns
   * it.
   */
  private Class<?> getArrayTypeMapping(
    OracleArray oracleArray, Class<?> javaType) {

    if (!Object.class.equals(javaType))
      return javaType;

    int jdbcType = fromJdbc(oracleArray::getBaseType);

    // Check if the array is multi-dimensional
    if (jdbcType == Types.ARRAY) {

      Object[] oracleArrays = (Object[]) fromJdbc(oracleArray::getArray);

      // An instance of OracleArray representing base type is needed in order to
      // know the base type of the next dimension.
      final OracleArray oracleArrayElement;
      if (oracleArrays.length > 0) {
        oracleArrayElement = (OracleArray) oracleArrays[0];
      }
      else {
        // The array is empty, so an OracleArray will need to be created. The
        // type information for the ARRAY should be cached by Oracle JDBC, and
        // so createOracleArray should not perform a blocking call.
        oracleArrayElement = (OracleArray) fromJdbc(() ->
          jdbcConnection.unwrap(OracleConnection.class)
            .createOracleArray(oracleArray.getBaseTypeName(), new Object[0]));
      }

      // Recursively call getJavaArrayType, creating a Java array at each level
      // of recursion until a non-array SQL type is found. Returning back up the
      // stack, the top level of recursion will then create an array with
      // the right number of dimensions, and the class of this multi-dimensional
      // array is returned.
      return java.lang.reflect.Array.newInstance(
        getArrayTypeMapping(oracleArrayElement, Object.class), 0)
        .getClass();
    }

    // If the element type is DATE, handle it as if it were TIMESTAMP.
    // This is consistent with how DATE columns are usually handled, and
    // reflects the fact that Oracle DATE values have a time component.
    Type r2dbcType = SqlTypeMap.toR2dbcType(
      jdbcType == Types.DATE ? Types.TIMESTAMP : jdbcType);

    // Use R2DBC's default type mapping, if the SQL type is recognized.
    // Otherwise, leave the javaType as Object.class and Oracle JDBC's
    // default type mapping will be used.
    return r2dbcType != null
      ? r2dbcType.getJavaType()
      : javaType;
  }

  /**
   * Converts an array from Oracle JDBC into a Java array of a primitive type,
   * such as int[], boolean[], etc. This method is handles the case where user
   * code explicitly requests a primitive array by passing the class type
   * to {@link #get(int, Class)} or {@link #get(String, Class)}.
   */
  private Object convertPrimitiveArray(
    OracleArray oracleArray, Class<?> primitiveType) {
    try {
      if (boolean.class.equals(primitiveType)) {
        // OracleArray does not support conversion to boolean[], so map shorts
        // to booleans.
        short[] shorts = oracleArray.getShortArray();
        boolean[] booleans = new boolean[shorts.length];
        for (int i = 0; i < shorts.length; i++)
          booleans[i] = shorts[i] != 0;
        return booleans;
      }
      else if (byte.class.equals(primitiveType)) {
        // OracleArray does not support conversion to byte[], so map shorts to
        // bytes
        short[] shorts = oracleArray.getShortArray();
        byte[] bytes = new byte[shorts.length];
        for (int i = 0; i < shorts.length; i++)
          bytes[i] = (byte) shorts[i];
        return bytes;
      }
      else if (short.class.equals(primitiveType)) {
        return oracleArray.getShortArray();
      }
      else if (int.class.equals(primitiveType)) {
        return oracleArray.getIntArray();
      }
      else if (long.class.equals(primitiveType)) {
        return oracleArray.getLongArray();
      }
      else if (float.class.equals(primitiveType)) {
        return oracleArray.getFloatArray();
      }
      else if (double.class.equals(primitiveType)) {
        return oracleArray.getDoubleArray();
      }
      else {
        // Attempt to have Oracle JDBC convert the array
        Object javaArray = oracleArray.getArray(
          Map.of(oracleArray.getSQLTypeName(), primitiveType));

        // Check if Oracle JDBC ignored the Map argument or not
        Class<?> javaArrayType = javaArray.getClass().getComponentType();
        if (primitiveType.equals(javaArrayType))
          return javaArray;

        throw unsupportedArrayConversion(javaArrayType, primitiveType);
      }
    }
    catch (SQLException sqlException) {
      throw toR2dbcException(sqlException);
    }
  }

  /**
   * Converts a given {@code array} to an array of a {@code desiredType}. This
   * method handles arrays returned by {@link Array#getArray(Map)}, which may
   * contain objects that are the default desiredType mapping for a JDBC driver.
   * This method converts the default JDBC desiredType mappings to the default
   * R2DBC desiredType mappings.
   */
  @SuppressWarnings("unchecked")
  private <T> T[] convertArray(Object[] array, Class<T> desiredType) {

    if (desiredType.isAssignableFrom(array.getClass().getComponentType()))
      return (T[])array;

    if (array.length == 0)
      return (T[]) java.lang.reflect.Array.newInstance(desiredType, 0);

    // The array's component type could be Object.class; Oracle JDBC returns an
    // Object[] in some cases. Search for a non-null element to determine the
    // true type of type objects in the array.
    Class<?> elementType = null;
    for (Object element : array) {
      if (element != null) {
        elementType = array[0].getClass();
        break;
      }
    }

    // If all array elements are null, then return an array of the desired type
    // with all null elements.
    if (elementType == null) {
      return (T[]) java.lang.reflect.Array.newInstance(
        desiredType, array.length);
    }

    if (desiredType.isAssignableFrom(elementType)) {
      // The elements are of the desired type, but the array type is something
      // different, like an Object[] full of BigDecimal.
      return (T[]) mapArray(
        array,
        length -> (T[])java.lang.reflect.Array.newInstance(desiredType, length),
        Function.identity());
    }
    else if (elementType.equals(BigDecimal.class)) {
      if (desiredType.equals(Boolean.class)) {
        return (T[]) mapArray(
          (BigDecimal[])array, Boolean[]::new,
          bigDecimal -> bigDecimal.shortValue() != 0);
      }
      else if (desiredType.equals(Byte.class)) {
        return (T[]) mapArray(
          (BigDecimal[])array, Byte[]::new, BigDecimal::byteValue);
      }
      else if (desiredType.equals(Short.class)) {
        return (T[]) mapArray(
          (BigDecimal[])array, Short[]::new, BigDecimal::shortValue);
      }
      else if (desiredType.equals(Integer.class)) {
        return (T[]) mapArray(
          (BigDecimal[])array, Integer[]::new, BigDecimal::intValue);
      }
      else if (desiredType.equals(Long.class)) {
        return (T[]) mapArray(
          (BigDecimal[])array, Long[]::new, BigDecimal::longValue);
      }
      else if (desiredType.equals(Float.class)) {
        return (T[]) mapArray(
          (BigDecimal[])array, Float[]::new, BigDecimal::floatValue);
      }
      else if (desiredType.equals(Double.class)) {
        return (T[]) mapArray(
          (BigDecimal[])array, Double[]::new, BigDecimal::doubleValue);
      }
    }
    else if (byte[].class.equals(elementType)
      && desiredType.isAssignableFrom(ByteBuffer.class)) {
      return (T[]) mapArray(
        (byte[][])array, ByteBuffer[]::new, ByteBuffer::wrap);
    }
    else if (java.sql.Timestamp.class.isAssignableFrom(elementType)) {
      // Note that DATE values are represented as Timestamp by OracleArray.
      // For this reason, there is no branch for java.sql.Date conversions in
      // this method.
      if (desiredType.isAssignableFrom(LocalDateTime.class)) {
        return (T[]) mapArray(
          (java.sql.Timestamp[]) array, LocalDateTime[]::new,
          Timestamp::toLocalDateTime);
      }
      else if (desiredType.isAssignableFrom(LocalDate.class)) {
        return (T[]) mapArray(
          (java.sql.Timestamp[]) array, LocalDate[]::new,
          timestamp -> timestamp.toLocalDateTime().toLocalDate());
      }
      else if (desiredType.isAssignableFrom(LocalTime.class)) {
        return (T[]) mapArray(
          (java.sql.Timestamp[]) array, LocalTime[]::new,
          timestamp -> timestamp.toLocalDateTime().toLocalTime());
      }
    }
    else if (java.time.OffsetDateTime.class.isAssignableFrom(elementType)) {
      // This branch handles mapping from TIMESTAMP WITH LOCAL TIME ZONE values.
      // OracleArray maps these to OffsetDateTime, regardless of the Map
      // argument to OracleArray.getArray(Map). Oracle R2DBC defines
      // LocalDateTime as their default mapping.
      if (desiredType.isAssignableFrom(LocalDateTime.class)) {
        return (T[]) mapArray(
          (java.time.OffsetDateTime[]) array, LocalDateTime[]::new,
          OffsetDateTime::toLocalDateTime);
      }
      else if (desiredType.isAssignableFrom(LocalDate.class)) {
        return (T[]) mapArray(
          (java.time.OffsetDateTime[]) array, LocalDate[]::new,
          OffsetDateTime::toLocalDate);
      }
      else if (desiredType.isAssignableFrom(LocalTime.class)) {
        return (T[]) mapArray(
          (java.time.OffsetDateTime[]) array, LocalTime[]::new,
          OffsetDateTime::toLocalTime);
      }
    }
    else if (oracle.sql.INTERVALYM.class.isAssignableFrom(elementType)
      && desiredType.isAssignableFrom(java.time.Period.class)) {
      return (T[]) mapArray(
        (oracle.sql.INTERVALYM[]) array, java.time.Period[]::new,
        intervalym -> {
          // The binary representation is specified in the JavaDoc of
          // oracle.sql.INTERVALYM. In 21.x, the JavaDoc has bug: It neglects
          // to mention that the year value is offset by 0x80000000
          ByteBuffer byteBuffer = ByteBuffer.wrap(intervalym.shareBytes());
          return Period.of(
            byteBuffer.getInt() - 0x80000000, // 4 byte year
            (byte)(byteBuffer.get() - 60), // 1 byte month
            0); // day
        });
    }
    else if (oracle.sql.INTERVALDS.class.isAssignableFrom(elementType)
      && desiredType.isAssignableFrom(java.time.Duration.class)) {
      return (T[]) mapArray(
        (oracle.sql.INTERVALDS[]) array, java.time.Duration[]::new,
        intervalds -> {
          // The binary representation is specified in the JavaDoc of
          // oracle.sql.INTERVALDS. In 21.x, the JavaDoc has bug: It neglects
          // to mention that the day and fractional second values are offset by
          // 0x80000000
          ByteBuffer byteBuffer = ByteBuffer.wrap(intervalds.shareBytes());
          return Duration.of(
            TimeUnit.DAYS.toNanos(byteBuffer.getInt() - 0x80000000)// 4 byte day
            + TimeUnit.HOURS.toNanos(byteBuffer.get() - 60) // 1 byte hour
            + TimeUnit.MINUTES.toNanos(byteBuffer.get() - 60) // 1 byte minute
            + TimeUnit.SECONDS.toNanos(byteBuffer.get() - 60) // 1 byte second
            + byteBuffer.getInt() - 0x80000000, // 4 byte fractional second
            ChronoUnit.NANOS);
        });
    }
    else if (oracle.jdbc.OracleArray.class.isAssignableFrom(elementType)
      && desiredType.isArray()) {
      // Recursively convert a multi-dimensional array.
      return (T[]) mapArray(
        array,
        length ->
          (Object[]) java.lang.reflect.Array.newInstance(desiredType, length),
        oracleArray ->
          convertOracleArray(
            (OracleArray) oracleArray, desiredType.getComponentType()));
    }
    // OracleArray seems to support mapping TIMESTAMP WITH TIME ZONE to
    // OffsetDateTime, so that case is not handled in this method

    throw unsupportedArrayConversion(elementType, desiredType);
  }

  /**
   * Returns an exception indicating a type of elements in a Java array can
   * not be converted to a different type.
   */
  private static IllegalArgumentException unsupportedArrayConversion(
    Class<?> fromType, Class<?> toType) {
    return new IllegalArgumentException(format(
      "Conversion from array of %s to array of %s is not supported",
      fromType.getName(), toType.getName()));
  }

  /**
   * <p>
   * Maps the elements of a given {@code array} using a {@code mappingFunction},
   * and returns an array generated by an {@code arrayAllocator} that stores the
   * mapped elements.
   * </p><p>
   * The {@code array} may contain {@code null} elements. A {@code null} element
   * is automatically converted to {@code null}, and not supplied as input to
   * the {@code mappingFunction}.
   * </p>
   * @param array Array of elements to convert. Not null.
   * @param arrayAllocator Allocates an array of an input length. Not null.
   * @param mappingFunction Maps elements from the {@code array}. Not null.
   * @return Array of mapped elements.
   */
  private static <T,U> U[] mapArray(
    T[] array, IntFunction<U[]> arrayAllocator,
    Function<T, U> mappingFunction) {

    U[] result = arrayAllocator.apply(array.length);

    for (int i = 0; i < array.length; i++) {
      T arrayValue = array[i];
      result[i] = arrayValue == null
        ? null
        : mappingFunction.apply(array[i]);
    }

    return result;
  }

  /**
   * <p>
   * Converts the value of a column at the specified {@code index} to a
   * {@code Result}. This method is intended for mapping REF CURSOR values,
   * which JDBC will map to a {@link ResultSet}.
   * </p><p>
   * A REF CURSOR is closed when the JDBC statement that created it is closed.
   * To prevent the cursor from getting closed, the Result returned by this
   * method is immediately added to the collection of results that depend on the
   * JDBC statement.
   * </p><p>
   * The Result returned by this method is received by user code, and user code
   * MUST then fully consume it. The JDBC statement is not closed until the
   * result is fully consumed.
   * </p>
   * @param index 0 based column index
   * @return A column value as a {@code Result}, or null if the column value is
   * NULL.
   */
  private Result getResult(int index) {
    ResultSet resultSet = jdbcReadable.getObject(index, ResultSet.class);

    if (resultSet == null)
      return null;

    dependentCounter.increment();
    return OracleResultImpl.createQueryResult(
      dependentCounter, resultSet, adapter);
  }

  /**
   * Checks if the specified zero-based {@code index} is a valid column index
   * for this row. This method is used to verify index value parameters
   * supplied by user code.
   * @param index 0-based column index
   * @throws IndexOutOfBoundsException if the index is not valid.
   */
  private void requireValidIndex(int index) {
    if (index < 0) {
      throw new IndexOutOfBoundsException("Index is less than zero: " + index);
    }
    else if (index >= readablesMetadata.getList().size()) {
      throw new IndexOutOfBoundsException(
        "Index " + index + " is greater than or equal to column count: "
          + readablesMetadata.getList().size());
    }
  }

  /**
   * Implementation of the {@link Row} interface of the R2DBC SPI. This class
   * inherits the implementation of {@link Readable} from
   * {@link OracleReadableImpl}.
   */
  private static final class RowImpl
    extends OracleReadableImpl implements Row {

    /** Metadata for this row */
    private final RowMetadata metadata;

    /**
     * <p>
     * Constructs a new row that supplies column values from the specified
     * {@code jdbcReadable}, and uses the specified {@code rowMetadata} to
     * determine the default type mapping of column values.
     * </p>
     * @param jdbcConnection JDBC connection that created the
     *   {@code jdbcReadable}. Not null.
     * @param jdbcReadable Row data from the Oracle JDBC Driver. Not null.
     * @param metadata Meta-data for the specified row. Not null.
     * @param adapter Adapts JDBC calls into reactive streams. Not null.
     */
    private RowImpl(
      java.sql.Connection jdbcConnection, DependentCounter dependentCounter,
      JdbcReadable jdbcReadable, RowMetadataImpl metadata,
      ReactiveJdbcAdapter adapter) {
      super(jdbcConnection, dependentCounter, jdbcReadable, metadata, adapter);
      this.metadata = metadata;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implements the R2DBC SPI method by returning the metadata this
     * {@code Row} was constructed with.
     * </p>
     */
    @Override
    public RowMetadata getMetadata() {
      return metadata;
    }
  }

  /**
   * Implementation of the {@link OutParameters} interface of the R2DBC SPI.
   * This class inherits the implementation of {@link Readable} from
   * {@link OracleReadableImpl}.
   */
  private static final class OutParametersImpl
    extends OracleReadableImpl implements OutParameters  {

    private final OutParametersMetadata metadata;

    /**
     * <p>
     * Constructs a new set of out parameters that supplies values of a
     * {@code jdbcReadable} and obtains metadata of the values from
     * {@code outParametersMetaData}.
     * </p>
     * @param jdbcConnection JDBC connection that created the
     *   {@code jdbcReadable}. Not null.
     * @param jdbcReadable Readable values from a JDBC Driver. Not null.
     * @param metadata Metadata of each value. Not null.
     * @param adapter Adapts JDBC calls into reactive streams. Not null.
     */
    private OutParametersImpl(
      java.sql.Connection jdbcConnection, DependentCounter dependentCounter,
      JdbcReadable jdbcReadable, OutParametersMetadataImpl metadata,
      ReactiveJdbcAdapter adapter) {
      super(jdbcConnection, dependentCounter,jdbcReadable, metadata, adapter);
      this.metadata = metadata;
    }

    @Override
    public OutParametersMetadata getMetadata() {
      return metadata;
    }
  }

}
