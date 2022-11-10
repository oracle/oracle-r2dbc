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
import io.r2dbc.spi.ReadableMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Type;
import oracle.jdbc.OracleArray;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleStruct;
import oracle.r2dbc.OracleR2dbcObject;
import oracle.r2dbc.OracleR2dbcObjectMetadata;
import oracle.r2dbc.OracleR2dbcTypes;
import oracle.r2dbc.impl.ReactiveJdbcAdapter.JdbcReadable;
import oracle.r2dbc.impl.ReadablesMetadata.OracleR2dbcObjectMetadataImpl;
import oracle.r2dbc.impl.ReadablesMetadata.OutParametersMetadataImpl;
import oracle.r2dbc.impl.ReadablesMetadata.RowMetadataImpl;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeMap;
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
      Type sqlType = readablesMetadata.get(index).getType();

      if (sqlType instanceof OracleR2dbcTypes.ArrayType) {
        value = getOracleArray(index, type);
      }
      else if (sqlType instanceof OracleR2dbcTypes.ObjectType) {
        value = getOracleObject(index, type);
      }
      else {
        // Fallback to a built-in conversion supported by Oracle JDBC
        value = jdbcReadable.getObject(index, type);
      }
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
    return jdbcReadable.getObject(index, LocalDateTime.class);
    /*
    if (OracleR2dbcTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE
          .equals(readablesMetadata.get(index).getType())) {
      // TODO: Remove this when Oracle JDBC implements a correct conversion
      Timestamp timestamp = jdbcReadable.getObject(index, Timestamp.class);
      return timestamp == null ? null : timestamp.toLocalDateTime();
    }
    else {
      return jdbcReadable.getObject(index, LocalDateTime.class);
    }
     */
  }

  /**
   * Returns an ARRAY at the given {@code index} as a specified {@code type}.
   * This method supports conversions to Java arrays of the default Java type
   * mapping for the ARRAY's element type. This conversion is the standard type
   * mapping for a COLLECTION, as defined by the R2DBC Specification.
   */
  private <T> T getOracleArray(int index, Class<T> type) {
    if (type.isArray()) {
      return type.cast(getJavaArray(index, type.getComponentType()));
    }
    else {
      // Fallback to a built-in conversion supported by Oracle JDBC
      return jdbcReadable.getObject(index, type);
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

        throw unsupportedConversion(javaArrayType, primitiveType);
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

    return mapArray(
      array,
      length -> (T[]) java.lang.reflect.Array.newInstance(desiredType, length),
      (Function<Object, T>)getMappingFunction(elementType, desiredType));
  }

  /**
   * Converts the OBJECT column at a given {@code index} to the specified
   * {@code type}. For symmetry with the object types supported by Statement
   * bind methods, this method supports conversions to
   * {@code OracleR2dbcObject}, {@code Object[]}, or
   * {@code Map<String, Object>}.
   */
  private <T> T getOracleObject(int index, Class<T> type) {

    if (type.isAssignableFrom(OracleR2dbcObject.class)) {
      // Support conversion to OracleR2dbcObject by default
      return type.cast(getOracleR2dbcObject(index));
    }
    else if (type.isAssignableFrom(Object[].class)) {
      // Support conversion to Object[] for symmetry with bind types
      return type.cast(toArray(getOracleR2dbcObject(index)));
    }
    else if (type.isAssignableFrom(Map.class)) {
      // Support conversion to Map<String, Object> for symmetry with bind
      // types
      return type.cast(toMap(getOracleR2dbcObject(index)));
    }
    else {
      // Fallback to a built-in conversion of Oracle JDBC
      return jdbcReadable.getObject(index, type);
    }
  }

  /**
   * Returns the OBJECT at a given {@code index} as an {@code OracleR2dbcObject}
   */
  private OracleR2dbcObjectImpl getOracleR2dbcObject(int index) {

    OracleStruct oracleStruct =
      jdbcReadable.getObject(index, OracleStruct.class);

    if (oracleStruct == null)
      return null;

    return new OracleR2dbcObjectImpl(
      jdbcConnection,
      dependentCounter,
      new StructJdbcReadable(oracleStruct),
      ReadablesMetadata.createAttributeMetadata(oracleStruct),
      adapter);
  }

  /**
   * Returns of an array with the default mapping of each value in a
   * {@code readable}
   */
  private static Object[] toArray(OracleReadableImpl readable) {
    if (readable == null)
      return null;

    Object[] array =
      new Object[readable.readablesMetadata.getList().size()];

    for (int i = 0; i < array.length; i++)
      array[i] = readable.get(i);

    return array;
  }

  /**
   * Returns a map containing the default mapping of each value in an
   * {@code object}, keyed to the value's name.
   */
  private static Map<String, Object> toMap(OracleReadableImpl readable) {
    if (readable == null)
      return null;

    List<? extends ReadableMetadata> metadataList =
      readable.readablesMetadata.getList();

    Map<String, Object> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    for (int i = 0; i < metadataList.size(); i++)
      map.put(metadataList.get(i).getName(), readable.get(i));

    return map;
  }

  private <T,U> Function<T,U> getMappingFunction(
    Class<T> fromType, Class<U> toType) {

    Function<? extends Object, Object> mappingFunction = null;

    if (toType.isAssignableFrom(fromType)) {
      return toType::cast;
    }
    else if (fromType.equals(BigDecimal.class)) {
      if (toType.equals(Boolean.class)) {
        mappingFunction = (BigDecimal bigDecimal) ->
          bigDecimal.shortValue() != 0;
      }
      else if (toType.equals(Byte.class)) {
        mappingFunction = (BigDecimal bigDecimal) -> bigDecimal.byteValue();
      }
      else if (toType.equals(Short.class)) {
        mappingFunction = (BigDecimal bigDecimal) -> bigDecimal.shortValue();
      }
      else if (toType.equals(Integer.class)) {
        mappingFunction = (BigDecimal bigDecimal) -> bigDecimal.intValue();
      }
      else if (toType.equals(Long.class)) {
        mappingFunction = (BigDecimal bigDecimal) -> bigDecimal.longValue();
      }
      else if (toType.equals(Float.class)) {
        mappingFunction = (BigDecimal bigDecimal) -> bigDecimal.floatValue();
      }
      else if (toType.equals(Double.class)) {
        mappingFunction = (BigDecimal bigDecimal) -> bigDecimal.doubleValue();
      }
    }
    else if (byte[].class.equals(fromType)
      && toType.isAssignableFrom(ByteBuffer.class)) {
      mappingFunction = (byte[] byteArray) -> ByteBuffer.wrap(byteArray);
    }
    else if (Timestamp.class.isAssignableFrom(fromType)) {
      // Note that DATE values are represented as Timestamp by OracleArray.
      // For this reason, there is no branch for java.sql.Date conversions in
      // this method.
      if (toType.isAssignableFrom(LocalDateTime.class)) {
        mappingFunction = (Timestamp timeStamp) -> timeStamp.toLocalDateTime();
      }
      else if (toType.isAssignableFrom(LocalDate.class)) {
        mappingFunction = (Timestamp timeStamp) ->
          timeStamp.toLocalDateTime().toLocalDate();
      }
      else if (toType.isAssignableFrom(LocalTime.class)) {
        mappingFunction = (Timestamp timeStamp) ->
          timeStamp.toLocalDateTime().toLocalTime();
      }
    }
    else if (OffsetDateTime.class.isAssignableFrom(fromType)) {
      // This branch handles mapping from TIMESTAMP WITH LOCAL TIME ZONE values.
      // Oracle JDBC maps these to OffsetDateTime. Oracle R2DBC defines
      // LocalDateTime as their default mapping.
      if (toType.isAssignableFrom(OffsetTime.class)) {
        mappingFunction = (OffsetDateTime offsetDateTime) ->
          offsetDateTime.toOffsetTime();
      }
      else if (toType.isAssignableFrom(LocalDateTime.class)) {
        mappingFunction = (OffsetDateTime offsetDateTime) ->
          offsetDateTime.toLocalDateTime();
      }
      else if (toType.isAssignableFrom(LocalDate.class)) {
        mappingFunction = (OffsetDateTime offsetDateTime) ->
          offsetDateTime.toLocalDate();
      }
      else if (toType.isAssignableFrom(LocalTime.class)) {
        mappingFunction = (OffsetDateTime offsetDateTime) ->
          offsetDateTime.toLocalTime();
      }
    }
    else if (TIMESTAMPTZ.class.isAssignableFrom(fromType)) {
      if (toType.isAssignableFrom(OffsetDateTime.class)) {
        mappingFunction = (TIMESTAMPTZ timestampWithTimeZone) ->
          fromJdbc(() -> timestampWithTimeZone.toOffsetDateTime());
      }
      else if (toType.isAssignableFrom(OffsetTime.class)) {
        mappingFunction = (TIMESTAMPTZ timestampWithTimeZone) ->
          fromJdbc(() -> timestampWithTimeZone.toOffsetTime());
      }
      else if (toType.isAssignableFrom(LocalDateTime.class)) {
        mappingFunction = (TIMESTAMPTZ timestampWithTimeZone) ->
          fromJdbc(() -> timestampWithTimeZone.toLocalDateTime());
      }
    }
    else if (TIMESTAMPLTZ.class.isAssignableFrom(fromType)) {
      if (toType.isAssignableFrom(OffsetDateTime.class)) {
        mappingFunction = (TIMESTAMPLTZ timestampWithLocalTimeZone) ->
          fromJdbc(() ->
            timestampWithLocalTimeZone.offsetDateTimeValue(jdbcConnection));
      }
      else if (toType.isAssignableFrom(OffsetTime.class)) {
        mappingFunction = (TIMESTAMPLTZ timestampWithLocalTimeZone) ->
          fromJdbc(() ->
            timestampWithLocalTimeZone.offsetTimeValue(jdbcConnection));
      }
      else if (toType.isAssignableFrom(LocalDateTime.class)) {
        mappingFunction = (TIMESTAMPLTZ timestampWithLocalTimeZone) ->
          fromJdbc(() ->
            timestampWithLocalTimeZone.localDateTimeValue(jdbcConnection));
      }
    }
    else if (INTERVALYM.class.isAssignableFrom(fromType)
      && toType.isAssignableFrom(Period.class)) {
      mappingFunction = (INTERVALYM intervalym) -> {
          // The binary representation is specified in the JavaDoc of
          // oracle.sql.INTERVALYM. In 21.x, the JavaDoc has bug: It neglects
          // to mention that the year value is offset by 0x80000000
          ByteBuffer byteBuffer = ByteBuffer.wrap(intervalym.shareBytes());
          return Period.of(
            byteBuffer.getInt() - 0x80000000, // 4 byte year
            (byte)(byteBuffer.get() - 60), // 1 byte month
            0); // day
        };
    }
    else if (INTERVALDS.class.isAssignableFrom(fromType)
      && toType.isAssignableFrom(Duration.class)) {
      mappingFunction = (INTERVALDS intervalds) -> {
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
        };
    }
    else if (java.sql.Blob.class.isAssignableFrom(fromType)
      && byte[].class.equals(toType)) {
      mappingFunction = (java.sql.Blob blob) ->
        fromJdbc(() -> blob.getBytes(1L, Math.toIntExact(blob.length())));
    }
    else if (java.sql.Clob.class.isAssignableFrom(fromType)
      && String.class.isAssignableFrom(toType)) {
      mappingFunction = (java.sql.Clob clob) ->
        fromJdbc(() -> clob.getSubString(1L, Math.toIntExact(clob.length())));
    }
    else if (OracleArray.class.isAssignableFrom(fromType)
      && toType.isArray()) {
      // Recursively convert a multi-dimensional array.
      mappingFunction = (OracleArray oracleArray) ->
        convertOracleArray(oracleArray, toType.getComponentType());
    }

    if (mappingFunction == null)
      throw unsupportedConversion(fromType, toType);

    @SuppressWarnings("unchecked")
    Function<T, U> typedMappingFunction = (Function<T, U>) mappingFunction;
    return typedMappingFunction;
  }

  /**
   * Returns an exception indicating that conversion between from one Java type
   * to another is not supported.
   */
  private static IllegalArgumentException unsupportedConversion(
    Class<?> fromType, Class<?> toType) {
    return new IllegalArgumentException(format(
      "Conversion from %s to %s is not supported",
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
      super(jdbcConnection, dependentCounter, jdbcReadable, metadata, adapter);
      this.metadata = metadata;
    }

    @Override
    public OutParametersMetadata getMetadata() {
      return metadata;
    }
  }

  private final class OracleR2dbcObjectImpl
    extends OracleReadableImpl implements OracleR2dbcObject {

    private final OracleR2dbcObjectMetadata metadata;

    /**
     * <p>
     * Constructs a new set of out parameters that supplies values of a
     * {@code jdbcReadable} and obtains metadata of the values from
     * {@code outParametersMetaData}.
     * </p>
     * @param jdbcConnection JDBC connection that created the
     *   {@code jdbcReadable}. Not null.
     * @param structJdbcReadable Readable values from a JDBC Driver. Not null.
     * @param metadata Metadata of each value. Not null.
     * @param adapter Adapts JDBC calls into reactive streams. Not null.
     */
    private OracleR2dbcObjectImpl(
      java.sql.Connection jdbcConnection,
      DependentCounter dependentCounter,
      StructJdbcReadable structJdbcReadable,
      OracleR2dbcObjectMetadataImpl metadata,
      ReactiveJdbcAdapter adapter) {
      super(
        jdbcConnection, dependentCounter, structJdbcReadable, metadata, adapter);
      this.metadata = metadata;
    }

    @Override
    public OracleR2dbcObjectMetadata getMetadata() {
      return metadata;
    }

    @Override
    public String toString() {
      return format(
        "%s = %s",
        metadata.getObjectType().getName(),
        toMap(this));
    }
  }

  /** A {@code JdbcReadable} backed by a java.sql.Struct */
  private final class StructJdbcReadable implements JdbcReadable {

    /** Attributes of the Struct, mapped to their default Java type for JDBC */
    private final Object[] attributes;

    private StructJdbcReadable(Struct struct) {
      attributes = fromJdbc(struct::getAttributes);
    }

    @Override
    public <T> T getObject(int index, Class<T> type) {
      Object attribute = attributes[index];

      if (attribute == null)
        return null;

      if (type.isInstance(attribute))
        return type.cast(attribute);

      @SuppressWarnings("unchecked")
      Function<Object, T> mappingFunction =
        (Function<Object, T>) getMappingFunction(attribute.getClass(), type);
      return mappingFunction.apply(attribute);
    }
  }

}
