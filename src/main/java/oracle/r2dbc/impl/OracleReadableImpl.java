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
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Type;
import oracle.jdbc.OracleArray;
import oracle.r2dbc.OracleR2dbcTypes;
import oracle.r2dbc.impl.ReactiveJdbcAdapter.JdbcReadable;
import oracle.r2dbc.impl.ReadablesMetadata.OutParametersMetadataImpl;
import oracle.r2dbc.impl.ReadablesMetadata.RowMetadataImpl;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
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

  /** Adapts JDBC Driver APIs into Reactive Streams APIs */
  private final ReactiveJdbcAdapter adapter;

  /** This values of this {@code Readable}. Values are supplied by a JDBC Driver */
  private final JdbcReadable jdbcReadable;

  /** Metadata of the values of this {@code Readable}. */
  private final ReadablesMetadata<?> readablesMetadata;

  /**
   * <p>
   * Constructs a new {@code Readable} that supplies values of a
   * {@code jdbcReadable} and obtains metadata of the values from
   * {@code resultMetadata}.
   * </p>
   * @param jdbcReadable Readable values from a JDBC Driver. Not null.
   * @param readablesMetadata Metadata of each value. Not null.
   * @param adapter Adapts JDBC calls into reactive streams. Not null.
   */
  private OracleReadableImpl(
    JdbcReadable jdbcReadable, ReadablesMetadata<?> readablesMetadata,
    ReactiveJdbcAdapter adapter) {
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
   * @param jdbcReadable Row data from the Oracle JDBC Driver. Not null.
   * @param metadata Meta-data for the specified row. Not null.
   * @param adapter Adapts JDBC calls into reactive streams. Not null.
   * @return A {@code Row} backed by the {@code jdbcReadable} and
   *   {@code metadata}. Not null.
   */
  static Row createRow(
    JdbcReadable jdbcReadable, RowMetadataImpl metadata,
    ReactiveJdbcAdapter adapter) {
    return new RowImpl(jdbcReadable, metadata, adapter);
  }
  /**
   * <p>
   * Creates a new {@code OutParameters} that supplies values and metadata from
   * the provided {@code jdbcReadable} and {@code rowMetadata}. The metadata
   * object is used to determine the default type mapping of column values.
   * </p>
   * @param jdbcReadable Row data from the Oracle JDBC Driver. Not null.
   * @param metadata Meta-data for the specified row. Not null.
   * @param adapter Adapts JDBC calls into reactive streams. Not null.
   * @return An {@code OutParameters} backed by the {@code jdbcReadable} and
   *   {@code metadata}. Not null.
   */
  static OutParameters createOutParameters(
    JdbcReadable jdbcReadable, OutParametersMetadataImpl metadata,
    ReactiveJdbcAdapter adapter) {
    return new OutParametersImpl(jdbcReadable, metadata, adapter);
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
   * value has a matching name, this method returns lowest index of all
   * matching values.
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
    final OracleArray jdbcArray =
      jdbcReadable.getObject(index, OracleArray.class);

    if (jdbcArray == null)
      return null;

    try {
      // For arrays of primitive types, use API extensions on OracleArray.
      if (boolean.class.equals(javaType)) {
        // OracleArray does not support conversion to boolean[], so map shorts
        // to booleans.
        short[] shorts = jdbcArray.getShortArray();
        boolean[] booleans = new boolean[shorts.length];
        for (int i = 0; i < shorts.length; i++)
          booleans[i] = shorts[i] != 0;
        return booleans;
      }
      else if (byte.class.equals(javaType)) {
        // OracleArray does not support conversion to byte[], so map shorts to
        // bytes
        short[] shorts = jdbcArray.getShortArray();
        byte[] bytes = new byte[shorts.length];
        for (int i = 0; i < shorts.length; i++)
          bytes[i] = (byte)shorts[i];
        return bytes;
      }
      else if (short.class.equals(javaType)) {
        return jdbcArray.getShortArray();
      }
      else if (int.class.equals(javaType)) {
        return jdbcArray.getIntArray();
      }
      else if (long.class.equals(javaType)) {
        return jdbcArray.getLongArray();
      }
      else if (float.class.equals(javaType)) {
        return jdbcArray.getFloatArray();
      }
      else if (double.class.equals(javaType)) {
        return jdbcArray.getDoubleArray();
      }
      else {
        // Check if Row.get(int/String) was called without a Class argument.
        // In this case, the default mapping is declared as Object[] in
        // SqlTypeMap, and this method gets called with Object.clas
        if (Object.class.equals(javaType)) {
          // The correct default mapping for the ARRAY is not actually Object[],
          // it is the default mapping of the ARRAY's element type.
          // If the element type is DATE, handle it as if it were TIMESTAMP.
          // This is consistent with how DATE columns are usually handled, and
          // reflects the fact that Oracle DATE values have a time component.
          int jdbcType = jdbcArray.getBaseType();
          Type arrayType = SqlTypeMap.toR2dbcType(
            jdbcType == Types.DATE ? Types.TIMESTAMP : jdbcType);

          // Use R2DBC's default type mapping, if the SQL type is recognized.
          // Otherwise, leave the javaType as Object.class and Oracle JDBC's
          // default type mapping will be used.
          if (arrayType != null)
            javaType = arrayType.getJavaType();
        }

        // Oracle JDBC seems to ignore the Map argument in many cases, and just
        // maps values to their default JDBC type. The convertArray method
        // should correct this by converting the array to the proper type.
        return convertArray(
          (Object[]) jdbcArray.getArray(
            Map.of(jdbcArray.getBaseTypeName(), javaType)),
          javaType);
      }
    }
    catch (SQLException sqlException) {
      throw toR2dbcException(sqlException);
    }
    finally {
      runJdbc(jdbcArray::free);
    }
  }

  /**
   * Converts a given {@code array} to an array of a specified
   * {@code elementType}. This method handles arrays returned by
   * {@link Array#getArray(Map)}, which contain objects that are the default
   * type mapping for a JDBC driver. This method converts the default JDBC
   * type mappings to the default R2DBC type mappings.
   */
  @SuppressWarnings("unchecked")
  private <T> T[] convertArray(Object[] array, Class<T> type) {

    Class<?> arrayType = array.getClass().getComponentType();

    if (type.isAssignableFrom(arrayType)) {
      return (T[])array;
    }
    else if (arrayType.equals(BigDecimal.class)) {
      if (type.equals(Boolean.class)) {
        return (T[]) mapArray(
          (BigDecimal[])array, Boolean[]::new,
          bigDecimal -> bigDecimal.shortValue() != 0);
      }
      else if (type.equals(Byte.class)) {
        return (T[]) mapArray(
          (BigDecimal[])array, Byte[]::new, BigDecimal::byteValue);
      }
      else if (type.equals(Short.class)) {
        return (T[]) mapArray(
          (BigDecimal[])array, Short[]::new, BigDecimal::shortValue);
      }
      else if (type.equals(Integer.class)) {
        return (T[]) mapArray(
          (BigDecimal[])array, Integer[]::new, BigDecimal::intValue);
      }
      else if (type.equals(Long.class)) {
        return (T[]) mapArray(
          (BigDecimal[])array, Long[]::new, BigDecimal::longValue);
      }
      else if (type.equals(Float.class)) {
        return (T[]) mapArray(
          (BigDecimal[])array, Float[]::new, BigDecimal::floatValue);
      }
      else if (type.equals(Double.class)) {
        return (T[]) mapArray(
          (BigDecimal[])array, Double[]::new, BigDecimal::doubleValue);
      }
    }
    else if (byte[].class.equals(arrayType)
      && type.isAssignableFrom(ByteBuffer.class)) {
      return (T[]) mapArray(
        (byte[][])array, ByteBuffer[]::new, ByteBuffer::wrap);
    }
    else if (java.sql.Timestamp.class.isAssignableFrom(arrayType)) {
      // Note that DATE values are represented as Timestamp by OracleArray.
      // For this reason, there is no branch for java.sql.Date conversions in
      // this method.
      if (type.isAssignableFrom(LocalDateTime.class)) {
        return (T[]) mapArray(
          (java.sql.Timestamp[]) array, LocalDateTime[]::new,
          Timestamp::toLocalDateTime);
      }
      else if (type.isAssignableFrom(LocalDate.class)) {
        return (T[]) mapArray(
          (java.sql.Timestamp[]) array, LocalDate[]::new,
          timestamp -> timestamp.toLocalDateTime().toLocalDate());
      }
      else if (type.isAssignableFrom(LocalTime.class)) {
        return (T[]) mapArray(
          (java.sql.Timestamp[]) array, LocalTime[]::new,
          timestamp -> timestamp.toLocalDateTime().toLocalTime());
      }
    }
    else if (java.time.OffsetDateTime.class.isAssignableFrom(arrayType)) {
      // This branch handles mapping from TIMESTAMP WITH LOCAL TIME ZONE values.
      // OracleArray maps these to OffsetDateTime, regardless of the Map
      // argument to OracleArray.getArray(Map). Oracle R2DBC defines
      // LocalDateTime as their default mapping.
      if (type.isAssignableFrom(LocalDateTime.class)) {
        return (T[]) mapArray(
          (java.time.OffsetDateTime[]) array, LocalDateTime[]::new,
          OffsetDateTime::toLocalDateTime);
      }
      else if (type.isAssignableFrom(LocalDate.class)) {
        return (T[]) mapArray(
          (java.time.OffsetDateTime[]) array, LocalDate[]::new,
          OffsetDateTime::toLocalDate);
      }
      else if (type.isAssignableFrom(LocalTime.class)) {
        return (T[]) mapArray(
          (java.time.OffsetDateTime[]) array, LocalTime[]::new,
          OffsetDateTime::toLocalTime);
      }
    }
    else if (oracle.sql.INTERVALYM.class.isAssignableFrom(arrayType)
      && type.isAssignableFrom(java.time.Period.class)) {
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
    else if (oracle.sql.INTERVALDS.class.isAssignableFrom(arrayType)
      && type.isAssignableFrom(java.time.Duration.class)) {
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
    // OracleArray seems to support mapping TIMESTAMP WITH TIME ZONE to
    // OffsetDateTime, so that case is not handled in this method

    throw new IllegalArgumentException(format(
      "Conversion from array of %s to array of %s is not supported",
      arrayType.getName(), type));
  }

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
   * Converts an array of {@code BigDecimal} values to objects of a given
   * type. This method handles the case where Oracle JDBC does not perform
   * conversions specified by the {@code Map} argument to
   * {@link Array#getArray(Map)}
   */
  private <T> T[] convertBigDecimalArray(
    BigDecimal[] bigDecimals, Class<T> type) {

    final Function<BigDecimal, T> mapFunction;

    if (type.equals(Byte.class)) {
      mapFunction = bigDecimal -> type.cast(bigDecimal.byteValue());
    }
    else if (type.equals(Short.class)) {
      mapFunction = bigDecimal -> type.cast(bigDecimal.shortValue());
    }
    else if (type.equals(Integer.class)) {
      mapFunction = bigDecimal -> type.cast(bigDecimal.intValue());
    }
    else if (type.equals(Long.class)) {
      mapFunction = bigDecimal -> type.cast(bigDecimal.longValue());
    }
    else if (type.equals(Float.class)) {
      mapFunction = bigDecimal -> type.cast(bigDecimal.floatValue());
    }
    else if (type.equals(Double.class)) {
      mapFunction = bigDecimal -> type.cast(bigDecimal.doubleValue());
    }
    else {
      throw new IllegalArgumentException(
        "Can not convert BigDecimal to " + type);
    }

    return Arrays.stream(bigDecimals)
      .map(mapFunction)
      .toArray(length -> {
        @SuppressWarnings("unchecked")
        T[] array = (T[])java.lang.reflect.Array.newInstance(type, length);
        return array;
      });
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
     *
     * @param jdbcReadable Row data from the Oracle JDBC Driver. Not null.
     * @param metadata Meta-data for the specified row. Not null.
     * @param adapter Adapts JDBC calls into reactive streams. Not null.
     */
    private RowImpl(
      JdbcReadable jdbcReadable,
      RowMetadataImpl metadata,
      ReactiveJdbcAdapter adapter) {
      super(jdbcReadable, metadata, adapter);
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
     *
     * @param jdbcReadable Readable values from a JDBC Driver. Not null.
     * @param metadata Metadata of each value. Not null.
     * @param adapter Adapts JDBC calls into reactive streams. Not null.
     */
    private OutParametersImpl(
      JdbcReadable jdbcReadable,
      OutParametersMetadataImpl metadata,
      ReactiveJdbcAdapter adapter) {
      super(jdbcReadable, metadata, adapter);
      this.metadata = metadata;
    }

    @Override
    public OutParametersMetadata getMetadata() {
      return metadata;
    }
  }

}
