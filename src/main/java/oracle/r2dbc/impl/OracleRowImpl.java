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
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Row;

import oracle.jdbc.OracleTypes;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;

import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;

/**
 * <p>
 * Implementation of the {@link Row} SPI for Oracle Database.
 * </p><p>
 * Instances of this class supply the column values of a
 * {@link ReactiveJdbcAdapter.JdbcRow} that represents one row of data
 * from a JDBC {@link ResultSet}.
 * </p>
 *
 * @author  harayuanwang, michael-a-mcmahon
 * @since   0.1.0
 */
final class OracleRowImpl implements Row {

  /** Adapts JDBC Driver APIs into Reactive Streams APIs */
  private final ReactiveJdbcAdapter adapter;

  /** This row's column values. Supplied by a JDBC ResultSet */
  private final ReactiveJdbcAdapter.JdbcRow jdbcRow;

  /** Metadata for this row */
  private final OracleRowMetadataImpl rowMetadata;

  /**
   * <p>
   * Constructs a new row that supplies column values from the specified
   * {@code jdbcRow}, and uses the specified {@code rowMetadata} to determine
   * the default type mapping of column values.
   * </p>
   * @param adapter Adapts JDBC calls into reactive streams.
   * @param jdbcRow Row data from the Oracle JDBC Driver.
   * @param rowMetadata Meta-data for the specified row.
   */
  OracleRowImpl(
    ReactiveJdbcAdapter.JdbcRow jdbcRow,
    OracleRowMetadataImpl rowMetadata,
    ReactiveJdbcAdapter adapter) {
    this.adapter = adapter;
    this.jdbcRow = jdbcRow;
    this.rowMetadata = rowMetadata;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by using the {@code JdbcRow} that backs
   * this row to convert the specified column value into the Oracle R2DBC
   * Driver's default Java type mapping for the column's SQL type.
   * </p><p>
   * This implementation does not support mapping the {@code BLOB} SQL type to
   * {@code ByteBuffer}, nor does it support mapping the {@code CLOB} SQL
   * type to {@code String}. This implementation will map {@code BLOB} to
   * {@link Blob} and map {@code CLOB} to {@link Clob}.
   * </p>
   *
   * @implNote Mapping {@code BLOB/CLOB} to {@code ByteBuffer/String} is not
   * supported because Oracle Database allows LOBs to store terabytes of
   * data. If the Oracle R2DBC Driver were to fully materialize a LOB
   * prior to emitting this row, the amount of memory necessary to do so
   * might exceed the capacity of {@code ByteBuffer/String}, and could even
   * exceed the amount of memory available to the Java Virtual Machine.
   *
   * @throws IllegalArgumentException If the {@code index} is less than 0,
   * or greater than the maximum column index.
   */
  @Override
  public Object get(int index) {
    requireValidIndex(index);
    return convertColumnValue(index,
      rowMetadata.getColumnMetadata(index).getJavaType());
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by using the JDBC ResultSet that backs
   * this row to convert the specified column value into the specified {@code
   * type}.
   * </p><p>
   * This implementation does not support mapping the {@code BLOB} SQL type to
   * {@code ByteBuffer}, nor does it support mapping the {@code CLOB} SQL
   * type to {@code String}. This implementation only supports mapping
   * {@code BLOB} to {@link Blob} and {@code CLOB} to {@link Clob}.
   * </p>
   *
   * @implNote Mapping {@code BLOB/CLOB} to {@code ByteBuffer/String} is not
   * supported because Oracle Database allows LOBs to store terabytes of data.
   * If the Oracle R2DBC Driver were to fully materialize a LOB
   * prior to emitting this row, the amount of memory necessary to do so
   * might exceed the capacity of {@code ByteBuffer/String}, and could even
   * exceed the amount of memory available to the Java Virtual Machine.
   *
   * @throws IllegalArgumentException {@inheritDoc}
   * @throws IllegalArgumentException If the {@code index} is less than 0,
   * or greater than the maximum column index.
   * @throws IllegalArgumentException If conversion to the specified
   * {@code type} is not supported.
   */
  @Override
  public <T> T get(int index, Class<T> type) {
    requireNonNull(type, "type must not be null");
    requireValidIndex(index);
    return convertColumnValue(index, type);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by using the JDBC ResultSet that backs
   * this row to convert the specified column value into the Oracle R2DBC
   * Driver's default Java type mapping for the column's SQL type.
   * </p><p>
   * This method uses a case-insensitive column name match. If more than one
   * column has a matching name, this method returns the value of the
   * matching column with the lowest index.
   * </p><p>
   * This implementation does not support mapping the {@code BLOB} SQL type to
   * {@code ByteBuffer}, nor does it support mapping the {@code CLOB} SQL
   * type to {@code String}. This implementation will map {@code BLOB} to
   * {@link Blob} and map {@code CLOB} to {@link Clob}.
   * </p>
   *
   * @implNote Mapping {@code BLOB/CLOB} to {@code ByteBuffer/String} is not
   * supported because Oracle Database allows LOBs to store terabytes of data.
   * If the Oracle R2DBC Driver were to fully materialize a LOB
   * prior to emitting this row, the amount of memory necessary to do so
   * might exceed the capacity of {@code ByteBuffer/String}, and could even
   * exceed the amount of memory available to the Java Virtual Machine.
   *
   * @throws IllegalArgumentException {@inheritDoc}
   * @throws IllegalArgumentException If there is no column with a matching
   * {@code name}.
   */
  @Override
  public Object get(String name) {
    requireNonNull(name, "name must not be null");
    int columnIndex = getColumnIndex(name);
    return convertColumnValue(columnIndex,
      rowMetadata.getColumnMetadata(columnIndex).getJavaType());
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by using the JDBC ResultSet that backs
   * this row to convert the specified column value into the specified {@code
   * type}.
   * </p><p>
   * This method uses a case-insensitive column name match. If more than one
   * column has a matching name, this method returns the value of the
   * matching column with the lowest index.
   * </p><p>
   * This implementation does not support mapping the {@code BLOB} SQL type to
   * {@code ByteBuffer}, nor does it support mapping the {@code CLOB} SQL
   * type to {@code String}. This implementation only supports mapping
   * {@code BLOB} to {@link Blob} and {@code CLOB} to {@link Clob}.
   * </p>
   *
   * @implNote Mapping {@code BLOB/CLOB} to {@code ByteBuffer/String} is not
   * supported because Oracle Database allows LOBs to store terabytes of data.
   * If the Oracle R2DBC Driver were to fully materialize a LOB
   * prior to emitting this row, the amount of memory necessary to do so
   * might exceed the capacity of {@code ByteBuffer/String}, and could even
   * exceed the amount of memory available to the Java Virtual Machine.
   *
   * @throws IllegalArgumentException {@inheritDoc}
   * @throws IllegalArgumentException If conversion to the specified
   * {@code type} is not supported.
   */
  @Override
  public <T> T get(String name, Class<T> type) {
    requireNonNull(name, "name must not be null");
    requireNonNull(type, "type must not be null");
    return convertColumnValue(getColumnIndex(name), type);
  }

  /**
   * Returns the 0-based index of the column with the specified name. This
   * method implements a case-insensitive column name match. If more than one
   * column has a matching name, this method returns lowest index of the
   * matching column with the lowest index.
   * @param name The name of a column
   * @return The index of the named column within a row
   * @throws IllegalArgumentException If no column has a matching name.
   */
  private int getColumnIndex(String name) {
    int columnIndex = rowMetadata.getColumnIndex(name);
    if (columnIndex != -1)
      return columnIndex;
    else
      throw new IllegalArgumentException("Unrecognized name: " + name);
  }

  /**
   * Converts the value of a column at a specified {@code index} to the
   * specified {@code type}. This method implements conversions to target
   * types that are not supported by JDBC drivers. The Oracle R2DBC Driver
   * will implement some conversions that adapt JDBC supported types into
   * R2DBC supported types.
   *
   * @param index 0-based index of a column
   * @param type Class of the type that the column value is converted to
   * @param <T> The type that the column value is converted to
   * @return The converted column value
   * @throws R2dbcException If the conversion is not supported.
   */
  private <T> T convertColumnValue(int index, Class<T> type) {
    requireSupportedTypeMapping(index, type);

    if (type.equals(ByteBuffer.class))
      return type.cast(getByteBuffer(index));
    else if (type.equals(io.r2dbc.spi.Blob.class))
      return type.cast(getBlob(index));
    else if (type.equals(io.r2dbc.spi.Clob.class))
      return type.cast(getClob(index));
    else if (type.equals(LocalDateTime.class))
      return type.cast(getLocalDateTime(index));
    else
      return jdbcRow.getObject(index, type);
  }

  /**
   * <p>
   * Converts the value of a column at the specified {@code index} to a
   * {@code ByteBuffer}.
   * </p><p>
   * A JDBC driver is not required to support {@code ByteBuffer} conversions
   * for any SQL type, so this method is necessary to implement the
   * conversion to {@code ByteBuffer} from a type that is supported by JDBC.
   * </p><p>
   * This method should NOT be called when the database column type is BLOB.
   * The JDBC driver may require blocking network I/O in order to materialize a
   * BLOB as a ByteBuffer.
   * </p>
   * @param index 0 based column index
   * @return A column value as a {@code ByteBuffer}, or null if the column
   * value is NULL.
   */
  private ByteBuffer getByteBuffer(int index) {
    byte[] columnValue = jdbcRow.getObject(index, byte[].class);
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
    java.sql.Blob jdbcBlob = jdbcRow.getObject(index, java.sql.Blob.class);
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
    final java.sql.Clob jdbcClob;
    switch (getColumnTypeNumber(index)) {
      case Types.NCLOB:
      case Types.LONGNVARCHAR:
      case Types.NVARCHAR:
      case Types.NCHAR:
        jdbcClob = jdbcRow.getObject(index, java.sql.NClob.class);
        break;
      default:
        jdbcClob = jdbcRow.getObject(index, java.sql.Clob.class);
        break;
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
    if (getColumnTypeNumber(index) == OracleTypes.TIMESTAMPLTZ) {
      Timestamp timestamp = jdbcRow.getObject(index, Timestamp.class);
      return timestamp == null ? null : timestamp.toLocalDateTime();
    }
    else {
      return jdbcRow.getObject(index, LocalDateTime.class);
    }
  }

  /**
   * Checks if the specified zero-based {@code index} is a valid column index
   * for this row. This method is used to verify index value parameters
   * supplied by user code.
   * @param index 0-based column index
   * @throws IllegalStateException if the index is not valid.
   */
  private void requireValidIndex(int index) {
    if (index < 0) {
      throw new IllegalArgumentException("Index is less than zero: " + index);
    }
    else if (index >= rowMetadata.getColumnNames().size()) {
      throw new IllegalArgumentException(
        "Index " + index + " is greater than or equal to column count: "
          + rowMetadata.getColumnNames().size());
    }
  }

  /**
   * <p>
   * Checks if the Oracle R2DBC Driver supports mapping the database type
   * of the column at the specified {@code index} to the Java type specified
   * as {@code type}.
   * </p><p>
   * This method handles cases where the JDBC driver may support a mapping 
   * that the Oracle R2DBC Driver does not support. For instance, the JDBC
   * driver may support mapping CLOB columns to String, but the Oracle R2DBC
   * Driver does not support this as the JDBC driver may require blocking 
   * network I/O to convert a CLOB into a String.
   * </p>
   *
   * @param index 0-based column index
   * @param type Class of the type that the column value is converted to
   * @throws R2dbcException if the type mapping is not supported
   */
  private void requireSupportedTypeMapping(int index, Class<?> type) {

    switch (getColumnTypeNumber(index)) {
      case Types.BLOB:
        if (! type.equals(Blob.class))
          throw unsupportedTypeMapping("BLOB", index, type);
        break;
      case Types.CLOB:
        if (! type.equals(Clob.class))
          throw unsupportedTypeMapping("CLOB", index, type);
        break;
    }
  }

  /**
   * Returns the SQL type number of the column at a given {@code index}. The
   * returned number identifies either a standard type defined by
   * {@link Types} or a vendor specific type defined by the JDBC driver. This
   * method returns {@link Types#OTHER} if the JDBC driver does not provide a
   * type code for the column.
   * @param index 0 based column index
   * @return A SQL type number
   */
  private int getColumnTypeNumber(int index) {
    Integer typeNumber = rowMetadata.getColumnMetadata(index)
      .getNativeTypeMetadata()
      .getVendorTypeNumber();

    return typeNumber == null ? Types.OTHER : typeNumber;
  }

  /**
   * Returns an exception indicating that the Oracle R2DBC Driver does
   * not support mapping the database type specified as {@code sqlTypeName}
   * to the Java type specified as {@code type}.
   * @param sqlTypeName Name of a SQL type, like "BLOB" or "NUMBER"
   * @param index Column index having a SQL type named {@code sqlTypeName}
   * @param type Java type to which mapping is not supported
   * @return An exception that expresses the unsupported mapping
   */
  private static R2dbcException unsupportedTypeMapping(
    String sqlTypeName, int index, Class<?> type) {
    return OracleR2dbcExceptions.newNonTransientException(
      String.format("Unsupported SQL to Java type mapping. " +
        "SQL Type: %s, Column Index: %d, Java Type: %s",
        sqlTypeName, index, type.getName()),
      null);
  }

}