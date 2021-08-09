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
import oracle.r2dbc.OracleR2dbcTypes;
import oracle.r2dbc.impl.ReactiveJdbcAdapter.JdbcReadable;
import oracle.r2dbc.impl.ReadablesMetadata.OutParametersMetadataImpl;
import oracle.r2dbc.impl.ReadablesMetadata.RowMetadataImpl;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;

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
   * @param jdbcReadable Readable values from a JDBC Driver.
   * @param readablesMetadata Metadata of each value
   * @param adapter Adapts JDBC calls into reactive streams.
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
   * @param adapter Adapts JDBC calls into reactive streams.
   * @param jdbcReadable Row data from the Oracle JDBC Driver.
   * @param metadata Meta-data for the specified row.
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
   * @param adapter Adapts JDBC calls into reactive streams.
   * @param jdbcReadable Row data from the Oracle JDBC Driver.
   * @param metadata Meta-data for the specified row.
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
   * @throws IllegalArgumentException If the {@code index} is less than 0,
   * or greater than the maximum value index.
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
   * @param name The name of a value
   * @return The index of the named value within this {@code Readable}
   * @throws IllegalArgumentException If no column has a matching name.
   */
  private int indexOf(String name) {
    int columnIndex = readablesMetadata.getColumnIndex(name);
    if (columnIndex != -1)
      return columnIndex;
    else
      throw new IllegalArgumentException("Unrecognized name: " + name);
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
   * @param type Java type that the value is converted to
   * @param <T> Java type that the value is converted to
   * @return The converted column value
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
    else if (Object.class.equals(type)) {
      value = convert(index, readablesMetadata.get(index).getJavaType());
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
    else if (index >= readablesMetadata.getList().size()) {
      throw new IllegalArgumentException(
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
     * @param jdbcReadable Row data from the Oracle JDBC Driver.
     * @param metadata Meta-data for the specified row.
     * @param adapter Adapts JDBC calls into reactive streams.
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
     * @param jdbcReadable Readable values from a JDBC Driver.
     * @param metadata Metadata of each value
     * @param adapter Adapts JDBC calls into reactive streams.
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