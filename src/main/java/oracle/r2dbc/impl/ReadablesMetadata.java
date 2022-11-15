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

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.OutParameterMetadata;
import io.r2dbc.spi.OutParametersMetadata;
import io.r2dbc.spi.ReadableMetadata;
import io.r2dbc.spi.RowMetadata;
import oracle.jdbc.OracleStruct;
import oracle.jdbc.OracleTypeMetaData;
import oracle.r2dbc.OracleR2dbcObjectMetadata;
import oracle.r2dbc.OracleR2dbcTypes;

import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import static oracle.r2dbc.impl.OracleR2dbcExceptions.fromJdbc;

/**
 * <p>
 * Metadata for values returned in a {@link Readable} result. An instance
 * of this class implements behavior that is common to both {@link RowMetadata}
 * and {@link OutParametersMetadata}.
 * </p><p>
 * An instance of this class retains a collection of
 * {@link ReadableMetadata} objects and implements methods to access an
 * individual {@code ReadableMetadata} object by index or by name.
 * </p>
 *
 * @author  harayuanwang, michael-a-mcmahon
 * @since   0.1.0
 */
class ReadablesMetadata<T extends ReadableMetadata> {

  /**
   * An unmodifiable list of metadata for each value, ordered by the index of
   * the value within a row or set of out parameters.
   */
  private final List<T> metadataList;

  /**
   * <p>
   * Maps value names to their index in the {@link #metadataList}. This map is
   * implemented to perform case-insensitive comparisons when mapping a name
   * to an index. If a name maps to multiple indexes, then this map
   * stores the lowest index that the name maps to. This is done in accordance
   * with the specification of {@link RowMetadata#getColumnMetadata(String)}
   * which returns the first column that matches a specified name.
   */
  private final Map<String, Integer> nameIndexes;

  /**
   * Constructs a new instance which supplies metadata from an array of
   * {@code metadatas}.
   * @param metadatas Metadata from each value in a readable. Not null.
   * Retained. Not modified.
   */
  private ReadablesMetadata(T[] metadatas) {

    // Map value names to their index. Use TreeMap for case insensitive name
    // look ups. If multiple values have the same case insensitive name, map
    // the name to the lowest index of those values.
    nameIndexes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (int i = 0; i < metadatas.length; i++)
      nameIndexes.putIfAbsent(metadatas[i].getName(), i);

    metadataList = List.of(metadatas);
  }

  /**
   * Creates {@code RowMetadata} that supplies column metadata from a
   * JDBC {@code ResultSetMetaData} object.
   * @param resultSetMetaData JDBC meta data. Not null. Retained.
   * @return R2DBC {@code RowMetadata}. Not null. Not retained.
   */
  static RowMetadataImpl createRowMetadata(
    ResultSetMetaData resultSetMetaData) {
    return new RowMetadataImpl(
      ReadablesMetadata.toColumnMetadata(resultSetMetaData));
  }

  /**
   * Creates {@code OracleR2dbcObjectMetadata} that supplies attribute metadata
   * from a JDBC {@code OracleStruct} object.
   */
  static OracleR2dbcObjectMetadataImpl createAttributeMetadata(
    OracleStruct oracleStruct) {
    return fromJdbc(() ->
      new OracleR2dbcObjectMetadataImpl(
        OracleR2dbcTypes.objectType(oracleStruct.getSQLTypeName()),
        ReadablesMetadata.toColumnMetadata(
          ((OracleTypeMetaData.Struct)oracleStruct.getOracleMetaData())
            .getMetaData())));
  }

  /**
   * Creates {@code OutParametersMetadata} from an array of {@code metadata}.
   * @param metadata Metadata of out parameter values. Not null. Retained.
   * @return {@code OutParametersMetadata} backed by the provided
   * {@code metadata}
   */
  static OutParametersMetadataImpl createOutParametersMetadata(
    OutParameterMetadata[] metadata) {
    return new OutParametersMetadataImpl(metadata);
  }

  /**
   * Converts JDBC {@code ResultSetMetaData} into an array of
   * {@code ColumnMetadata}
   */
  private static ColumnMetadata[] toColumnMetadata(
    ResultSetMetaData resultSetMetaData) {

    int columnCount = fromJdbc(resultSetMetaData::getColumnCount);
    ColumnMetadata[] columnMetadataArray = new ColumnMetadata[columnCount];

    for (int i = 0; i < columnCount; i++) {
      columnMetadataArray[i] =
        OracleReadableMetadataImpl.createColumnMetadata(resultSetMetaData, i);
    }

    return columnMetadataArray;
  }

  /**
   * Returns the {@link ReadableMetadata} for one value in this result.
   *
   * @param index the value index starting at 0
   * @return the {@link ReadableMetadata} for one value in this result. Not
   * null.
   * @throws IndexOutOfBoundsException if the {@code index} is less than
   * zero or greater than the number of available values.
   * @implSpec This method implements common behavior specified for both
   * {@link RowMetadata#getColumnMetadata(int)} and
   * {@link OutParametersMetadata#getParameterMetadata(int)}
   */
  protected final T get(int index) {
    if (index < 0) {
      throw new IndexOutOfBoundsException("Negative index: " + index);
    }
    else if (index >= metadataList.size()) {
      throw new IndexOutOfBoundsException(
        "Index " + index + " exceeds the maximum index: "
          + metadataList.size());
    }
    else {
      return metadataList.get(index);
    }
  }

  /**
   * Returns the {@link ReadableMetadata} for one value in this result.
   *
   * @param name the name of the value. Not null. Value names are case
   * insensitive. When a get method contains several values with same name,
   * then the metadata of the first matching value will be returned
   * @return the {@link ReadableMetadata} for one value in this result. Not
   * null.
   * @throws IllegalArgumentException if {@code name} is {@code null}
   * @throws NoSuchElementException if there is no value with the {@code name}
   * @implSpec This method implements common behavior specified for both
   * {@link RowMetadata#getColumnMetadata(String)} and
   * {@link OutParametersMetadata#getParameterMetadata(String)}.
   */
  protected final T get(String name) {
    OracleR2dbcExceptions.requireNonNull(name, "name is null");

    // Look up with lower case name for case insensitivity
    int index = getColumnIndex(name);
    if (index == -1)
      throw new NoSuchElementException("No column found with name: " + name);

    return metadataList.get(index);
  }

  /**
   * Returns the {@link ReadableMetadata} for all values in this result.
   *
   * @return the {@link ReadableMetadata} for all values in this result. Not
   * null.
   *
   * @implSpec This method implements common behavior specified for both
   * {@link RowMetadata#getColumnMetadatas()} and
   * {@link OutParametersMetadata#getParameterMetadatas()}.
   */
  protected final List<T> getList() {
    return metadataList;
  }

  /**
   * Returns the 0-based index of the column with the specified name. This
   * method implements a case-insensitive column name match. If more than one
   * column has a matching name, this method returns the lowest index of a
   * matching column.
   * @param name The name of a column. May be null.
   * @return The index of the named column within a row, or {@code -1} if no
   * column has a matching name.
   */
  final int getColumnIndex(String name) {
    Integer index =  nameIndexes.get(name);
    return index == null ? -1 : index;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ReadablesMetadata))
      return super.equals(other);

    ReadablesMetadata<?> otherMetadata = (ReadablesMetadata<?>)other;
    return metadataList.equals(otherMetadata.metadataList);
  }

  @Override
  public int hashCode() {
    return metadataList.hashCode();
  }

  static final class RowMetadataImpl
    extends ReadablesMetadata<ColumnMetadata> implements RowMetadata {

    /**
     * Constructs a new instance which supplies metadata from an array of
     * {@code columnMetadata}.
     *
     * @param columnMetadata Metadata from each column in a row. Not null.
     *   Retained. Not modified.
     */
    private RowMetadataImpl(ColumnMetadata[] columnMetadata) {
      super(columnMetadata);
    }

    @Override
    public ColumnMetadata getColumnMetadata(int index) {
      return get(index);
    }

    @Override
    public ColumnMetadata getColumnMetadata(String name) {
      return get(name);
    }

    @Override
    public List<? extends ColumnMetadata> getColumnMetadatas() {
      return getList();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implements the R2DBC SPI method by returning {@code true} if the
     * {@code columnName} maps to a valid index. If the {@code columnName} is
     * {@code null}, this method returns {@code false}.
     * </p>
     */
    @Override
    public boolean contains(String columnName) {
      return getColumnIndex(columnName) != -1;
    }
  }

  static final class OutParametersMetadataImpl
    extends ReadablesMetadata<OutParameterMetadata>
    implements OutParametersMetadata {

    /**
     * Constructs a new instance which supplies metadata from an array of {@code
     * columnMetadata}.
     *
     * @param columnMetadata Metadata from each column in a row. Not null.
     *   Retained. Not modified.
     */
    private OutParametersMetadataImpl(
      OutParameterMetadata[] columnMetadata) {
      super(columnMetadata);
    }

    @Override
    public OutParameterMetadata getParameterMetadata(int index) {
      return get(index);
    }

    @Override
    public OutParameterMetadata getParameterMetadata(String name) {
      return get(name);
    }

    @Override
    public List<? extends OutParameterMetadata> getParameterMetadatas() {
      return getList();
    }
  }

  static final class OracleR2dbcObjectMetadataImpl
    extends ReadablesMetadata<ReadableMetadata>
    implements OracleR2dbcObjectMetadata {

    /** Type of the OBJECT which metadata is provided for */
    private final OracleR2dbcTypes.ObjectType objectType;

    /**
     * Constructs a new instance which supplies metadata from an array of {@code
     * columnMetadata}.
     *
     * @param attributeMetadata Metadata from each column in a row. Not null.
     *   Retained. Not modified.
     */
    private OracleR2dbcObjectMetadataImpl(
      OracleR2dbcTypes.ObjectType objectType,
      ReadableMetadata[] attributeMetadata) {
      super(attributeMetadata);
      this.objectType = objectType;
    }

    @Override
    public OracleR2dbcTypes.ObjectType getObjectType() {
      return objectType;
    }

    @Override
    public ReadableMetadata getAttributeMetadata(int index) {
      return get(index);
    }

    @Override
    public ReadableMetadata getAttributeMetadata(String name) {
      return get(name);
    }

    @Override
    public List<ReadableMetadata> getAttributeMetadatas() {
      return getList();
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof OracleR2dbcObjectMetadata))
        return false;

      OracleR2dbcObjectMetadata otherMetadata =
        (OracleR2dbcObjectMetadata) other;

      if (!objectType.equals(otherMetadata.getObjectType()))
        return false;

      return super.equals(other);
    }
  }

}