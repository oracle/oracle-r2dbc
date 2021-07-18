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
import io.r2dbc.spi.RowMetadata;

import java.sql.ResultSetMetaData;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import static oracle.r2dbc.impl.OracleR2dbcExceptions.getOrHandleSQLException;

/**
 * <p>
 * Implementation of {@link RowMetadata} for Oracle Database.
 * </p><p>
 * This implementation manages a collection of {@link ColumnMetadata}
 * objects. The metadata object collection is exposed through the SPI methods
 * which this class implements.
 * </p>
 *
 * @author  harayuanwang, michael-a-mcmahon
 * @since   0.1.0
 */
final class OracleRowMetadataImpl implements RowMetadata {

  /**
   * An unmodifiable list of metadata for each column in the row. This list is
   * ordered by the index of the metadata's column. This list is used by SPI
   * methods which perform index based look ups.
   */
  private final List<OracleColumnMetadataImpl> columnMetadataList;

  /**
   * <p>
   * Maps column names to their index in the list of column metadata objects.
   * This map is used by SPI methods which perform case insensitive name
   * based look ups. The mapped index can then be used for index based look
   * ups in the list of column metadata objects.
   * </p><p>
   * To support case insensitive look ups, this map stores column names in all
   * lower case characters. When a name is provided for a case insensitive
   * look up, converting that name to all lower case characters will allow it
   * to match an entry in this map, regardless of which case was originally used
   * by the application and database.
   * </p>
   * If multiple columns have the same name, this map
   * stores the lowest index of those columns. This is done in accordance
   * with the specification of {@link #getColumnMetadata(String)} which
   * returns the first column that matches a specified name.
   */
  private final Map<String, Integer> columnNameIndexes;

  /**
   * Constructs a new instance which supplies metadata from the specified
   * {@code columnMetadata}.
   * @param columnMetadata Metadata from each column in a row. Not null.
   * Retained. Not modified.
   */
  OracleRowMetadataImpl(OracleColumnMetadataImpl[] columnMetadata) {

    // Map column names to their index. Use TreeMap for case insensitive name
    // look ups. If multiple columns have the same case insensitive name, map
    // the name to the lowest index of those columns.
    columnNameIndexes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (int i = 0; i < columnMetadata.length; i++)
      columnNameIndexes.putIfAbsent(columnMetadata[i].getName(), i);

    columnMetadataList = List.of(columnMetadata);
  }

  /**
   * Creates {@code RowMetadata} that supplies column metadata from a
   * JDBC {@code ResultSetMetaData} object.
   * @param resultSetMetaData JDBC meta data. Not null. Retained.
   * @return R2DBC {@code RowMetadata}. Not null. Not retained.
   */
  static OracleRowMetadataImpl createRowMetadata(ResultSetMetaData resultSetMetaData) {
    int columnCount =
      getOrHandleSQLException(resultSetMetaData::getColumnCount);
    OracleColumnMetadataImpl[] columnMetadataArray =
      new OracleColumnMetadataImpl[columnCount];

    for (int i = 0; i < columnCount; i++) {
      columnMetadataArray[i] =
        OracleColumnMetadataImpl.fromResultSetMetaData(resultSetMetaData, i);
    }

    return new OracleRowMetadataImpl(columnMetadataArray);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning the column metadata at the
   * provided {@code index} if it is within the range of valid column indexes.
   * </p>
   */
  @Override
  public OracleColumnMetadataImpl getColumnMetadata(int index) {
    if (index < 0) {
      throw new ArrayIndexOutOfBoundsException("Negative index: " + index);
    }
    else if (index > columnMetadataList.size()) {
      throw new ArrayIndexOutOfBoundsException(
        "Index " + index + " exceeds the maximum column index: "
          + columnMetadataList.size());
    }
    else {
      return columnMetadataList.get(index);
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by performing a case insensitive name
   * based look up for a column with the provided {@code name}. If the look
   * up finds a match, the {@code name} is mapped to the index of a column.
   * If more than one column has a matching name, this method returns the
   * metadata of the column at the lowest index.
   * </p>
   */
  @Override
  public OracleColumnMetadataImpl getColumnMetadata(String name) {
    OracleR2dbcExceptions.requireNonNull(name, "name is null");

    // Look up with lower case name for case insensitivity
    int index = getColumnIndex(name);
    if (index == -1)
      throw new NoSuchElementException("No column found with name: " + name);

    return columnMetadataList.get(index);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning a {@code List} of column
   * metadata that imposes the same column ordering as the query result. The
   * returned {@code List} implements {@link Iterable#iterator()} to
   * return an {@code Iterator} that does not support {@link Iterator#remove()}.
   * </p>
   */
  @Override
  public List<? extends ColumnMetadata> getColumnMetadatas() {
    return columnMetadataList;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning a view of the column metadata
   * objects, with each list entry mapped to {@link ColumnMetadata#getName()}.
   * As specified by the SPI method documentation, the returned collection is
   * unmodifiable, imposes the same column ordering as the query result, and
   * supports case insensitive look ups.
   * </p>
   */
  @Override
  public Collection<String> getColumnNames() {
    throw new UnsupportedOperationException(
      "The getColumnNames method is deprecated for removal. Use the " +
        "contains(String) or getColumnMetadatas() methods instead.");
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

  /**
   * Returns the 0-based index of the column with the specified name. This
   * method implements a case-insensitive column name match. If more than one
   * column has a matching name, this method returns the lowest index of a
   * matching column.
   * @param name The name of a column. May be null.
   * @return The index of the named column within a row, or {@code -1} if no
   * column has a matching name.
   */
  int getColumnIndex(String name) {
    Integer index =  columnNameIndexes.get(name);
    return index == null ? -1 : index;
  }

}