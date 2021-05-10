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

import java.sql.ParameterMetaData;
import java.sql.ResultSetMetaData;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.stream.IntStream;

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
  private final HashMap<String, Integer> columnNameIndexes;

  /**
   * An unmodifiable collection of names for each column in the row. This list
   * is ordered by the index of the named column. This list is used by SPI
   * methods which return column names.
   */
  private final ColumnNamesCollection columnNames;

  /**
   * Constructs a new instance which supplies metadata from the specified
   * {@code columnMetadata}.
   * @param columnMetadata Metadata from each column in a row. Not null.
   * Retained. Not modified.
   */
  OracleRowMetadataImpl(OracleColumnMetadataImpl[] columnMetadata) {
    // TODO: Use TreeMap with case insensitive comparator?
    columnNameIndexes = new HashMap<>(columnMetadata.length);
    for (int i = 0; i < columnMetadata.length; i++) {
      // Store lower case names for case insensitivity
      columnNameIndexes.putIfAbsent(
        columnMetadata[i].getName().toLowerCase(), i);
    }

    columnMetadataList =
      Collections.unmodifiableList(Arrays.asList(columnMetadata));
    columnNames = new ColumnNamesCollection();
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
   * Implements the R2DBC SPI method by returning an {@code Iterable} of column
   * metadata that imposes the same column ordering as the query result. The
   * returned {@code Iterable} implements {@link Iterable#iterator()} to
   * return an {@code Iterator} that does not support {@link Iterator#remove()}.
   * </p>
   */
  @Override
  public Iterable<OracleColumnMetadataImpl> getColumnMetadatas() {
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
    return columnNames;
  }

  /**
   * Returns the 0-based index of the column with the specified name. This
   * method implements a case-insensitive column name match. If more than one
   * column has a matching name, this method returns the lowest index of a
   * matching column.
   * @param name The name of a column
   * @return The index of the named column within a row, or {@code -1} if no
   * column has a matching name.
   */
  int getColumnIndex(String name) {
    // Column names keys are always lower case
    Integer index =  columnNameIndexes.get(name.toLowerCase());
    return index == null ? -1 : index;
  }

  /**
   * View of column metadata objects, with each object entry mapped to
   * {@link ColumnMetadata#getName()}. In compliance with the
   * {@link #getColumnNames()} SPI method specification, this collection is
   * unmodifiable, imposes the same column ordering as the column metadata
   * objects list, and supports case insensitive look ups.
   */
  private final class ColumnNamesCollection implements Collection<String> {

    /**
     * {@inheritDoc}
     * <p>
     * Returns true if the specified {@code name} is not null and is a
     * String matching at least one column name, ignoring case.
     * </p>
     * @param name A column name {@code String}, not null
     * @return True if a matching name is found
     * @throws NullPointerException If {@code name} is null
     * @throws ClassCastException If {@code name} is not a {@code String}
     */
    @Override
    public boolean contains(Object name) {
      if (name == null) {
        throw new NullPointerException("Argument is null");
      }
      else if (!(name instanceof String)) {
        throw new ClassCastException(
          "Argument's type is not a String: " + name.getClass());
      }
      else {
        return getColumnIndex((String)name) != -1;
      }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns true if the case insensitive match implemented by
     * {@link #contains(Object)} returns true for every element in the
     * specified {@code collection}.
     * </p>
     */
    @Override
    public boolean containsAll(Collection<?> collection) {
      Objects.requireNonNull(collection, "collection is null");
      for (Object element : collection) {
        if (! contains(element))
          return false;
      }
      return true;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns an iterator over the list of column metadata which this row
     * metadata object manages, with {@link Iterator#next()} mapping each
     * metadata object to its name.
     * </p>
     * @implNote The returned iterator throws UnsupportedOperationException
     * when {@link Iterator#remove()} is invoked.
     * @implNote The super class implements {@link #toArray()} and
     * {@link #toArray(Object[])} by invoking this method.
     */
    @Override
    public Iterator<String> iterator() {
      return new Iterator<>() {
        /** Index of the next column metadata object which is iterated over */
        int index = 0;

        /**
         * {@inheritDoc}
         * <p>
         * Returns {@code true} if the current {@link #index} is a valid
         * index in the list of column metadata.
         * </p>
         */
        @Override
        public boolean hasNext() {
          return index < columnMetadataList.size();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Returns the next column metadata object mapped to
         * {@link ColumnMetadata#getName()}, and advances the {@link #index}.
         * </p>
         */
        @Override
        public String next() {
          if (! hasNext())
            throw new NoSuchElementException();

          return columnMetadataList.get(index++).getName();
        }
      };
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns an array which stores the column names in the same order as
     * the query result.
     * </p>
     */
    @Override
    public String[] toArray() {
      String[] namesArray = new String[columnMetadataList.size()];
      for (int i = 0; i < namesArray.length; i++)
        namesArray[i] = columnMetadataList.get(i).getName();
      return namesArray;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns an array which stores the column names in the same order as
     * the query result.
     * </p>
     * @implNote As specified by the {@code Collection} interface, the
     * the {@code array} object is returned if it has capacity to store all
     * column names. A null value is set after the last column name if the
     * {@code array} capacity exceeds the number of column names. Otherwise,
     * a newly allocated array is returned if the {@code array} does not have
     * capacity to store all column names.
     * @implNote Unchecked class cast warnings are suppressed for
     * invocations of {@link java.lang.reflect.Array#newInstance(Class, int)}
     * which returns a {@code T[]} as {@code Object}.
     * @implNote Unchecked class cast warnings are suppressed for casting the
     * column name {@cod String} objects as {@code (T)} type objects which
     * are stored in a {@code T[]} array. An {@code ArrayStoreException} results
     * at runtime if {@code T} is not {@code String}.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] array) {
      Objects.requireNonNull(array, "array is null");

      int size = columnMetadataList.size();

      if (array.length < size) {
        array = (T[])java.lang.reflect.Array.newInstance(
          array.getClass().getComponentType(), size);
      }
      else if (array.length > size) {
        array[size] = null;
      }

      for (int i = 0; i < size; i++)
        array[i] = (T) columnMetadataList.get(i).getName();

      return array;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns an array allocated by the {@code generator}, which stores the
     * column names in the same order as the query result.
     * </p>
     * @implNote Unchecked class cast warnings are suppressed for casting the
     * column name {@cod String} objects as {@code (T)} type objects which
     * are stored in a {@code T[]} array. An {@code ArrayStoreException} results
     * at runtime if {@code T} is not {@code String}.
     */
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(IntFunction<T[]> generator) {
      Objects.requireNonNull(generator, "generator is null");

      T[] array = generator.apply(columnMetadataList.size());
      for (int i = 0; i < columnMetadataList.size(); i++)
        array[i] = (T) columnMetadataList.get(i).getName();
      return array;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the size of the column metadata list.
     * </p>
     * @implNote The super class implements
     * {@link AbstractCollection#isEmpty()} by invoking this method.
     */
    @Override
    public int size() {
      return columnMetadataList.size();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns true if the column metadata object list is empty.
     * </p>
     */
    @Override
    public boolean isEmpty() {
      return columnMetadataList.isEmpty();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Prevents modification by throwing {@code UnsupportedOperationException}
     * </p>
     */
    @Override
    public boolean add(String o) {
      throw modificationNotSupported();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Prevents modification by throwing {@code UnsupportedOperationException}
     * </p>
     */
    @Override
    public boolean remove(Object o) {
      throw modificationNotSupported();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Prevents modification by throwing {@code UnsupportedOperationException}
     * </p>
     */
    @Override
    public boolean addAll(Collection<? extends String> c) {
      throw modificationNotSupported();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Prevents modification by throwing {@code UnsupportedOperationException}
     * </p>
     */
    @Override
    public boolean removeIf(Predicate<? super String> filter) {
      throw modificationNotSupported();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Prevents modification by throwing {@code UnsupportedOperationException}
     * </p>
     */
    @Override
    public void clear() {
      throw modificationNotSupported();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Prevents modification by throwing {@code UnsupportedOperationException}
     * </p>
     */
    @Override
    public boolean retainAll(Collection<?> c) {
      throw modificationNotSupported();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Prevents modification by throwing {@code UnsupportedOperationException}
     * </p>
     */
    @Override
    public boolean removeAll(Collection<?> c) {
      throw modificationNotSupported();
    }

    /**
     * Returns an {@code UnsupportedOperationException} with a message
     * indicating that {@link #getColumnNames()} returns a collection which
     * is not modifiable.
     * @return A exception indicating that this collection is not modifiable.
     */
    private UnsupportedOperationException modificationNotSupported() {
      return new UnsupportedOperationException(
        "The getColumnNames() Collection is unmodifiable");
    }
  }
}