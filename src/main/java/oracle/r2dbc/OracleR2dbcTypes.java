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
package oracle.r2dbc;

import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.Type;
import oracle.sql.json.OracleJsonObject;

import java.nio.ByteBuffer;
import java.sql.RowId;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.Objects;

/**
 * SQL types supported by Oracle Database that are not defined as standard types
 * by {@link io.r2dbc.spi.R2dbcType}.
 */
public final class OracleR2dbcTypes {

  private OracleR2dbcTypes() {}

  /**
   * A 64-bit, double-precision floating-point number data type.
   */
  public static final Type BINARY_DOUBLE =
    new TypeImpl(Double.class, "BINARY_DOUBLE");

  /**
   * A 32-bit, single-precision floating-point number data type.
   */
  public static final Type BINARY_FLOAT =
    new TypeImpl(Float.class, "BINARY_FLOAT");

  /**
   * Stores a period of time in days, hours, minutes, and seconds.
   */
  public static final Type INTERVAL_DAY_TO_SECOND =
    new TypeImpl(Duration.class, "INTERVAL DAY TO SECOND");

  /**
   * Stores a period of time in years and months.
   */
  public static final Type INTERVAL_YEAR_TO_MONTH =
    new TypeImpl(Period.class, "INTERVAL YEAR TO MONTH");

  /**
   * Stores a JSON value.
   */
  public static final Type JSON =
    new TypeImpl(OracleJsonObject.class, "JSON");

  /**
   * Character data of variable length up to 2 gigabytes.
   */
  public static final Type LONG =
    new TypeImpl(String.class, "LONG");

  /**
   * Raw binary data of variable length up to 2 gigabytes.
   */
  public static final Type LONG_RAW =
    new TypeImpl(ByteBuffer.class, "LONG RAW");

  /**
   * Base 64 string representing the unique address of a row in its table.
   */
  public static final Type ROWID =
    new TypeImpl(RowId.class, "ROWID");

  /**
   * Timestamp that is converted to the database's timezone when stored, and
   * converted to the local timezone (the session timezone) when retrieved.
   */
  public static final Type TIMESTAMP_WITH_LOCAL_TIME_ZONE =
    new TypeImpl(LocalDateTime.class, "TIMESTAMP WITH LOCAL TIME ZONE");

  /**
   * <p>
   * Creates an {@link ArrayType} representing a user defined {@code ARRAY}
   * type. The {@code name} passed to this method must identify the name of a
   * user defined {@code ARRAY} type.
   * </p><p>
   * Typically, the name passed to this method should be UPPER CASE, unless the
   * {@code CREATE TYPE} command that created the type used an "enquoted" type
   * name.
   * </p><p>
   * The {@code ArrayType} object returned by this method may be used to create
   * a {@link Parameter} that binds an array value to a {@link Statement}.
   * </p><pre>{@code
   * Publisher<Result> arrayBindExample(Connection connection) {
   *   Statement statement =
   *     connection.createStatement("INSERT INTO example VALUES (:array_bind)");
   *
   *   // Use the name defined for an ARRAY type:
   *   // CREATE TYPE MY_ARRAY AS ARRAY(8) OF NUMBER
   *   ArrayType arrayType = OracleR2dbcTypes.arrayType("MY_ARRAY");
   *   Integer[] arrayValues = {1, 2, 3};
   *   statement.bind("arrayBind", Parameters.in(arrayType, arrayValues));
   *
   *   return statement.execute();
   * }
   * }</pre>
   * @param name Name of a user defined ARRAY type. Not null.
   * @return A {@code Type} object representing the user defined ARRAY type. Not
   * null.
   */
  public static ArrayType arrayType(String name) {
    return new ArrayTypeImpl(Objects.requireNonNull(name, "name is null"));
  }

  /**
   * Extension of the standard {@link Type} interface used to represent user
   * defined ARRAY types. An instance of {@code ArrayType} must be used when
   * binding an array value to a {@link Statement} created by the Oracle R2DBC
   * Driver.
   * </p><p>
   * Oracle Database does not support an anonymous {@code ARRAY} type, which is
   * what the standard {@link R2dbcType#COLLECTION} type represents. Oracle
   * Database only supports {@code ARRAY} types which are declared as a user
   * defined type, as in:
   * <pre>{@code
   * CREATE TYPE MY_ARRAY AS ARRAY(8) OF NUMBER
   * }</pre>
   * In order to bind an array, the name of a user defined ARRAY type must
   * be known to Oracle R2DBC. Instances of {@code ArrayType} retain the name
   * that is provided to the {@link #arrayType(String)} factory method.
   */
  public interface ArrayType extends Type {

    /**
     * {@inheritDoc}
     * Returns {@code Object[].class}, which is the standard mapping for
     * {@link R2dbcType#COLLECTION}. The true default type mapping is the array
     * variant of the default mapping for the element type of the {@code ARRAY}.
     * For instance, an {@code ARRAY} of {@code VARCHAR} maps to a
     * {@code String[]} by default.
     */
    @Override
    Class<?> getJavaType();

    /**
     * {@inheritDoc}
     * Returns the name of this user defined {@code ARRAY} type. For instance,
     * this method returns "MY_ARRAY" if the type is declared as:
     * <pre>{@code
     * CREATE TYPE MY_ARRAY AS ARRAY(8) OF NUMBER
     * }</pre>
     */
    @Override
    String getName();
  }

  /** Concrete implementation of the {@code ArrayType} interface */
  private static final class ArrayTypeImpl
    extends TypeImpl implements ArrayType {

    /**
     * Constructs an ARRAY type with the given {@code name}. The constructed
     * {@code ArrayType} as a default Java type mapping of
     * {@code Object[].class}. This is consistent with the standard
     * {@link R2dbcType#COLLECTION} type.
     * @param name User defined name of the type. Not null.
     */
    ArrayTypeImpl(String name) {
      super(Object[].class, name);
    }
  }

  /**
   * Implementation of the {@link Type} SPI.
   */
  private static class TypeImpl implements Type {

    /**
     * The Java Language mapping of this SQL type.
     */
    private final Class<?> javaType;

    /**
     * The name of this SQL type, as it would appear in a DDL expression.
     */
    private final String sqlName;

    /**
     * Constructs a {@code Type} having a {@code javaType} mapping and
     * {@code sqlName}.
     * @param javaType Java type
     * @param sqlName SQL type name
     */
    TypeImpl(Class<?> javaType, String sqlName) {
      this.javaType = javaType;
      this.sqlName = sqlName;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implements the R2DBC SPI method by returning the default Java type
     * mapping for values of this SQL type. The Java type returned by this
     * method is the type of {@code Object} returned by {@code Row.get
     * (String/int)} when accessing a value of this SQL type.
     * </p>
     */
    @Override
    public Class<?> getJavaType() {
      return javaType;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implements the R2DBC SPI method by returning the name of this SQL type.
     * The name returned by this method is recognized in expressions of a SQL
     * command, for instance: A column definition of a {@code CREATE TABLE}
     * command.
     * </p>
     *
     * @return
     */
    @Override
    public String getName() {
      return sqlName;
    }

    /**
     * Returns the name of this type.
     * @return Type name
     */
    @Override
    public String toString() {
      return getName();
    }
  }

}
