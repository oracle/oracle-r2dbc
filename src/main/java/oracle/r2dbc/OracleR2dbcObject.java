/*
  Copyright (c) 2020, 2022, Oracle and/or its affiliates.

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

/**
 * <p>
 * A {@link io.r2dbc.spi.Readable} that represents an instance of a user 
 * defined OBJECT type.
 * </p><p>
 * An OBJECT returned by a {@link io.r2dbc.spi.Result} may be mapped to an
 * {@code OracleR2dbcObject}:
 * <pre>{@code
 * Publisher<Pet> objectMapExample(Result result) {
 *   return result.map(row -> {
 *
 *     OracleR2dbcObject oracleObject = row.get(0, OracleR2dbcObject.class); 
 *
 *     return new Pet(
 *       oracleObject.get("name", String.class),
 *       oracleObject.get("species", String.class),
 *       oracleObject.get("weight", Float.class),
 *       oracleObject.get("birthday", LocalDate.class));
 *   });
 * }
 *
 * }</pre>
 * As seen in the example above, the values of an OBJECT's attributes may be 
 * accessed by name with {@link #get(String)} or {@link #get(String, Class)}.
 * Alternatively, attribute values may be accessed by index with {@link #get(int)} or
 * {@link #get(int, Class)}. The {@code get} methods support all standard
 * SQL-to-Java type mappings defined by the
 * <a href="https://r2dbc.io/spec/1.0.0.RELEASE/spec/html/#datatypes.mapping">
 * R2DBC Specification.
 * </a>
 * <p>
 * Instances of {@code OracleR2dbcObject} may be set as a bind value when 
 * passed to {@link io.r2dbc.spi.Statement#bind(int, Object)} or
 * {@link io.r2dbc.spi.Statement#bind(String, Object)}:
 * <pre>{@code
 * Publisher<Result> objectBindExample(
 *   OracleR2dbcObject oracleObject, Connection connection) {
 *
 *   Statement statement =
 *     connection.createStatement("INSERT INTO petTable VALUES (:petObject)");
 *   
 *   statement.bind("petObject", oracleObject);
 * 
 *   return statement.execute();
 * }
 * }</pre>
 */
public interface OracleR2dbcObject extends io.r2dbc.spi.Readable {

  /**
   * Returns metadata for the attributes of this OBJECT.
   * @return The metadata of this OBJECT's attributes. Not null.
   */
  OracleR2dbcObjectMetadata getMetadata();

}
