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
package oracle.r2dbc.test;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.Statement;
import oracle.r2dbc.OracleR2dbcObject;
import oracle.r2dbc.OracleR2dbcTypes;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static oracle.r2dbc.util.Awaits.awaitOne;

public class TestUtils {

  /**
   * Queries the {@code user_errors} data dictionary view and prints all rows.
   * When writing new tests that declare a PL/SQL procedure or function,
   * "ORA-17110: executed completed with a warning" results if the PL/SQL has
   * a syntax error. The error details will be printed by calling this method.
   */
  public static void showErrors(Connection connection) {
      Flux.from(connection.createStatement(
        "SELECT * FROM user_errors ORDER BY sequence")
        .execute())
        .flatMap(result ->
          result.map((row, metadata) ->
            metadata.getColumnMetadatas()
              .stream()
              .map(ColumnMetadata::getName)
              .map(name -> name + ": " + row.get(name))
              .collect(Collectors.joining("\n"))))
      .toStream()
      .map(errorText -> "\n" + errorText)
      .forEach(System.err::println);
  }

  /**
   * Constructs an OBJECT of a given {@code objectType} with the given attribute
   * {@code attributeValues}.
   */
  public static OracleR2dbcObject constructObject(
    Connection connection, OracleR2dbcTypes.ObjectType objectType,
    Object... attributeValues) {

    Statement constructor = connection.createStatement(format(
      "{? = call %s(%s)}",
      objectType.getName(),
      Arrays.stream(attributeValues)
        .map(value ->
          // Bind the NULL literal, as SQL type of the bind value can not be
          // inferred from a null value
          value == null ? "NULL" : "?")
        .collect(Collectors.joining(","))));

    constructor.bind(0, Parameters.out(objectType));

    for (int i = 0; i < attributeValues.length; i++) {
      if (attributeValues[i] != null)
        constructor.bind(i + 1, attributeValues[i]);
    }

    return awaitOne(Flux.from(constructor.execute())
      .flatMap(result ->
        result.map(row -> row.get(0, OracleR2dbcObject.class))));
  }
}
