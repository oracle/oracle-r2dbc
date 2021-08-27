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

import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

import static java.util.Arrays.asList;
import static oracle.r2dbc.test.DatabaseConfig.connectTimeout;
import static oracle.r2dbc.test.DatabaseConfig.sharedConnection;
import static oracle.r2dbc.util.Awaits.awaitError;
import static oracle.r2dbc.util.Awaits.awaitExecution;
import static oracle.r2dbc.util.Awaits.awaitNone;
import static oracle.r2dbc.util.Awaits.awaitOne;
import static oracle.r2dbc.util.Awaits.awaitUpdate;

/**
 * Verifies that
 * {@link OracleReadableImpl} implements behavior that is specified in it's class
 * and method level javadocs.
 */
public class OracleReadableImplTest {

  /**
   * Verifies the implementation of
   * {@link OracleReadableImpl#get(int)}
   */
  @Test
  public void testGetByIndex() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // INSERT and SELECT rows from this table
      awaitExecution(connection.createStatement(
        "CREATE TABLE testGetByIndex (x NUMBER, y NUMBER)"));
      try {
        BigDecimal xValue = BigDecimal.ZERO;
        BigDecimal yValue = BigDecimal.ONE;
        awaitUpdate(1, connection.createStatement(
          "INSERT INTO testGetByIndex (x,y) VALUES (0,1)"));

        // Expect IllegalArgumentException for an index less than 0
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByIndex")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get(-1))));

        // Expect IllegalArgumentException for an index greater than or equal
        // to the number of columns
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByIndex")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get(2))));

        // Expect valid indexes to return the INSERTed values
        awaitOne(asList(xValue, yValue),
          Flux.from(connection.createStatement(
            "SELECT x,y FROM testGetByIndex")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) -> asList(row.get(0), row.get(1)))
            ));
        awaitOne(asList(yValue, xValue),
          Flux.from(connection.createStatement(
            "SELECT y,x FROM testGetByIndex")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) -> asList(row.get(0), row.get(1)))
            ));
        awaitOne(asList(xValue, yValue),
          Flux.from(connection.createStatement(
            "SELECT * FROM testGetByIndex")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) -> asList(row.get(0), row.get(1)))
            ));
      }
      finally {
        awaitExecution(connection.createStatement(
          "DROP TABLE testGetByIndex"));
      }
    }
    finally {
      awaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleReadableImpl#get(String)}
   */
  @Test
  public void testGetByName() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // INSERT and SELECT rows from this table
      awaitExecution(connection.createStatement(
        "CREATE TABLE testGetByName (x NUMBER, y NUMBER)"));
      try {
        BigDecimal xValue = BigDecimal.valueOf(2);
        BigDecimal yValue = BigDecimal.valueOf(3);
        awaitUpdate(1, connection.createStatement(
          "INSERT INTO testGetByName (x,y) VALUES (2,3)"));

        // Expect IllegalArgumentException for a null name
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByName")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get(null))));

        // Expect IllegalArgumentException for unmatched names
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByName")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get("z"))));
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByName")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get("xx"))));
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByName")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get("x "))));
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByName")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get(" x"))));
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByName")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get(" "))));
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByName")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get(""))));

        // Expect valid names to return the INSERTed values
        awaitOne(asList(xValue, yValue),
          Flux.from(connection.createStatement(
            "SELECT x,y FROM testGetByName")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) -> asList(row.get("x"), row.get("y")))
            ));
        awaitOne(asList(yValue, yValue),
          Flux.from(connection.createStatement(
            "SELECT x,y FROM testGetByName")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) -> asList(row.get("y"), row.get("y")))
            ));
        awaitOne(asList(yValue, xValue),
          Flux.from(connection.createStatement(
            "SELECT y,x FROM testGetByName")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) -> asList(row.get("y"), row.get("x")))
            ));
        awaitOne(asList(xValue, yValue),
          Flux.from(connection.createStatement(
            "SELECT * FROM testGetByName")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) -> asList(row.get("x"), row.get("y")))
            ));

        // Expect case-insensitive column name matching
        awaitOne(asList(xValue, yValue, xValue.multiply(yValue)),
          Flux.from(connection.createStatement(
            "SELECT x, y, (x * y) AS product FROM testGetByName")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(row.get("X"), row.get("Y"), row.get("pRoDuCt")))
            ));

        // Expect aliased column name matching
        awaitOne(asList(xValue, yValue),
          Flux.from(connection.createStatement(
            "SELECT x AS width, y AS height FROM testGetByName")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(row.get("width"), row.get("height")))
            ));
        awaitOne(asList(xValue, yValue, xValue.multiply(yValue)),
          Flux.from(connection.createStatement(
            "SELECT " +
              "x AS width, " +
              "y AS height, " +
              "(x * y) AS area " +
              "FROM testGetByName")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(row.get("width"), row.get("height"), row.get("area")))
            ));

        // Expect value from lowest column index for a duplicate column name
        awaitOne(yValue,
          Flux.from(connection.createStatement(
            "SELECT y AS x, x FROM testGetByName")
            .execute())
            .flatMap(result -> result.map((row, metadata) ->
              row.get("x"))));
        awaitUpdate(1, connection.createStatement(
          "INSERT INTO testGetByName (x, y) VALUES (4, 5)"));
        awaitOne(asList(BigDecimal.valueOf(4), BigDecimal.valueOf(5)),
          Flux.from(connection.createStatement(
          "SELECT l.x, l.y, r.x, r.y"
          + " FROM testGetByName l, testGetByName r"
          + " WHERE l.x = 4 AND r.x = ?")
          .bind(0, xValue)
          .execute())
          .flatMap(result -> result.map((row, metadata) ->
            asList(row.get("x"), row.get("y")))
          ));
      }
      finally {
        awaitExecution(connection.createStatement(
          "DROP TABLE testGetByName"));
      }
    }
    finally {
      awaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleReadableImpl#get(int, Class)}
   */
  @Test
  public void testGetByIndexAndType() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // INSERT and SELECT rows from this table
      awaitExecution(connection.createStatement(
        "CREATE TABLE testGetByIndexAndType (x NUMBER, y NUMBER)"));
      try {
        int xValue = 0;
        int yValue = 1;
        awaitUpdate(1, connection.createStatement(
          "INSERT INTO testGetByIndexAndType (x,y) VALUES (0,1)"));

        // Expect IllegalArgumentException for a null type
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByIndexAndType")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get(0, null))));

        // Expect IllegalArgumentException for an unsupported type
        class Unsupported {}
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByIndexAndType")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) ->
                row.get(0, Unsupported.class))));

        // Expect IllegalArgumentException for an index less than 0
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByIndexAndType")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get(-1, Integer.class))));

        // Expect IllegalArgumentException for an index greater than or equal
        // to the number of columns
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByIndexAndType")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get(2, Integer.class))));

        // Expect valid indexes to return the INSERTed values
        awaitOne(asList(xValue, yValue),
          Flux.from(connection.createStatement(
            "SELECT x,y FROM testGetByIndexAndType")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(row.get(0, Integer.class), row.get(1, Integer.class)))
            ));
        awaitOne(asList(yValue, xValue),
          Flux.from(connection.createStatement(
            "SELECT y,x FROM testGetByIndexAndType")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(row.get(0, Integer.class), row.get(1, Integer.class)))
            ));
        awaitOne(asList(xValue, yValue),
          Flux.from(connection.createStatement(
            "SELECT * FROM testGetByIndexAndType")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(row.get(0, Integer.class), row.get(1, Integer.class)))
            ));
      }
      finally {
        awaitExecution(connection.createStatement(
          "DROP TABLE testGetByIndexAndType"));
      }
    }
    finally {
      awaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleReadableImpl#get(String, Class)}
   */
  @Test
  public void testGetByNameAndType() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // INSERT and SELECT rows from this table
      awaitExecution(connection.createStatement(
        "CREATE TABLE testGetByNameAndType (x NUMBER, y NUMBER)"));
      try {
        int xValue = 2;
        int yValue = 3;
        awaitUpdate(1, connection.createStatement(
          "INSERT INTO testGetByNameAndType (x,y) VALUES (2,3)"));

        // Expect IllegalArgumentException for a null type
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByNameAndType")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get("x", null))));

        // Expect IllegalArgumentException for an unsupported type
        class Unsupported {}
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByNameAndType")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) ->
                row.get("x", Unsupported.class))));

        // Expect IllegalArgumentException for a null name
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByNameAndType")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get(null, Integer.class))));

        // Expect IllegalArgumentException for unmatched names
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByNameAndType")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get("z", Integer.class))));
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByNameAndType")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get("xx", Integer.class))));
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByNameAndType")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get("x ", Integer.class))));
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByNameAndType")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get(" x", Integer.class))));
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByNameAndType")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get(" ", Integer.class))));
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetByNameAndType")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> row.get("", Integer.class))));

        // Expect valid names to return the INSERTed values
        awaitOne(asList(xValue, yValue),
          Flux.from(connection.createStatement(
            "SELECT x,y FROM testGetByNameAndType")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  row.get("x", Integer.class), row.get("y", Integer.class)))
            ));
        awaitOne(asList(yValue, yValue),
          Flux.from(connection.createStatement(
            "SELECT x,y FROM testGetByNameAndType")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  row.get("y", Integer.class), row.get("y", Integer.class)))
            ));
        awaitOne(asList(yValue, xValue),
          Flux.from(connection.createStatement(
            "SELECT y,x FROM testGetByNameAndType")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  row.get("y", Integer.class), row.get("x", Integer.class)))
            ));
        awaitOne(asList(xValue, yValue),
          Flux.from(connection.createStatement(
            "SELECT * FROM testGetByNameAndType")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  row.get("x", Integer.class), row.get("y", Integer.class)))
            ));

        // Expect case-insensitive column name matching
        awaitOne(asList(xValue, yValue, xValue * yValue),
          Flux.from(connection.createStatement(
            "SELECT x, y, (x * y) AS product FROM testGetByNameAndType")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  row.get("X", Integer.class),
                  row.get("Y", Integer.class),
                  row.get("pRoDuCt", Integer.class)))
            ));

        // Expect aliased column name matching
        awaitOne(asList(xValue, yValue),
          Flux.from(connection.createStatement(
            "SELECT x AS width, y AS height FROM testGetByNameAndType")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  row.get("width", Integer.class),
                  row.get("height", Integer.class)))
            ));
        awaitOne(asList(xValue, yValue, xValue * yValue),
          Flux.from(connection.createStatement(
            "SELECT " +
              "x AS width, " +
              "y AS height, " +
              "(x * y) AS area " +
              "FROM testGetByNameAndType")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  row.get("width", Integer.class),
                  row.get("height", Integer.class),
                  row.get("area", Integer.class)))
            ));

        // Expect value from lowest column index for a duplicate column name
        awaitOne(yValue,
          Flux.from(connection.createStatement(
            "SELECT y AS x, x FROM testGetByNameAndType")
            .execute())
            .flatMap(result -> result.map((row, metadata) ->
              row.get("x", Integer.class))));
        awaitUpdate(1, connection.createStatement(
          "INSERT INTO testGetByNameAndType (x, y) VALUES (4, 5)"));
        awaitOne(asList(4, 5),
          Flux.from(connection.createStatement(
            "SELECT l.x, l.y, r.x, r.y"
              + " FROM testGetByNameAndType l, testGetByNameAndType r"
              + " WHERE l.x = 4 AND r.x = ?")
            .bind(0, xValue)
            .execute())
            .flatMap(result -> result.map((row, metadata) ->
              asList(row.get("x", Integer.class), row.get("y", Integer.class)))
            ));
      }
      finally {
        awaitExecution(connection.createStatement(
          "DROP TABLE testGetByNameAndType"));
      }
    }
    finally {
      awaitNone(connection.close());
    }
  }
}
