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
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.Type;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.sql.RowId;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static oracle.r2dbc.DatabaseConfig.connectTimeout;
import static oracle.r2dbc.DatabaseConfig.newConnection;
import static oracle.r2dbc.DatabaseConfig.sharedConnection;
import static oracle.r2dbc.DatabaseConfig.showErrors;
import static oracle.r2dbc.util.Awaits.awaitError;
import static oracle.r2dbc.util.Awaits.awaitExecution;
import static oracle.r2dbc.util.Awaits.awaitMany;
import static oracle.r2dbc.util.Awaits.awaitNone;
import static oracle.r2dbc.util.Awaits.awaitOne;
import static oracle.r2dbc.util.Awaits.awaitQuery;
import static oracle.r2dbc.util.Awaits.awaitUpdate;
import static oracle.r2dbc.util.Awaits.consumeOne;
import static oracle.r2dbc.util.Awaits.tryAwaitExecution;
import static oracle.r2dbc.util.Awaits.tryAwaitNone;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies that
 * {@link OracleStatementImpl} implements behavior that is specified in it's
 * class and method level javadocs.
 */
public class OracleStatementImplTest {

  /**
   * Verifies the implementation of
   * {@link OracleStatementImpl#bind(int, Object)}
   */
  @Test
  public void testBindByIndex() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // INSERT and SELECT from this table with a parameterized statements
      awaitExecution(connection.createStatement(
        "CREATE TABLE testBindByIndex (x NUMBER, y NUMBER)"));

      // Expect bind values to be applied in VALUES clause
      awaitUpdate(
        asList(1, 1, 1, 1),
        connection
          .createStatement("INSERT INTO testBindByIndex VALUES (?, ?)")
          .bind(0, 0).bind(1, 0).add()
          .bind(0, 1).bind(1, 0).add()
          .bind(0, 1).bind(1, 1).add()
          .bind(0, 1).bind(1, 2).add());

      // Expect bind values to be applied in WHERE clause as:
      // SELECT x, y FROM testBindByIndex WHERE x = 1 AND y > 0
      awaitQuery(
        asList(asList(1, 1), asList(1, 2)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y" +
            " FROM testBindByIndex" +
            " WHERE x = ? and y > ?" +
            " ORDER BY x, y")
          .bind(0, 1).bind(1, 0));

      Statement statement = connection.createStatement(
        "SELECT x FROM testBindByIndex WHERE x > ?");

      // Expect IllegalArgumentException for a null value
      assertThrows(
        IllegalArgumentException.class,
        () -> statement.bind(0, null));
      assertThrows(
        IllegalArgumentException.class,
        () -> statement.bind(1, null));

      // Expect IllegalArgumentException for an unsupported conversion
      class UnsupportedType { }
      assertThrows(
        IllegalArgumentException.class,
        () -> statement.bind(0, new UnsupportedType()));

      // Expect IndexOutOfBoundsException for an out of range index
      assertThrows(
        IndexOutOfBoundsException.class,
        () -> statement.bind(-1, 1));
      assertThrows(
        IndexOutOfBoundsException.class,
        () -> statement.bind(-2, 1));
      assertThrows(
        IndexOutOfBoundsException.class,
        () -> statement.bind(1, 1));
      assertThrows(
        IndexOutOfBoundsException.class,
        () -> statement.bind(2, 1));
      assertThrows(
        IndexOutOfBoundsException.class, () ->
          connection.createStatement("SELECT x FROM testBindByIndex")
            .bind(0, 0));

      // Expect bind values to be replaced when set more than once
      awaitUpdate(
        asList(1),
        connection
          .createStatement("INSERT INTO testBindByIndex VALUES (?, ?)")
          .bind(0, 99).bind(1, 99)
          .bind(0, 2).bind(1, 0));
      awaitUpdate(
        asList(1),
        connection
          .createStatement("INSERT INTO testBindByIndex VALUES (:x, :y)")
          .bind("x", 99).bind("y", 99)
          .bind(0, 2).bind(1, 1));
      awaitQuery(
        asList(asList(2, 0), asList(2, 1)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y FROM testBindByIndex WHERE x = 2 ORDER BY y"));

      // Expect bind values to be replaced when set more than once, after
      // calling add()
      awaitUpdate(
        asList(1, 1),
        connection
          .createStatement("INSERT INTO testBindByIndex VALUES (?, ?)")
          .bind(0, 3).bind(1, 0).add()
          .bind(0, 99).bind(1, 99)
          .bind(0, 3).bind(1, 1));
      awaitUpdate(
        asList(1, 1),
        connection
          .createStatement("INSERT INTO testBindByIndex VALUES (:x, :y)")
          .bind(0, 3).bind(1, 2).add()
          .bind("x", 99).bind("y", 99)
          .bind(0, 3).bind(1, 3));
      awaitQuery(
        asList(asList(3, 0), asList(3, 1), asList(3, 2), asList(3, 3)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y FROM testBindByIndex WHERE x = 3 ORDER BY y"));

    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testBindByIndex"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleStatementImpl#bind(String, Object)}
   */
  @Test
  public void testBindByName() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // INSERT and SELECT from this table with a parameterized statement
      awaitExecution(connection.createStatement(
        "CREATE TABLE testBindByName (x NUMBER, y NUMBER)"));

      // Expect bind values to be applied in VALUES clause
      awaitUpdate(
        asList(1, 1, 1, 1),
        connection
          .createStatement("INSERT INTO testBindByName VALUES (:X, :Y)")
          .bind("X", 0).bind("Y", 0).add()
          .bind("X", 1).bind("Y", 0).add()
          .bind("X", 1).bind("Y", 1).add()
          .bind("X", 1).bind("Y", 2).add());

      // Expect bind values to be applied in WHERE clause as:
      // SELECT x, y FROM testBindByName WHERE x = 1 AND y > 0
      awaitQuery(
        asList(asList(1, 1), asList(1, 2)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y" +
            " FROM testBindByName" +
            " WHERE x = :x and y > :y" +
            " ORDER BY x, y")
          .bind("x", 1).bind("y", 0));

      // Using a duplicate parameter name, expect bind values to be applied
      // in WHERE clause as:
      // SELECT x, y FROM testBindByName WHERE x = 1 AND y > 0 AND y < 2
      awaitQuery(
        asList(asList(1, 1)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y" +
            " FROM testBindByName" +
            " WHERE x = :x AND y > :y AND y < :y")
          .bind("x", 1).bind("y", 0).bind(2, 2));

      // Expect IllegalArgumentException for a null value
      Statement statement = connection.createStatement(
        "SELECT x FROM testBindByIndex WHERE x > :x");
      assertThrows(
        IllegalArgumentException.class,
        () -> statement.bind("x", null));

      // Expect IllegalArgumentException for a null identifier
      assertThrows(
        IllegalArgumentException.class,
        () -> statement.bind(null, 1));

      // Expect IllegalArgumentException for an unsupported conversion
      class UnsupportedType {
      }
      assertThrows(
        IllegalArgumentException.class,
        () -> statement.bind("x", new UnsupportedType()));

      // Expect IllegalArgumentException for an unmatched identifier
      assertThrows(
        IllegalArgumentException.class,
        () -> statement.bind("z", 1));
      assertThrows(
        IllegalArgumentException.class,
        () -> statement.bind("xx", 1));
      assertThrows(
        IllegalArgumentException.class,
        () -> statement.bind("", 1));
      assertThrows(
        IllegalArgumentException.class,
        () -> statement.bind("X", 1));
      assertThrows(
        IllegalArgumentException.class,
        () ->
          connection.createStatement("SELECT x FROM testBindByIndex")
            .bind("x", 0));

      // Expect bind values to be replaced when set more than once
      awaitUpdate(
        asList(1),
        connection
          .createStatement("INSERT INTO testBindByName VALUES (:x, :y)")
          .bind(0, 99).bind(1, 99)
          .bind("x", 2).bind(1, 0));
      awaitUpdate(
        asList(1),
        connection
          .createStatement("INSERT INTO testBindByName VALUES (:x, :y)")
          .bind("x", 99).bind("y", 99)
          .bind("x", 2).bind(1, 1));
      awaitUpdate(
        asList(1),
        connection
          .createStatement("INSERT INTO testBindByName VALUES (:x, :y)")
          .bind("x", 99).bind("y", 99)
          .bind("x", 2).bind("y", 2));
      awaitQuery(
        asList(asList(2, 0), asList(2, 1), asList(2, 2)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y FROM testBindByName WHERE x = 2 ORDER BY y"));

      // Expect bind values to be replaced when set more than once, after
      // calling add()
      awaitUpdate(
        asList(1, 1),
        connection
          .createStatement("INSERT INTO testBindByName VALUES (:x, :y)")
          .bind("x", 3).bind(1, 0).add()
          .bind(0, 99).bind(1, 99)
          .bind("x", 3).bind(1, 1));
      awaitUpdate(
        asList(1, 1),
        connection
          .createStatement("INSERT INTO testBindByName VALUES (:x, :y)")
          .bind("x", 3).bind("y", 2).add()
          .bind("x", 99).bind("y", 99)
          .bind("x", 3).bind("y", 3));
      awaitQuery(
        asList(asList(3, 0), asList(3, 1), asList(3, 2), asList(3, 3)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y FROM testBindByName WHERE x = 3 ORDER BY y"));

      // When the same name is used for multiple parameters, expect a value
      // bound to that name to be set as the value for all of those parameters.
      // Expect a value bound to the index of one of those parameters to be
      // set only for the parameter at that index.
      awaitUpdate(asList(1, 1, 1),
        connection
          .createStatement("INSERT INTO testBindByName VALUES (:same, :same)")
          .bind("same", 4).add()
          .bind("same", 4).bind(1, 5).add()
          .bind(0, 4).bind(1, 6));
      awaitQuery(asList(asList(4,4)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y FROM testBindByName WHERE x = :x_and_y AND y = :x_and_y")
          .bind("x_and_y", 4));
      awaitQuery(
        asList(asList(4, 5), asList(4, 6)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y FROM testBindByName" +
            " WHERE x = :both AND y <> :both" +
            " ORDER BY y")
          .bind("both", 4));
      awaitQuery(asList(asList(4,4)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y FROM testBindByName" +
            " WHERE x = :x_and_y" +
            " AND (x * y) = :x_times_y" +
            " AND y = :x_and_y")
          .bind("x_times_y", 16)
          .bind("x_and_y", 4));

    }
    finally {
      tryAwaitExecution(connection.createStatement("DROP TABLE testBindByName"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleStatementImpl#bindNull(int, Class)}
   */
  @Test
  public void testBindNullByIndex() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // INSERT into this table with a parameterized VALUES clause
      awaitExecution(connection.createStatement(
        "CREATE TABLE testBindNullByIndex (x NUMBER, y NUMBER)"));
      Statement selectStatement = connection.createStatement(
        "SELECT x, y" +
          " FROM testBindNullByIndex" +
          " WHERE x = :x and y > :y" +
          " ORDER BY x, y");

      // Expect IllegalArgumentException for a null Class
      assertThrows(
        IllegalArgumentException.class,
        () -> selectStatement.bindNull(0, null));
      assertThrows(
        IllegalArgumentException.class,
        () -> selectStatement.bindNull(1, null));

      // Expect IndexOutOfBoundsException for an out of range index
      assertThrows(
        IndexOutOfBoundsException.class,
        () -> selectStatement.bindNull(-1, Integer.class));
      assertThrows(
        IndexOutOfBoundsException.class,
        () -> selectStatement.bindNull(-2, Integer.class));
      assertThrows(
        IndexOutOfBoundsException.class,
        () -> selectStatement.bindNull(2, Integer.class));
      assertThrows(
        IndexOutOfBoundsException.class,
        () -> selectStatement.bindNull(3, Integer.class));
      assertThrows(
        IndexOutOfBoundsException.class,
        () ->
          connection.createStatement("SELECT x FROM testBindByIndex")
            .bind(0, 0));

      // Expect NULL bind values to be applied in VALUES clause
      awaitUpdate(
        asList(1, 1, 1, 1, 1, 1),
        connection
          .createStatement("INSERT INTO testBindNullByIndex VALUES (?, ?)")
          .bindNull(0, Integer.class).bindNull(1, Integer.class).add()
          .bindNull(0, Integer.class).bind(1, 0).add()
          .bindNull(0, Integer.class).bind(1, 1).add()
          .bindNull(0, Integer.class).bind(1, 2).add()
          .bind(0, 0).bind(1, 3).add()
          .bind(0, 0).bindNull(1, Integer.class).add());
      awaitQuery(
        asList(
          asList(null, 0),
          asList(null, 1),
          asList(null, 2)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y" +
            " FROM testBindNullByIndex" +
            " WHERE x IS NULL and y IS NOT NULL" +
            " ORDER BY y"));

      // Expect bind values to be replaced when set more than once
      awaitUpdate(
        asList(1),
        connection
          .createStatement("INSERT INTO testBindNullByIndex VALUES (?, ?)")
          .bind(0, 99).bind(1, 99)
          .bind(0, 1).bindNull(1, Integer.class));
      awaitUpdate(
        asList(1),
        connection
          .createStatement("INSERT INTO testBindNullByIndex VALUES (?, ?)")
          .bindNull(0, Integer.class).bindNull(1, Integer.class)
          .bind(0, 1).bind(1, 0));
      awaitQuery(
        asList(asList(1, 0), asList(1, null)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y FROM testBindNullByIndex WHERE x = 1 ORDER BY y"));
      awaitUpdate(
        asList(1),
        connection
          .createStatement("INSERT INTO testBindNullByIndex VALUES (:x, :y)")
          .bind("x", 99).bind("y", 99)
          .bind(0, 2).bindNull(1, Integer.class));
      awaitUpdate(
        asList(1),
        connection
          .createStatement("INSERT INTO testBindNullByIndex VALUES (:x, :y)")
          .bindNull("x", Integer.class).bindNull("y", Integer.class)
          .bind(0, 2).bind(1, 0));
      awaitQuery(
        asList(asList(2, 0), asList(2, null)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y FROM testBindNullByIndex WHERE x = 2 ORDER BY y"));

      // Expect bind values to be replaced when set more than once, after
      // calling add()
      awaitUpdate(
        asList(1, 1),
        connection
          .createStatement("INSERT INTO testBindNullByIndex VALUES (?, ?)")
          .bind(0, 3).bind(1, 0).add()
          .bind(0, 99).bind(1, 99)
          .bind(0, 3).bindNull(1, Integer.class));
      awaitUpdate(
        asList(1, 1),
        connection
          .createStatement("INSERT INTO testBindNullByIndex VALUES (:x, :y)")
          .bind(0, 3).bind(1, 1).add()
          .bind("x", 99).bindNull("y", Integer.class)
          .bind(0, 3).bind(1, 3));
      awaitQuery(
        asList(
          asList(3, 0), asList(3, 1), asList(3, 3), asList(3, null)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y FROM testBindNullByIndex WHERE x = 3 ORDER BY y"));
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testBindNullByIndex"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleStatementImpl#bindNull(String, Class)}
   */
  @Test
  public void testBindNullByName() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // INSERT into this table with a parameterized VALUES clause
      awaitExecution(connection.createStatement(
        "CREATE TABLE testNullBindByName (x NUMBER, y NUMBER)"));
      Statement selectStatement = connection.createStatement(
        "SELECT x, y" +
          " FROM testNullBindByName" +
          " WHERE x = :x and y > :y" +
          " ORDER BY x, y");

      // Expect IllegalArgumentException for a null class
      assertThrows(
        IllegalArgumentException.class,
        () -> selectStatement.bindNull("x", null));
      assertThrows(
        IllegalArgumentException.class,
        () -> selectStatement.bindNull("y", null));

      // Expect IllegalArgumentException for a null identifier
      assertThrows(
        IllegalArgumentException.class,
        () -> selectStatement.bindNull(null, Integer.class));

      // Expect IllegalArgumentException for an unmatched identifier
      assertThrows(
        IllegalArgumentException.class,
        () -> selectStatement.bindNull("z", Integer.class));
      assertThrows(
        IllegalArgumentException.class,
        () -> selectStatement.bindNull("xx", Integer.class));
      assertThrows(
        IllegalArgumentException.class,
        () -> selectStatement.bindNull("", Integer.class));
      assertThrows(
        IllegalArgumentException.class,
        () -> selectStatement.bindNull("X", Integer.class));
      assertThrows(
        IllegalArgumentException.class,
        () ->
          connection.createStatement("SELECT x FROM testBindByIndex")
            .bind("x", 0));


      // Expect NULL bind values to be applied in VALUES clause
      awaitUpdate(
        asList(1, 1, 1, 1, 1, 1),
        connection
          .createStatement("INSERT INTO testNullBindByName VALUES (:x, :y)")
          .bindNull("x", Integer.class).bindNull("y", Integer.class).add()
          .bindNull("x", Integer.class).bind("y", 0).add()
          .bindNull("x", Integer.class).bind("y", 1).add()
          .bindNull("x", Integer.class).bind("y", 2).add()
          .bind("x", 0).bind("y", 3).add()
          .bind("x", 0).bindNull("y", Integer.class).add());
      awaitQuery(
        asList(
          asList(null, 0),
          asList(null, 1),
          asList(null, 2)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y" +
            " FROM testNullBindByName" +
            " WHERE x IS NULL and y IS NOT NULL" +
            " ORDER BY y"));

      // Using a duplicate parameter name, expect bind values to be applied
      // in WHERE clause as:
      // UPDATE testNullBindByName SET x = NULL WHERE x = 0
      awaitUpdate(
        asList(2),
        connection.createStatement(
          "UPDATE testNullBindByName" +
            " SET x = :x WHERE x = :x")
          .bindNull("x", Integer.class).bind(1, 0));
      awaitQuery(
        asList(
          asList(null, 0),
          asList(null, 1),
          asList(null, 2),
          asList(null, 3),
          asList(null, null),
          asList(null, null)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y" +
            " FROM testNullBindByName" +
            " WHERE x IS NULL" +
            " ORDER BY y"));

      // Expect bind values to be replaced when set more than once
      awaitUpdate(
        asList(1),
        connection
          .createStatement("INSERT INTO testNullBindByName VALUES (:x, :y)")
          .bind(0, 99).bind(1, 99)
          .bind("x", 1).bindNull("y", Integer.class));
      awaitUpdate(
        asList(1),
        connection
          .createStatement("INSERT INTO testNullBindByName VALUES (:x, :y)")
          .bindNull(0, Integer.class).bindNull(1, Integer.class)
          .bind("x", 1).bind("y", 0));
      awaitQuery(
        asList(asList(1, 0), asList(1, null)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y FROM testNullBindByName WHERE x = 1 ORDER BY y"));
      awaitUpdate(
        asList(1),
        connection
          .createStatement("INSERT INTO testNullBindByName VALUES (:x, :y)")
          .bind("x", 99).bind("y", 99)
          .bind("x", 2).bindNull("y", Integer.class));
      awaitUpdate(
        asList(1),
        connection
          .createStatement("INSERT INTO testNullBindByName VALUES (:x, :y)")
          .bindNull("x", Integer.class).bindNull("y", Integer.class)
          .bind("x", 2).bind("y", 0));
      awaitQuery(
        asList(asList(2, 0), asList(2, null)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y FROM testNullBindByName WHERE x = 2 ORDER BY y"));

      // Expect bind values to be replaced when set more than once, after
      // calling add()
      awaitUpdate(
        asList(1, 1),
        connection
          .createStatement("INSERT INTO testNullBindByName VALUES (:x, :y)")
          .bind("x", 3).bind("y", 0).add()
          .bind(0, 99).bind(1, 99)
          .bind("x", 3).bindNull("y", Integer.class));
      awaitUpdate(
        asList(1, 1),
        connection
          .createStatement("INSERT INTO testNullBindByName VALUES (:x, :y)")
          .bind("x", 3).bind("y", 1).add()
          .bind("x", 99).bindNull("y", Integer.class)
          .bind("x", 3).bind("y", 3));
      awaitQuery(
        asList(
          asList(3, 0), asList(3, 1), asList(3, 3), asList(3, null)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y FROM testNullBindByName WHERE x = 3 ORDER BY y"));

      // When the same name is used for multiple parameters, expect a value
      // bound to that name to be set as the value for all of those parameters.
      // Expect a value bound to the index of one of those parameters to be
      // set only for the parameter at that index.
      awaitUpdate(2, connection.createStatement(
        "DELETE FROM testNullBindByName WHERE x IS NULL AND y IS NULL"));
      awaitUpdate(asList(1, 1, 1),
        connection
          .createStatement(
            "INSERT INTO testNullBindByName VALUES (:same, :same)")
          .bindNull("same", Integer.class).add()
          .bindNull("same", Integer.class).bind(0, 4).add()
          .bind(0, 5).bindNull(1, Integer.class));
      awaitQuery(asList(asList(null, null)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y FROM testNullBindByName" +
            " WHERE x IS NULL AND y IS NULL"));
      awaitQuery(asList(asList(4, null), asList(5, null)),
        row ->
          asList(row.get(0, Integer.class), row.get(1,Integer.class)),
        connection.createStatement(
          "SELECT x, y FROM testNullBindByName" +
            " WHERE x >= 4 AND x IS NOT NULL AND y IS NULL" +
            " ORDER BY x, y"));
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testNullBindByName"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleStatementImpl#add()}
   */
  @Test
  public void testAdd() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // INSERT into this table with a parameterized VALUES clause
      awaitExecution(connection.createStatement(
        "CREATE TABLE testAdd (x NUMBER, y NUMBER)"));
      // Expect add() with zero parameters to execute a batch of INSERTs
      awaitUpdate(
        asList(1, 1, 1),
        connection.createStatement("INSERT INTO testAdd VALUES(0, 0)")
          .add().add().add());
      awaitQuery(
        asList(asList(0, 0), asList(0, 0), asList(0, 0)),
        row -> asList(row.get(0, Integer.class), row.get(1, Integer.class)),
        connection.createStatement("SELECT x, y FROM testAdd"));

      // Expect add() with parameters to execute a batch of INSERTs
      awaitUpdate(
        asList(1, 1, 1),
        connection.createStatement("INSERT INTO testAdd VALUES(:x, :y)")
          .bind("x", 1).bind("y", 1).add()
          .bind("x", 1).bind("y", 2).add()
          .bind("x", 1).bind("y", 3).add());
      awaitQuery(
        asList(asList(1, 1), asList(1, 2), asList(1, 3)),
        row -> asList(row.get(0, Integer.class), row.get(1, Integer.class)),
        connection.createStatement(
          "SELECT x, y" +
            " FROM testAdd" +
            " WHERE x = 1" +
            " ORDER BY y"));

      // Expect an implicit add() after add() has been called once
      awaitUpdate(
        asList(1, 1),
        connection.createStatement("INSERT INTO testAdd VALUES(:x, :y)")
          .bind("x", 2).bind("y", 1).add()
          .bind("x", 2).bind("y", 2)); // implicit .add()
      awaitQuery(
        asList(asList(2, 1), asList(2, 2)),
        row -> asList(row.get(0, Integer.class), row.get(1, Integer.class)),
        connection.createStatement(
          "SELECT x, y" +
            " FROM testAdd" +
            " WHERE x = 2" +
            " ORDER BY y"));

      // Expect R2dbcException when executing a non-DML batch
      awaitError(
        R2dbcException.class,
        connection.createStatement("SELECT ? FROM dual")
          .bind(0, 1).add()
          .bind(0, 2).add()
          .bind(0, 3).add()
          .execute());

      // Expect IllegalStateException if not all parameters are set
      assertThrows(
        IllegalStateException.class,
        () ->
          connection.createStatement("INSERT INTO table VALUES(?)")
            .add());
      assertThrows(
        IllegalStateException.class,
        () ->
          connection.createStatement("INSERT INTO table VALUES(?, ?)")
            .bind(0, 0).add());
      assertThrows(
        IllegalStateException.class,
        () ->
          connection.createStatement("INSERT INTO table VALUES(:x, :y)")
            .bind("y", 1).add());
      assertThrows(
        IllegalStateException.class,
        () ->
          connection.createStatement("INSERT INTO table VALUES(?)")
            .bind(0, 0).add()
            .add());
      assertThrows(
        IllegalStateException.class,
        () ->
          connection.createStatement("INSERT INTO table VALUES(?, ?)")
            .bind(0, 0).bind(1, 1).add()
            .bind(1, 1).add());
      assertThrows(
        IllegalStateException.class,
        () ->
          connection.createStatement("INSERT INTO table VALUES(:x, :y)")
            .bind("x", 0).bind("y", 1).add()
            .bind("x", 0).add());
      assertThrows(
        IllegalStateException.class,
        () ->
          connection.createStatement("INSERT INTO table VALUES(:x, :y)")
            .bind("x", 0).bind("y", 1).add()
            .bind("y", 1).execute()); // implicit add()

    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testAdd"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleStatementImpl#execute()}
   */
  @Test
  public void testExecute() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Expect DDL to result in an update count of zero
      awaitUpdate(0, connection.createStatement(
        "CREATE TABLE testExecute (x NUMBER)"));
      // Expect DDL to result in no row data
      awaitQuery(
        Collections.emptyList(),
        row -> row.get(0),
        connection.createStatement(
          "ALTER TABLE testExecute ADD (y NUMBER)"));

      // Expect DML to result in an update count
      Statement insertStatement = connection.createStatement(
        "INSERT INTO testExecute (x, y) VALUES (:x, :y)");
      awaitUpdate(
        asList(1),
        insertStatement.bind("x", 0).bind("y", 0));

      // Expect DML to result in no row data
      awaitQuery(
        Collections.emptyList(),
        row -> row.get(0),
        insertStatement.bind("x", 0).bind("y", 1));

      // Expect batch DML to result in an update count
      Statement updateStatement = connection.createStatement(
        "UPDATE testExecute SET y = :newValue WHERE y = :oldValue");
      awaitUpdate(
        asList(1, 1),
        updateStatement
          .bind("oldValue", 1).bind("newValue", 2).add()
          .bind("oldValue", 0).bind("newValue", 1).add());

      // Expect bind values to be cleared after execute with explicit add()
      assertThrows(IllegalStateException.class, updateStatement::execute);

      // Expect batch DML to result in no row data
      awaitQuery(
        Collections.emptyList(),
        row -> row.get(0),
        updateStatement
          .bind("oldValue", 2).bind("newValue", 3).add()
          .bind("oldValue", 1).bind("newValue", 2));

      // Expect bind values to be cleared after execute with implicit add()
      assertThrows(IllegalStateException.class, updateStatement::execute);

      // Expect publisher to defer execution until a subscriber subscribes
      Publisher<? extends Result> updatePublisher =
        updateStatement.bind("oldValue", 3).bind("newValue", 1).execute();

      // Expect DQL to result in no update count
      Statement selectStatement = connection.createStatement(
        "SELECT x, y FROM testExecute WHERE x = :x ORDER BY y");
      awaitUpdate(
        Collections.emptyList(),
        selectStatement.bind("x", 0));

      // Expect DQL to result in row data
      awaitQuery(
        asList(asList(0, 2), asList(0, 3)),
        row ->
          asList(row.get("x", Integer.class), row.get("y", Integer.class)),
        selectStatement.bind("x", 0));

      // Expect bind values to be cleared after execute without add()
      assertThrows(
        IllegalStateException.class,
        selectStatement::execute);

      // Expect update to execute when a subscriber subscribes
      awaitOne(1,
        Flux.from(updatePublisher)
          .flatMap(result -> result.getRowsUpdated()));
      awaitQuery(
        asList(asList(0, 1), asList(0, 2)),
        row ->
          asList(row.get("x", Integer.class), row.get("y", Integer.class)),
        selectStatement.bind("x", 0));

      // Expect publisher to reject multiple subscribers
      awaitError(IllegalStateException.class, updatePublisher);

      // TODO: Verify that cursors opened by execute() are closed after the
      //   result has been consumed. Consider querying V$ tables to verify the
      //   open cursor count. Consider that the JDBC driver may be caching
      //   statements and leaving cursors open until a cache eviction happens.
    }
    finally {
      tryAwaitExecution(connection.createStatement("DROP TABLE testExecute"));
      tryAwaitNone(connection.close());
    }
  }


  /**
   * Verifies the implementation of
   * {@link OracleStatementImpl#returnGeneratedValues(String...)}
   */
  @Test
  public void testReturnGeneratedValues() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      awaitExecution(connection.createStatement(
        "CREATE TABLE testReturnGeneratedValues (" +
          "x NUMBER GENERATED ALWAYS AS IDENTITY, " +
          "y VARCHAR2(100))"));

      Statement statement = connection.createStatement(
        "INSERT INTO testReturnGeneratedValues(y) VALUES (?)");

      // Expect IllegalArgumentException for a null argument
      assertThrows(IllegalArgumentException.class,
        () -> statement.returnGeneratedValues((String[])null));
      // Expect IllegalArgumentException for a null String[] element
      assertThrows(IllegalArgumentException.class,
        () -> statement.returnGeneratedValues("x", null));

      // Expect a failure with invalid column name "eye-d"
      assertEquals(statement, statement.returnGeneratedValues("x", "eye-d"));
      awaitError(R2dbcException.class, statement.bind(0, "test").execute());

      // Expect a ROWID value when no column names are specified
      Statement rowIdQuery = connection.createStatement(
        "SELECT x, y FROM testReturnGeneratedValues WHERE rowid=?");
      Result rowIdResult = awaitOne(
        statement.returnGeneratedValues().bind(0, "test1").execute());
      RowId rowId = awaitOne(
        rowIdResult.map((row, metadata) -> row.get(0, RowId.class)));
      // Expect a generated value of 1 when the ROWID is queried
      awaitQuery(asList(asList(1, "test1")),
        row -> asList(row.get(0, Integer.class), row.get(1, String.class)),
        rowIdQuery.bind(0, rowId));

      // Expect the second insert to generate a value of 2
      awaitQuery(asList(asList(2, "test2")),
        row -> asList(row.get(0, Integer.class), row.get(1, String.class)),
        statement.returnGeneratedValues("x", "y").bind(0, "test2"));

      // Expect an update count of 1 ...
      awaitUpdate(1, statement.returnGeneratedValues("x").bind(0, "test3"));
      // ... and generated value of 3
      awaitQuery(asList(asList(3, "test3")),
        row -> asList(row.get(0, Integer.class), row.get(1, String.class)),
        connection.createStatement(
          "SELECT x, y FROM testReturnGeneratedValues WHERE x = 3"));

      // Expect non-generated values to be returned as well
      assertEquals(statement, statement.returnGeneratedValues("x", "y"));
      awaitQuery(asList(asList(4, "test4")),
        row -> asList(row.get("x", Integer.class), row.get("Y", String.class)),
        statement.bind(0, "test4"));

      // Expect an error when attempting to batch execute with generated
      // values
      assertThrows(IllegalStateException.class, () ->
        statement.bind(0, "a").add()
          .bind(0, "b").add()
          .bind(0, "c").add()
          .execute());

      // Expect multiple results of generated values when executing an UPDATE
      // on multiple rows
      awaitQuery(asList(
        asList(1, "TEST1"),
        asList(2, "TEST2"),
        asList(3, "TEST3"),
        asList(4, "TEST4")),
      row -> asList(row.get("x", Integer.class), row.get("y", String.class)),
      connection.createStatement(
        "UPDATE testReturnGeneratedValues SET y =:prefix||x")
        .bind("prefix", "TEST")
        .returnGeneratedValues("x", "y"));

      // Expect Rows of generated values to remain valid after a connection is
      // closed.
      // Note: This behavior is not described in the R2DBC 0.8.2
      // Specification, nor in the R2DBC SPI Javadoc. However, this behavior
      // is verified by the 0.8.2 R2DBC SPI TCK, so it is considered to be a
      // standard convention that all R2DBC Drivers follow; Programmers will
      // expect this behavior.
      Connection newConnection = awaitOne(newConnection());
      Result cachedResult = awaitOne(
        newConnection.createStatement(
          "INSERT INTO testReturnGeneratedValues(y) VALUES (:y)")
        .bind("y", "a")
        .returnGeneratedValues("x", "y")
        .execute());
      awaitNone(newConnection.close());
      awaitOne(asList(BigDecimal.valueOf(5), "a"),
        cachedResult.map((row, metadata) ->
            asList(
              row.get("x", metadata.getColumnMetadata("x").getJavaType()),
              row.get("y", metadata.getColumnMetadata("y").getJavaType()))));

      // Expect an IllegalStateException when executing a query with a
      // Statement configured to return columns generated by DML.
      awaitError(IllegalStateException.class,
        connection.createStatement(
          "SELECT x, y" +
            " FROM testReturnGeneratedValues" +
            " WHERE x < :old_x" +
            " ORDER BY x")
          .bind("old_x", 10)
          .returnGeneratedValues("x")
          .execute());

      // TODO: Uncomment this test when the Oracle JDBC Team fixes a bug where
      //   ResultSet.isBeforeFirst() returns true for an empty ResultSet
      //   returned by Statement.getGeneratedKeys(). The team is aware of the
      //   bug and is working on a fix. For now, this test will result in:
      //   ORA-17023 Unsupported feature: getMetaData
      // Expect the column names to be ignored if the SQL is not an INSERT or
      // UPDATE
      /*
      awaitQuery(asList(
        asList(1, "TEST1"),
        asList(2, "TEST2"),
        asList(3, "TEST3"),
        asList(4, "TEST4")),
        row -> asList(row.get("x", Integer.class), row.get("y", String.class)),
        connection.createStatement(
          "DELETE FROM testReturnGeneratedValues WHERE x < :old_x")
          .bind("old_x", 10)
          .returnGeneratedValues("x", "y"));
       */
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testReturnGeneratedValues"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleStatementImpl#fetchSize(int)
   */
  @Test
  public void testFetchSize() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Expect normal return when argument is at least 0
      Statement statement = connection.createStatement(
        "SELECT x, y FROM testFetchSize");
      assertEquals(statement, statement.fetchSize(0));
      assertEquals(statement, statement.fetchSize(100));

      // Expect IllegalArgumentException when argument is less than 0
      assertThrows(IllegalArgumentException.class,
        () -> statement.fetchSize(-1));
      assertThrows(IllegalArgumentException.class,
        () -> statement.fetchSize(-100));

      // TODO: Figure out a way to verify that the implementation actually
      //  set the fetch size. Might expect a large query to complete quicker
      //  when a large fetch size is set, but execution time is not consistent
      //  due to external factors like network latency and database response
      //  time.
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies {@link OracleStatementImpl#execute()} when calling a procedure
   * having no out parameters.
   */
  @Test
  public void testNoOutCall() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      awaitExecution(connection.createStatement(
        "CREATE TABLE testNoOutCall (value VARCHAR2(100))"));

      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE testNoOutCallAdd(" +
          "value VARCHAR2 DEFAULT 'Default Value') IS" +
          " BEGIN " +
          " INSERT INTO testNoOutCall VALUES (value);" +
          " END;"));

      // Execute the procedure with no out parameters. Expect a single Result
      // with no update count. Expect the IN parameter's default value to
      // have been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testNoOutCallAdd; END;")
        .execute(),
        result0 -> {
          awaitNone(result0.getRowsUpdated());
          awaitQuery(asList("Default Value"),
            row -> row.get(0),
            connection.createStatement("SELECT * FROM testNoOutCall"));
        });

      // Execute the procedure again with no out parameters. Expect a single
      // Result with no rows. Expect the IN parameter's default value to have
      // been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testNoOutCallAdd; END;")
        .execute(),
        result1 -> {
          awaitNone(result1.map((row, metadata) -> "Unexpected"));
          awaitQuery(asList("Default Value", "Default Value"),
            row -> row.get(0),
            connection.createStatement("SELECT * FROM testNoOutCall"));
        });

      // Delete the previously inserted rows
      awaitExecution(connection.createStatement(
        "TRUNCATE TABLE testNoOutCall"));

      // Execute the procedure with no out parameters. Expect a single Result
      // with no update count. Expect the an indexed based String bind to
      // have been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testNoOutCallAdd(?); END;")
        .bind(0, "Indexed Bind")
        .execute(),
        result2 -> {
          awaitNone(result2.getRowsUpdated());
          awaitQuery(asList("Indexed Bind"),
            row -> row.get(0),
            connection.createStatement("SELECT * FROM testNoOutCall"));
        });

      // Execute the procedure with no out parameters. Expect a single Result
      // with no update count. Expect the a named String bind to have been
      // inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testNoOutCallAdd(:parameter); END;")
        .bind("parameter", "Named Bind")
        .execute(),
        result3 -> {
          awaitNone(result3.map((row, metadata) -> "Unexpected"));
          awaitQuery(asList("Indexed Bind", "Named Bind"),
            row -> row.get(0),
            connection.createStatement(
              "SELECT * FROM testNoOutCall ORDER BY value"));
        });

      // Delete the previously inserted rows
      awaitExecution(connection.createStatement(
        "TRUNCATE TABLE testNoOutCall"));

      // Execute the procedure with no out parameters. Expect a single Result
      // with no update count. Expect the an indexed based Parameter bind to
      // have been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testNoOutCallAdd(?); END;")
        .bind(0, Parameters.in(R2dbcType.VARCHAR, "Indexed Parameter"))
        .execute(),
        result4 -> {
          awaitNone(result4.getRowsUpdated());
          awaitQuery(asList("Indexed Parameter"),
            row -> row.get(0),
            connection.createStatement("SELECT * FROM testNoOutCall"));
        });

      // Execute the procedure with no out parameters. Expect a single Result
      // with no update count. Expect the a named Parameter bind to have been
      // inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testNoOutCallAdd(:parameter); END;")
        .bind("parameter",
          Parameters.in(R2dbcType.VARCHAR, "Named Parameter"))
        .execute(),
        result5 -> {
          awaitNone(result5.map((row, metadata) -> "Unexpected"));
          awaitQuery(asList("Indexed Parameter", "Named Parameter"),
            row -> row.get(0),
            connection.createStatement(
              "SELECT * FROM testNoOutCall ORDER BY value"));
        });

      // Delete the previously inserted rows
      awaitExecution(connection.createStatement(
        "TRUNCATE TABLE testNoOutCall"));

      // Execute the procedure with no out parameters. Expect a single Result
      // with no update count. Expect the an indexed based Parameter.In bind to
      // have been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testNoOutCallAdd(?); END;")
        .bind(0, Parameters.in(R2dbcType.VARCHAR, "Indexed Parameter.In"))
        .execute(),
        result6 -> {
          awaitNone(result6.getRowsUpdated());
          awaitQuery(asList("Indexed Parameter.In"),
            row -> row.get(0),
            connection.createStatement("SELECT * FROM testNoOutCall"));
        });

      // Execute the procedure with no out parameters. Expect a single Result
      // with no update count. Expect the a named Parameter.In bind to have been
      // inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testNoOutCallAdd(:parameter); END;")
        .bind("parameter",
          Parameters.in(R2dbcType.VARCHAR, "Named Parameter.In"))
        .execute(),
        result7 -> {
          awaitNone(result7.map((row, metadata) -> "Unexpected"));
          awaitQuery(asList("Indexed Parameter.In", "Named Parameter.In"),
            row -> row.get(0),
            connection.createStatement(
              "SELECT * FROM testNoOutCall ORDER BY value"));
        });
    }
    finally {
      tryAwaitExecution(connection.createStatement("DROP TABLE testNoOutCall"));
      tryAwaitExecution(connection.createStatement(
        "DROP PROCEDURE testNoOutCallAdd"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies {@link OracleStatementImpl#execute()} when calling a procedure
   * having a single in-out parameter.
   */
  @Test
  public void testOneInOutCall() {

    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());

    try {
      // Create a table with one value. Create a procedure that updates the
      // value and returns the previous value
      awaitExecution(connection.createStatement(
        "CREATE TABLE testOneInOutCall (value NUMBER)"));
      awaitUpdate(1, connection.createStatement(
        "INSERT INTO testOneInOutCall VALUES (0)"));
      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE testOneInOutCallAdd(" +
          " inout_value IN OUT NUMBER) IS" +
          " previous NUMBER;" +
          " BEGIN " +
          " SELECT value INTO previous FROM testOneInOutCall;" +
          " UPDATE testOneInOutCall SET value = inout_value;" +
          " inout_value := previous;" +
          " END;"));

      // Execute the procedure with one in-out parameter. Expect a single Result
      // with no update count. Expect the IN parameter's value to have been
      // inserted by the call.
      Result result0 = awaitOne(connection.createStatement(
        "BEGIN testOneInOutCallAdd(?); END;")
        .bind(0, new InOutParameter(1, R2dbcType.NUMERIC))
        .execute());
      awaitNone(result0.getRowsUpdated());
      awaitQuery(asList(1),
        row -> row.get("value", Integer.class),
        connection.createStatement("SELECT * FROM testOneInOutCall"));

      // Execute the procedure again with one in-out parameter. Expect a single
      // Result with one rows having the previous value. Expect the IN
      // parameter's default value to have been inserted by the call.
      Result result1 = awaitOne(connection.createStatement(
        "BEGIN testOneInOutCallAdd(:value); END;")
        .bind("value", new InOutParameter(2, R2dbcType.NUMERIC))
        .execute());
      awaitOne(1, result1.map((row, metadata) ->
        row.get("value", Integer.class)));
      awaitQuery(asList(2),
        row -> row.get(0, Integer.class),
        connection.createStatement("SELECT * FROM testOneInOutCall"));

      // Execute the procedure with one in-out parameter having an inferred
      // type. Expect a single Result with no update count. Expect the IN
      // parameter's value to have been inserted by the call.
      Result result2 = awaitOne(connection.createStatement(
        "BEGIN testOneInOutCallAdd(:value); END;")
        .bind("value", new InOutParameter(3))
        .execute());
      awaitNone(result2.getRowsUpdated());
      awaitQuery(asList(3),
        row -> row.get(0, Integer.class),
        connection.createStatement("SELECT * FROM testOneInOutCall"));;

      // Execute the procedure again with one in-out parameter. Expect a single
      // Result with one rows having the previous value. Expect the IN
      // parameter's value to have been inserted by the call.
      Result result3 = awaitOne(connection.createStatement(
        "BEGIN testOneInOutCallAdd(?); END;")
        .bind(0, new InOutParameter(4))
        .execute());
      awaitOne(3, result3.map((row, metadata) ->
        row.get(0, Integer.class)));
      awaitQuery(asList(4),
        row -> row.get(0, Integer.class),
        connection.createStatement("SELECT * FROM testOneInOutCall"));
    }
    finally {
      tryAwaitExecution(connection.createStatement("DROP TABLE testOneInOutCall"));
      tryAwaitExecution(connection.createStatement(
        "DROP PROCEDURE testOneInOutCallAdd"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies {@link OracleStatementImpl#execute()} when calling a procedure
   * having multiple in-out parameters.
   */
  @Test
  public void testMultiInOutCall() {

    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());

    try {
      // Create a table with one value. Create a procedure that updates the
      // value and returns the previous value
      awaitExecution(connection.createStatement(
        "CREATE TABLE testMultiInOutCall (value1 NUMBER, value2 NUMBER)"));
      awaitUpdate(1, connection.createStatement(
        "INSERT INTO testMultiInOutCall VALUES (0, 100)"));
      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE testMultiInOutCallAdd(" +
          " inout_value1 IN OUT NUMBER," +
          " inout_value2 IN OUT NUMBER) IS" +
          " previous1 NUMBER;" +
          " previous2 NUMBER;" +
          " BEGIN " +
          " SELECT value1, value2 INTO previous1, previous2" +
          "   FROM testMultiInOutCall;" +
          " UPDATE testMultiInOutCall" +
          "   SET value1 = inout_value1, value2 = inout_value2;" +
          " inout_value1 := previous1;" +
          " inout_value2 := previous2;" +
          " END;"));

      // Execute the procedure with one in-out parameter. Expect a single Result
      // with no update count. Expect the IN parameter's value to have been
      // inserted by the call.
      Result result0 = awaitOne(connection.createStatement(
        "BEGIN testMultiInOutCallAdd(:value1, :value2); END;")
        .bind("value1", new InOutParameter(1, R2dbcType.NUMERIC))
        .bind("value2", new InOutParameter(101, R2dbcType.NUMERIC))
        .execute());
      awaitNone(result0.getRowsUpdated());
      awaitQuery(asList(asList(1, 101)),
        row -> asList(
          row.get("value1", Integer.class),row.get("value2", Integer.class)),
        connection.createStatement("SELECT * FROM testMultiInOutCall"));

      // Execute the procedure again with one in-out parameter. Expect a single
      // Result with one rows having the previous value. Expect the IN
      // parameter's default value to have been inserted by the call.
      Result result1 = awaitOne(connection.createStatement(
        "BEGIN testMultiInOutCallAdd(?, :value2); END;")
        .bind(0, new InOutParameter(2, R2dbcType.NUMERIC))
        .bind("value2", new InOutParameter(102, R2dbcType.NUMERIC))
        .execute());
      awaitOne(asList(1, 101), result1.map((row, metadata) ->
        asList(
          row.get(0, Integer.class), row.get("value2", Integer.class))));
      awaitQuery(asList(asList(2, 102)),
        row ->
          asList(row.get("value1", Integer.class), row.get(1, Integer.class)),
        connection.createStatement("SELECT * FROM testMultiInOutCall"));

      // Execute the procedure with one in-out parameter having an inferred
      // type. Expect a single Result with no update count. Expect the IN
      // parameter's value to have been inserted by the call.
      Result result2 = awaitOne(connection.createStatement(
        "BEGIN testMultiInOutCallAdd(?, ?); END;")
        .bind(0, new InOutParameter(3))
        .bind(1, new InOutParameter(103))
        .execute());
      awaitNone(result2.getRowsUpdated());
      awaitQuery(asList(asList(3, 103)),
        row -> asList(row.get(0, Integer.class), row.get(1, Integer.class)),
        connection.createStatement("SELECT * FROM testMultiInOutCall"));;

      // Execute the procedure again with multiple in-out parameters having
      // the same name. Expect a single Result with one rows having the
      // previous value. Getting the parameter value by name should returned
      // the value of the first parameter. Expect the IN parameter's value to
      // have been inserted by the call.
      Result result3 = awaitOne(connection.createStatement(
        "BEGIN testMultiInOutCallAdd(" +
          "inout_value2 => :value2, inout_value1 => :value1); END;")
        .bind("value1", new InOutParameter(4))
        .bind("value2", new InOutParameter(104))
        .execute());
      awaitOne(asList(3, 103), result3.map((row, metadata) ->
        asList(
          row.get("value1", Integer.class), row.get(0, Integer.class))));
      awaitQuery(asList(asList(4, 104)),
        row -> asList(row.get(0, Integer.class), row.get(1, Integer.class)),
        connection.createStatement("SELECT * FROM testMultiInOutCall"));
    }
    finally {
      tryAwaitExecution(connection.createStatement("DROP TABLE testMultiInOutCall"));
      tryAwaitExecution(connection.createStatement(
        "DROP PROCEDURE testMultiInOutCallAdd"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies {@link OracleStatementImpl#execute()} when calling a procedure
   * having a single out parameter.
   */
  @Test
  public void testOneOutCall() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      awaitExecution(connection.createStatement(
        "CREATE TABLE testOneOutCall (" +
          "id NUMBER GENERATED ALWAYS AS IDENTITY, value VARCHAR2(100))"));

      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE testOneOutCallAdd(" +
          " value IN VARCHAR2 DEFAULT 'Default Value'," +
          " id OUT NUMBER) IS" +
          " BEGIN " +
          " INSERT INTO testOneOutCall(value) VALUES (value)" +
          "   RETURNING testOneOutCall.id INTO id;" +
          " END;"));

      // Execute the procedure with one in-out parameter. Expect a single Result
      // with no update count. Expect the IN parameter's default value to
      // have been inserted by the call.
      Result result0 = awaitOne(connection.createStatement(
        "BEGIN testOneOutCallAdd(id => ?); END;")
        .bind(0, Parameters.out(R2dbcType.NUMERIC))
        .execute());
      awaitNone(result0.getRowsUpdated());
      awaitQuery(asList(asList(1, "Default Value")),
        row -> asList(row.get("id", Integer.class), row.get("value")),
        connection.createStatement("SELECT * FROM testOneOutCall"));

      // Execute the procedure again with one in-out parameter. Expect a single
      // Result with one rows Expect the IN parameter's default value to have
      // been inserted by the call.
      Result result1 =  awaitOne(connection.createStatement(
        "BEGIN testOneOutCallAdd(id => ?); END;")
        .bind(0, Parameters.out(R2dbcType.NUMERIC))
        .execute());
      awaitOne(2, result1.map((row, metadata) -> row.get(0, Integer.class)));
      awaitQuery(asList(asList(1, "Default Value"), asList(2, "Default Value")),
        row -> asList(row.get("id", Integer.class), row.get("value")),
        connection.createStatement("SELECT * FROM testOneOutCall"));

      // Delete the previously inserted rows
      awaitExecution(connection.createStatement(
        "TRUNCATE TABLE testOneOutCall"));

      // Execute the procedure with one in-out parameter. Expect a single Result
      // with no update count. Expect the an indexed based String bind to
      // have been inserted by the call.
      Result result2 = awaitOne(connection.createStatement(
        "BEGIN testOneOutCallAdd(?, ?); END;")
        .bind(0, "Indexed Bind")
        .bind(1, Parameters.out(R2dbcType.NUMERIC))
        .execute());
      awaitNone(result2.getRowsUpdated());
      awaitQuery(asList(asList(3, "Indexed Bind")),
        row -> asList(row.get(0, Integer.class), row.get(1)),
        connection.createStatement("SELECT * FROM testOneOutCall"));

      // Execute the procedure with one in-out parameter. Expect a single Result
      // with no update count. Expect the a named String bind to have been
      // inserted by the call.
      Result result3 = awaitOne(connection.createStatement(
        "BEGIN testOneOutCallAdd(:parameter, :out); END;")
        .bind("parameter", "Named Bind")
        .bind("out", Parameters.out(R2dbcType.NUMERIC))
        .execute());
      awaitOne(4,
        result3.map((row, metadata) -> row.get("out", Integer.class)));
      awaitQuery(asList(asList(3, "Indexed Bind"), asList(4, "Named Bind")),
        row -> asList(row.get(0, Integer.class), row.get(1)),
        connection.createStatement(
          "SELECT * FROM testOneOutCall ORDER BY value"));

      // Delete the previously inserted rows
      awaitExecution(connection.createStatement(
        "TRUNCATE TABLE testOneOutCall"));

      // Execute the procedure with one in-out parameter. Expect a single Result
      // with no update count. Expect the an indexed based Parameter bind to
      // have been inserted by the call.
      Result result4 = awaitOne(connection.createStatement(
        "BEGIN testOneOutCallAdd(?, ?); END;")
        .bind(0, Parameters.in(R2dbcType.VARCHAR, "Indexed Parameter"))
        .bind(1, Parameters.out(R2dbcType.NUMERIC))
        .execute());
      awaitNone(result4.getRowsUpdated());
      awaitQuery(asList(asList(5, "Indexed Parameter")),
        row -> asList(row.get(0, Integer.class), row.get(1)),
        connection.createStatement("SELECT * FROM testOneOutCall"));

      // Execute the procedure with one in-out parameter. Expect a single Result
      // with no update count. Expect the a named Parameter bind to have been
      // inserted by the call.
      Result result5 = awaitOne(connection.createStatement(
        "BEGIN testOneOutCallAdd(:parameter, :out); END;")
        .bind("parameter",
          Parameters.in(R2dbcType.VARCHAR, "Named Parameter"))
        .bind("out", Parameters.out(R2dbcType.NUMERIC))
        .execute());
      awaitOne(6,
        result5.map((row, metadata) -> row.get("out", Integer.class)));
      awaitQuery(asList(
        asList(5, "Indexed Parameter"), asList(6, "Named Parameter")),
        row -> asList(row.get(0, Integer.class), row.get(1)),
        connection.createStatement(
          "SELECT * FROM testOneOutCall ORDER BY value"));

      // Delete the previously inserted rows
      awaitExecution(connection.createStatement(
        "TRUNCATE TABLE testOneOutCall"));

      // Execute the procedure with one in-out parameter. Expect a single Result
      // with no update count. Expect the an indexed based Parameter.In bind to
      // have been inserted by the call.
      Result result6 = awaitOne(connection.createStatement(
        "BEGIN testOneOutCallAdd(?, ?); END;")
        .bind(0, Parameters.in(R2dbcType.VARCHAR, "Indexed Parameter.In"))
        .bind(1, Parameters.out(R2dbcType.NUMERIC))
        .execute());
      awaitNone(result6.getRowsUpdated());
      awaitQuery(asList(asList(7, "Indexed Parameter.In")),
        row -> asList(row.get(0, Integer.class), row.get(1)),
        connection.createStatement("SELECT * FROM testOneOutCall"));

      // Execute the procedure with one in-out parameter. Expect a single Result
      // with no update count. Expect the a named Parameter.In bind to have been
      // inserted by the call.
      Result result7 = awaitOne(connection.createStatement(
        "BEGIN testOneOutCallAdd(:parameter, :out); END;")
        .bind("parameter",
          Parameters.in(R2dbcType.VARCHAR, "Named Parameter.In"))
        .bind("out", Parameters.out(R2dbcType.NUMERIC))
        .execute());
      awaitOne(result7.map((row, metadata) -> row.get("out")));
      awaitQuery(asList(
        asList(7, "Indexed Parameter.In"), asList(8, "Named Parameter.In")),
        row -> asList(row.get(0, Integer.class), row.get(1)),
        connection.createStatement(
          "SELECT * FROM testOneOutCall ORDER BY value"));
    }
    finally {
      tryAwaitExecution(connection.createStatement("DROP TABLE testOneOutCall"));
      tryAwaitExecution(connection.createStatement(
        "DROP PROCEDURE testOneOutCallAdd"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies {@link OracleStatementImpl#execute()} when calling a procedure
   * having a single out parameters.
   */
  @Test
  public void testMultiOutCall() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      awaitExecution(connection.createStatement(
        "CREATE TABLE testMultiOutCall (" +
          "id NUMBER GENERATED ALWAYS AS IDENTITY, value VARCHAR2(100))"));

      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE testMultiOutCallAdd(" +
          " value IN VARCHAR2 DEFAULT 'Default Value'," +
          " id OUT NUMBER," +
          " new_count OUT NUMBER) IS" +
          " BEGIN " +
          " INSERT INTO testMultiOutCall(value) VALUES (value)" +
          "   RETURNING testMultiOutCall.id INTO id;" +
          " SELECT COUNT(*) INTO new_count FROM testMultiOutCall;" +
          " END;"));

      // Execute the procedure with two out parameters. Expect a single Result
      // with no update count. Expect the IN parameter's default value to
      // have been inserted by the call.
      Result result0 = awaitOne(connection.createStatement(
        "BEGIN testMultiOutCallAdd(id => ?, new_count => ?); END;")
        .bind(0, Parameters.out(R2dbcType.NUMERIC))
        .bind(1, Parameters.out(R2dbcType.NUMERIC))
        .execute());
      awaitNone(result0.getRowsUpdated());
      awaitQuery(asList(asList(1, "Default Value")),
        row -> asList(row.get("id", Integer.class), row.get("value")),
        connection.createStatement("SELECT * FROM testMultiOutCall"));

      // Execute the procedure again with two out parameters. Expect a single
      // Result with one rows Expect the IN parameter's default value to have
      // been inserted by the call.
      Result result1 =  awaitOne(connection.createStatement(
        "BEGIN testMultiOutCallAdd(id => ?, new_count => ?); END;")
        .bind(0, Parameters.out(R2dbcType.NUMERIC))
        .bind(1, Parameters.out(R2dbcType.NUMERIC))
        .execute());
      awaitOne(asList(2, 2), result1.map((row, metadata) ->
        asList(row.get(0, Integer.class), row.get(1, Integer.class))));
      awaitQuery(asList(asList(1, "Default Value"), asList(2, "Default Value")),
        row -> asList(row.get("id", Integer.class), row.get("value")),
        connection.createStatement("SELECT * FROM testMultiOutCall"));

      // Delete the previously inserted rows
      awaitExecution(connection.createStatement(
        "TRUNCATE TABLE testMultiOutCall"));

      // Execute the procedure with two out parameters. Expect a single Result
      // with no update count. Expect the an indexed based String bind to
      // have been inserted by the call.
      Result result2 = awaitOne(connection.createStatement(
        "BEGIN testMultiOutCallAdd(?, ?, ?); END;")
        .bind(0, "Indexed Bind")
        .bind(1, Parameters.out(R2dbcType.NUMERIC))
        .bind(2, Parameters.out(R2dbcType.NUMERIC))
        .execute());
      awaitNone(result2.getRowsUpdated());
      awaitQuery(asList(asList(3, "Indexed Bind")),
        row -> asList(row.get(0, Integer.class), row.get(1)),
        connection.createStatement("SELECT * FROM testMultiOutCall"));

      // Execute the procedure with two out parameters. Expect a single Result
      // with no update count. Expect the a named String bind to have been
      // inserted by the call.
      Result result3 = awaitOne(connection.createStatement(
        "BEGIN testMultiOutCallAdd(:parameter, :out, :newCount); END;")
        .bind("parameter", "Named Bind")
        .bind("out", Parameters.out(R2dbcType.NUMERIC))
        .bind("newCount", Parameters.out(R2dbcType.NUMERIC))
        .execute());
      awaitOne(asList(4, 2), result3.map((row, metadata) ->
        asList(row.get("out", Integer.class),
          row.get("newCount", Integer.class))));
      awaitQuery(asList(asList(3, "Indexed Bind"), asList(4, "Named Bind")),
        row -> asList(row.get(0, Integer.class), row.get(1)),
        connection.createStatement(
          "SELECT * FROM testMultiOutCall ORDER BY value"));

      // Delete the previously inserted rows
      awaitExecution(connection.createStatement(
        "TRUNCATE TABLE testMultiOutCall"));

      // Execute the procedure with two out parameters. Expect a single Result
      // with no update count. Expect the an indexed based Parameter bind to
      // have been inserted by the call.
      Result result4 = awaitOne(connection.createStatement(
        "BEGIN testMultiOutCallAdd(?, ?, ?); END;")
        .bind(0, Parameters.in(R2dbcType.VARCHAR, "Indexed Parameter"))
        .bind(1, Parameters.out(R2dbcType.NUMERIC))
        .bind(2, Parameters.out(R2dbcType.NUMERIC))
        .execute());
      awaitNone(result4.getRowsUpdated());
      awaitQuery(asList(asList(5, "Indexed Parameter")),
        row -> asList(row.get(0, Integer.class), row.get(1)),
        connection.createStatement("SELECT * FROM testMultiOutCall"));

      // Execute the procedure with two out parameters. Expect a single Result
      // with no update count. Expect the a named Parameter bind to have been
      // inserted by the call.
      Result result5 = awaitOne(connection.createStatement(
        "BEGIN testMultiOutCallAdd(:parameter, :out, :newCount); END;")
        .bind("parameter",
          Parameters.in(R2dbcType.VARCHAR, "Named Parameter"))
        .bind("out", Parameters.out(R2dbcType.NUMERIC))
        .bind("newCount", Parameters.out(R2dbcType.NUMERIC))
        .execute());
      awaitOne(asList(6, 2), result5.map((row, metadata) ->
        asList(row.get("out", Integer.class),
          row.get("newCount", Integer.class))));
      awaitQuery(asList(
        asList(5, "Indexed Parameter"), asList(6, "Named Parameter")),
          row -> asList(row.get(0, Integer.class), row.get(1)),
        connection.createStatement(
          "SELECT * FROM testMultiOutCall ORDER BY value"));

      // Delete the previously inserted rows
      awaitExecution(connection.createStatement(
        "TRUNCATE TABLE testMultiOutCall"));

      // Execute the procedure with two out parameters. Expect a single Result
      // with no update count. Expect the an indexed based Parameter.In bind to
      // have been inserted by the call.
      Result result6 = awaitOne(connection.createStatement(
        "BEGIN testMultiOutCallAdd(?, ?, ?); END;")
        .bind(0, Parameters.in(R2dbcType.VARCHAR, "Indexed Parameter.In"))
        .bind(1, Parameters.out(R2dbcType.NUMERIC))
        .bind(2, Parameters.out(R2dbcType.NUMERIC))
        .execute());
      awaitNone(result6.getRowsUpdated());
      awaitQuery(asList(asList(7, "Indexed Parameter.In")),
        row -> asList(row.get(0, Integer.class), row.get(1)),
        connection.createStatement("SELECT * FROM testMultiOutCall"));

      // Execute the procedure with two out parameters. Expect a single Result
      // with no update count. Expect the a named Parameter.In bind to have been
      // inserted by the call.
      Result result7 = awaitOne(connection.createStatement(
        "BEGIN testMultiOutCallAdd(:parameter, :out, :newCount); END;")
        .bind("parameter",
          Parameters.in(R2dbcType.VARCHAR, "Named Parameter.In"))
        .bind("out", Parameters.out(R2dbcType.NUMERIC))
        .bind("newCount", Parameters.out(R2dbcType.NUMERIC))
        .execute());
      awaitOne(result7.map((row, metadata) -> row.get("out")));
      awaitQuery(asList(
        asList(7, "Indexed Parameter.In"), asList(8, "Named Parameter.In")),
        row -> asList(row.get(0, Integer.class), row.get(1)),
        connection.createStatement(
          "SELECT * FROM testMultiOutCall ORDER BY value"));
    }
    finally {
      tryAwaitExecution(connection.createStatement("DROP TABLE testMultiOutCall"));
      tryAwaitExecution(connection.createStatement(
        "DROP PROCEDURE testMultiOutCallAdd"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verify {@link OracleStatementImpl#execute()} when calling a procedure
   * having no out binds and returning implicit results.
   */
  @Test
  public void testNoOutImplicitResult() {
    Connection connection = awaitOne(sharedConnection());
    try {
      awaitExecution(connection.createStatement(
        "CREATE TABLE testNoOutImplicitResult (count NUMBER)"));

      // Load [0,100] into the table
      Statement insert = connection.createStatement(
        "INSERT INTO testNoOutImplicitResult VALUES (?)");
      IntStream.rangeClosed(0, 100)
        .forEach(i -> insert.bind(0, i).add());
      awaitOne(101, Flux.from(insert.execute())
        .reduce(0, (updateCount, result) ->
          updateCount + awaitOne(result.getRowsUpdated())));

      // Create a procedure that returns a cursor
      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE countDown (" +
          " countFrom IN NUMBER DEFAULT 100)" +
          " IS" +
          " countDownCursor SYS_REFCURSOR;" +
          " BEGIN" +
          " OPEN countDownCursor FOR " +
          "   SELECT count FROM testNoOutImplicitResult" +
          "   WHERE count <= countFrom" +
          "   ORDER BY count DESC;" +
          " DBMS_SQL.RETURN_RESULT(countDownCursor);" +
          " END;"));

      // Execute without setting the countFrom parameter, and expect one
      // Result with rows counting down from the default countFrom value, 100
      awaitQuery(Stream.iterate(
        100, previous -> previous >= 0, previous -> previous - 1)
          .collect(Collectors.toList()),
        row -> row.get(0, Integer.class),
        connection.createStatement("BEGIN countDown; END;"));

      // Execute with with an in bind parameter, and expect one
      // Result with rows counting down from the parameter value
      awaitQuery(Stream.iterate(
        10, previous -> previous >= 0, previous -> previous - 1)
          .collect(Collectors.toList()),
        row -> row.get(0, Integer.class),
        connection.createStatement("BEGIN countDown(?); END;")
          .bind(0, 10));

      // Create a procedure that returns multiple cursors
      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE countDown (" +
          " countFrom IN NUMBER DEFAULT 50)" +
          " IS" +
          " countDownCursor SYS_REFCURSOR;" +
          " countUpCursor SYS_REFCURSOR;" +
          " BEGIN" +

          " OPEN countDownCursor FOR " +
          "   SELECT count FROM testNoOutImplicitResult" +
          "   WHERE count <= countFrom" +
          "   ORDER BY count DESC;" +
          " DBMS_SQL.RETURN_RESULT(countDownCursor);" +

          " OPEN countUpCursor FOR " +
          "   SELECT count FROM testNoOutImplicitResult" +
          "   WHERE count >= countFrom" +
          "   ORDER BY count;" +
          " DBMS_SQL.RETURN_RESULT(countUpCursor);" +

          " END;"));


      awaitMany(asList(
        // countDownCursor
        Stream.iterate(
          50, previous -> previous >= 0, previous -> previous - 1)
          .collect(Collectors.toList()),
        // countUpCursor
        Stream.iterate(
          50, previous -> previous <= 100, previous -> previous + 1)
          .collect(Collectors.toList())),
        // Map rows of two Result.map(..) publishers into two Lists
        Flux.from(connection.createStatement("BEGIN countDown; END;")
          .execute())
          .flatMap(result ->
            Flux.from(result.map((row, metadata) ->
              row.get(0, Integer.class)))
              .collectList()));

      // Expect Implicit Results to have no update counts
      AtomicInteger count = new AtomicInteger(-9);
      awaitMany(asList(-9, -10),
        Flux.from(connection.createStatement("BEGIN countDown; END;")
          .execute())
          .flatMap(result ->
            Flux.from(result.getRowsUpdated())
              .defaultIfEmpty(count.getAndDecrement())));

    }
    finally {
      tryAwaitExecution(connection.createStatement("DROP PROCEDURE countDown"));
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testNoOutImplicitResult"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verify {@link OracleStatementImpl#execute()} when calling a procedure
   * having out binds and returning implicit results.
   */
  @Test
  public void testOutAndImplicitResult() {
    Connection connection = awaitOne(sharedConnection());
    try {
      awaitExecution(connection.createStatement(
        "CREATE TABLE testOutAndImplicitResult (count NUMBER)"));

      // Load [0,100] into the table
      Statement insert = connection.createStatement(
        "INSERT INTO testOutAndImplicitResult VALUES (?)");
      IntStream.rangeClosed(0, 100)
        .forEach(i -> insert.bind(0, i).add());
      awaitOne(101, Flux.from(insert.execute())
        .reduce(0, (updateCount, result) ->
          updateCount + awaitOne(result.getRowsUpdated())));

      // Create a procedure that returns a cursor
      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE countDown (" +
          " outValue OUT VARCHAR2)" +
          " IS" +
          " countDownCursor SYS_REFCURSOR;" +
          " BEGIN" +
          " outValue := 'test';" +
          " OPEN countDownCursor FOR " +
          "   SELECT count FROM testOutAndImplicitResult" +
          "   WHERE count <= 100" +
          "   ORDER BY count DESC;" +
          " DBMS_SQL.RETURN_RESULT(countDownCursor);" +
          " END;"));

      // Expect one Result with rows counting down from 100, then one Result
      // with the out bind value
      awaitMany(asList(
        Stream.iterate(
          100, previous -> previous >= 0, previous -> previous - 1)
          .map(String::valueOf)
          .collect(Collectors.toList()),
        asList("test")),
        Flux.from(connection.createStatement("BEGIN countDown(:outValue); END;")
          .bind("outValue", Parameters.out(R2dbcType.VARCHAR))
          .execute())
          .flatMap(result ->
            Flux.from(result.map((row, metadata) ->
              row.get(0, String.class)))
              .collectList()));

      // Create a procedure that returns multiple cursors
      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE countDown (" +
          " outValue OUT VARCHAR2)" +
          " IS" +
          " countDownCursor SYS_REFCURSOR;" +
          " countUpCursor SYS_REFCURSOR;" +
          " BEGIN" +

          " outValue := 'test';" +

          " OPEN countDownCursor FOR " +
          "   SELECT count FROM testOutAndImplicitResult" +
          "   WHERE count <= 50" +
          "   ORDER BY count DESC;" +
          " DBMS_SQL.RETURN_RESULT(countDownCursor);" +

          " OPEN countUpCursor FOR " +
          "   SELECT count FROM testOutAndImplicitResult" +
          "   WHERE count >= 50" +
          "   ORDER BY count;" +
          " DBMS_SQL.RETURN_RESULT(countUpCursor);" +

          " END;"));


      awaitMany(asList(
        // countDownCursor
        Stream.iterate(
          50, previous -> previous >= 0, previous -> previous - 1)
          .map(String::valueOf)
          .collect(Collectors.toList()),
        // countUpCursor
        Stream.iterate(
          50, previous -> previous <= 100, previous -> previous + 1)
          .map(String::valueOf)
          .collect(Collectors.toList()),
        asList("test")),
        // Map rows of two Result.map(..) publishers into two Lists
        Flux.from(connection.createStatement("BEGIN countDown(:outValue); END;")
          .bind("outValue", Parameters.out(R2dbcType.VARCHAR))
          .execute())
          .flatMap(result ->
            Flux.from(result.map((row, metadata) ->
              row.get(0, String.class)))
              .collectList()));

      // Expect Implicit Results to have no update counts
      AtomicInteger count = new AtomicInteger(-8);
      awaitMany(asList(-8, -9, -10),
        Flux.from(connection.createStatement("BEGIN countDown(?); END;")
          .bind(0, Parameters.out(R2dbcType.VARCHAR))
          .execute())
          .flatMap(result ->
            Flux.from(result.getRowsUpdated())
              .defaultIfEmpty(count.getAndDecrement())));

    }
    finally {
      tryAwaitExecution(connection.createStatement("DROP PROCEDURE countDown"));
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testOutAndImplicitResult"));
      tryAwaitNone(connection.close());
    }
  }

  // TODO: Repalce with Parameters.inOut when that's available
  private static final class InOutParameter
    implements Parameter, Parameter.In, Parameter.Out {
    final Type type;
    final Object value;

    InOutParameter(Object value) {
      this(value, new Type.InferredType() {
        @Override
        public Class<?> getJavaType() {
          return value.getClass();
        }

        @Override
        public String getName() {
          return "Inferred";
        }
      });
    }

    InOutParameter(Object value, Type type) {
      this.value = value;
      this.type = type;
    }

    @Override
    public Type getType() {
      return type;
    }

    @Override
    public Object getValue() {
      return value;
    }
  }
}
