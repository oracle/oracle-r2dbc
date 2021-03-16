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
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.sql.RowId;
import java.util.Collections;

import static java.util.Arrays.asList;
import static oracle.r2dbc.DatabaseConfig.connectTimeout;
import static oracle.r2dbc.DatabaseConfig.newConnection;
import static oracle.r2dbc.DatabaseConfig.sharedConnection;
import static oracle.r2dbc.util.Awaits.awaitError;
import static oracle.r2dbc.util.Awaits.awaitExecution;
import static oracle.r2dbc.util.Awaits.awaitNone;
import static oracle.r2dbc.util.Awaits.awaitOne;
import static oracle.r2dbc.util.Awaits.awaitQuery;
import static oracle.r2dbc.util.Awaits.awaitUpdate;
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
      class UnsupportedType {
      }
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
      awaitExecution(connection.createStatement(
        "DROP TABLE testBindByIndex"));
      awaitNone(connection.close());
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
    }
    finally {
      awaitExecution(connection.createStatement("DROP TABLE testBindByName"));
      awaitNone(connection.close());
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
      awaitExecution(connection.createStatement(
        "DROP TABLE testBindNullByIndex"));
      awaitNone(connection.close());
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
    }
    finally {
      awaitExecution(connection.createStatement(
        "DROP TABLE testNullBindByName"));
      awaitNone(connection.close());
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
      awaitExecution(connection.createStatement(
        "DROP TABLE testAdd"));
      awaitNone(connection.close());
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
      awaitExecution(connection.createStatement("DROP TABLE testExecute"));
      awaitNone(connection.close());
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
      awaitError(R2dbcException.class,
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

      // Expect row data when generated column names are specified for a SQL
      // query
      awaitQuery(asList(
        asList(1, "TEST1"),
        asList(2, "TEST2"),
        asList(3, "TEST3"),
        asList(4, "TEST4"),
        asList(5, "a")),
        row -> asList(row.get("x", Integer.class), row.get("y", String.class)),
        connection.createStatement(
          "SELECT x, y" +
            " FROM testReturnGeneratedValues" +
            " WHERE x < :old_x" +
            " ORDER BY x")
          .bind("old_x", 10)
          .returnGeneratedValues("x"));

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
      awaitExecution(connection.createStatement(
        "DROP TABLE testReturnGeneratedValues"));
      awaitNone(connection.close());
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
      awaitNone(connection.close());
    }
  }
}
