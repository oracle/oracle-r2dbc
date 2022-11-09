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
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientException;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Result.UpdateCount;
import io.r2dbc.spi.Statement;
import oracle.r2dbc.OracleR2dbcObject;
import oracle.r2dbc.OracleR2dbcOptions;
import oracle.r2dbc.OracleR2dbcTypes;
import oracle.r2dbc.OracleR2dbcWarning;
import oracle.r2dbc.test.DatabaseConfig;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;

import java.sql.RowId;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static oracle.r2dbc.test.DatabaseConfig.connectTimeout;
import static oracle.r2dbc.test.DatabaseConfig.connectionFactoryOptions;
import static oracle.r2dbc.test.DatabaseConfig.newConnection;
import static oracle.r2dbc.test.DatabaseConfig.sharedConnection;
import static oracle.r2dbc.test.TestUtils.constructObject;
import static oracle.r2dbc.test.TestUtils.showErrors;
import static oracle.r2dbc.test.DatabaseConfig.sqlTimeout;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
          .bind(0, 1).bind(1, 2));

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
          .bind("X", 1).bind("Y", 2));

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

      // Expect NoSuchElementException for an unmatched identifier
      assertThrows(
        NoSuchElementException.class,
        () -> statement.bind("z", 1));
      assertThrows(
        NoSuchElementException.class,
        () -> statement.bind("xx", 1));
      assertThrows(
        NoSuchElementException.class,
        () -> statement.bind("", 1));
      assertThrows(
        NoSuchElementException.class,
        () -> statement.bind("X", 1));
      assertThrows(
        NoSuchElementException.class,
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
          .bind(0, 0).bindNull(1, Integer.class));
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

      // Expect NoSuchElementException for an unmatched identifier
      assertThrows(
        NoSuchElementException.class,
        () -> selectStatement.bindNull("z", Integer.class));
      assertThrows(
        NoSuchElementException.class,
        () -> selectStatement.bindNull("xx", Integer.class));
      assertThrows(
        NoSuchElementException.class,
        () -> selectStatement.bindNull("", Integer.class));
      assertThrows(
        NoSuchElementException.class,
        () -> selectStatement.bindNull("X", Integer.class));
      assertThrows(
        NoSuchElementException.class,
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
          .bind("x", 0).bindNull("y", Integer.class));
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
          .add().add());
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
          .bind("x", 1).bind("y", 3));
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
        Mono.from(connection.createStatement("SELECT ? FROM dual")
          .bind(0, 1).add()
          .bind(0, 2).add()
          .bind(0, 3)
          .execute())
          .flatMapMany(Result::getRowsUpdated));

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

      // Expect the statement to execute with previously added binds, and
      // then emit an error if binds are missing in the final set of binds.
      List<Signal<Long>> signals =
        awaitOne(Flux.from(connection.createStatement(
          "INSERT INTO testAdd VALUES (:x, :y)")
          .bind("x", 0).bind("y", 1).add()
          .bind("y", 1).execute())
          .flatMap(Result::getRowsUpdated)
          .materialize()
          .collectList());
      assertEquals(2, signals.size());
      assertEquals(1, signals.get(0).get());
      assertTrue(
        signals.get(1).getThrowable() instanceof R2dbcNonTransientException);

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
          .bind("oldValue", 0).bind("newValue", 1));

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
      awaitOne(1L,
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
      awaitError(R2dbcException.class,
        Flux.from(statement.bind(0, "test").execute())
          .flatMap(result ->
            result.map(generatedValues -> fail("Unexpected row"))));

      // Expect a ROWID value when no column names are specified
      Statement rowIdQuery = connection.createStatement(
        "SELECT x, y FROM testReturnGeneratedValues WHERE rowid=?");
      RowId rowId = awaitOne(Mono.from(statement.returnGeneratedValues()
          .bind(0, "test1")
          .execute())
          .flatMapMany(result ->
            result.map(row -> row.get(0, RowId.class))));
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

      // Expect a normal row data result when executing a SELECT statement,
      // even if the Statement is configured to return columns generated by DML.
      awaitQuery(asList(
        asList(1, "TEST1"),
        asList(2, "TEST2"),
        asList(3, "TEST3"),
        asList(4, "TEST4")),
        row -> asList(row.get("x", Integer.class), row.get("y", String.class)),
        connection.createStatement(
          "SELECT x, y" +
            " FROM testReturnGeneratedValues" +
            " WHERE x < :old_x" +
            " ORDER BY x")
          .bind("old_x", 10)
          .returnGeneratedValues("x"));

      // Expect the column names to be ignored if the SQL is not an INSERT or
      // UPDATE
      awaitUpdate(4, connection.createStatement(
        "DELETE FROM testReturnGeneratedValues WHERE x < :old_x")
        .bind("old_x", 10)
        .returnGeneratedValues("x", "y"));

      // Expect no generated values if an UPDATE doesn't effect any rows.
      awaitUpdate(0, connection.createStatement(
        "UPDATE testReturnGeneratedValues SET y = 'effected' WHERE x IS NULL")
        .returnGeneratedValues("y"));
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
      awaitNone(Mono.from(connection.createStatement(
        "BEGIN testNoOutCallAdd; END;")
        .execute())
        .flatMapMany(Result::getRowsUpdated));
      awaitQuery(asList("Default Value"),
        row -> row.get(0),
        connection.createStatement("SELECT * FROM testNoOutCall"));

      // Execute the procedure again with no out parameters. Expect a single
      // Result with no rows. Expect the IN parameter's default value to have
      // been inserted by the call.
      awaitNone(Mono.from(connection.createStatement(
        "BEGIN testNoOutCallAdd; END;")
        .execute())
        .flatMap(result ->
          Mono.from(result.map(row -> "Unexpected"))));
      awaitQuery(asList("Default Value", "Default Value"),
        row -> row.get(0),
        connection.createStatement("SELECT * FROM testNoOutCall"));

      // Delete the previously inserted rows
      awaitExecution(connection.createStatement(
        "TRUNCATE TABLE testNoOutCall"));

      // Execute the procedure with no out parameters. Expect a single Result
      // with no update count. Expect the an indexed based String bind to
      // have been inserted by the call.
      awaitNone(Mono.from(connection.createStatement(
        "BEGIN testNoOutCallAdd(?); END;")
        .bind(0, "Indexed Bind")
        .execute())
        .flatMap(result -> Mono.from(result.getRowsUpdated())));
      awaitQuery(asList("Indexed Bind"),
        row -> row.get(0),
        connection.createStatement("SELECT * FROM testNoOutCall"));

      // Execute the procedure with no out parameters. Expect a single Result
      // with no update count. Expect the a named String bind to have been
      // inserted by the call.
      awaitNone(Mono.from(connection.createStatement(
        "BEGIN testNoOutCallAdd(:parameter); END;")
        .bind("parameter", "Named Bind")
        .execute())
        .flatMap(result ->
          Mono.from(result.map(row -> "Unexpected"))));
      awaitQuery(asList("Indexed Bind", "Named Bind"),
        row -> row.get(0),
        connection.createStatement(
          "SELECT * FROM testNoOutCall ORDER BY value"));

      // Delete the previously inserted rows
      awaitExecution(connection.createStatement(
        "TRUNCATE TABLE testNoOutCall"));

      // Execute the procedure with no out parameters. Expect a single Result
      // with no update count. Expect the an indexed based Parameter bind to
      // have been inserted by the call.
      awaitNone(Mono.from(connection.createStatement(
        "BEGIN testNoOutCallAdd(?); END;")
        .bind(0, Parameters.in(R2dbcType.VARCHAR, "Indexed Parameter"))
        .execute())
        .flatMap(result ->
          Mono.from(result.getRowsUpdated())));
      awaitQuery(asList("Indexed Parameter"),
        row -> row.get(0),
        connection.createStatement("SELECT * FROM testNoOutCall"));

      // Execute the procedure with no out parameters. Expect a single Result
      // with no update count. Expect the a named Parameter bind to have been
      // inserted by the call.
      awaitNone(Mono.from(connection.createStatement(
        "BEGIN testNoOutCallAdd(:parameter); END;")
        .bind("parameter",
          Parameters.in(R2dbcType.VARCHAR, "Named Parameter"))
        .execute())
        .flatMap(result ->
          Mono.from(result.map(row -> "Unexpected"))));
      awaitQuery(asList("Indexed Parameter", "Named Parameter"),
        row -> row.get(0),
        connection.createStatement(
          "SELECT * FROM testNoOutCall ORDER BY value"));

      // Delete the previously inserted rows
      awaitExecution(connection.createStatement(
        "TRUNCATE TABLE testNoOutCall"));

      // Execute the procedure with no out parameters. Expect a single Result
      // with no update count. Expect the an indexed based Parameter.In bind to
      // have been inserted by the call.
      awaitNone(Mono.from(connection.createStatement(
        "BEGIN testNoOutCallAdd(?); END;")
        .bind(0, Parameters.in(R2dbcType.VARCHAR, "Indexed Parameter.In"))
        .execute())
        .flatMap(result ->
          Mono.from(result.getRowsUpdated())));
      awaitQuery(asList("Indexed Parameter.In"),
        row -> row.get(0),
        connection.createStatement("SELECT * FROM testNoOutCall"));

      // Execute the procedure with no out parameters. Expect a single Result
      // with no update count. Expect the a named Parameter.In bind to have been
      // inserted by the call.
      awaitNone(Mono.from(connection.createStatement(
        "BEGIN testNoOutCallAdd(:parameter); END;")
        .bind("parameter",
          Parameters.in(R2dbcType.VARCHAR, "Named Parameter.In"))
        .execute())
        .flatMap(result ->
          Mono.from(result.map(row -> "Unexpected"))));
      awaitQuery(asList("Indexed Parameter.In", "Named Parameter.In"),
        row -> row.get(0),
        connection.createStatement(
          "SELECT * FROM testNoOutCall ORDER BY value"));
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
      consumeOne(connection.createStatement(
        "BEGIN testOneInOutCallAdd(?); END;")
        .bind(0, Parameters.inOut(R2dbcType.NUMERIC, 1))
        .execute(),
        result -> {
          awaitNone(result.getRowsUpdated());
        });
      awaitQuery(asList(1),
        row -> row.get("value", Integer.class),
        connection.createStatement("SELECT * FROM testOneInOutCall"));

      // Execute the procedure again with one in-out parameter. Expect a single
      // Result with one rows having the previous value. Expect the IN
      // parameter's default value to have been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testOneInOutCallAdd(:value); END;")
        .bind("value", Parameters.inOut(R2dbcType.NUMERIC, 2))
        .execute(),
        result ->
          awaitOne(1, result.map(row ->
            row.get("value", Integer.class))));
      awaitQuery(asList(2),
        row -> row.get(0, Integer.class),
        connection.createStatement("SELECT * FROM testOneInOutCall"));

      // Execute the procedure with one in-out parameter having an inferred
      // type. Expect a single Result with no update count. Expect the IN
      // parameter's value to have been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testOneInOutCallAdd(:value); END;")
        .bind("value", Parameters.inOut(3))
        .execute(),
        result ->
          awaitNone(result.getRowsUpdated()));
      awaitQuery(asList(3),
        row -> row.get(0, Integer.class),
        connection.createStatement("SELECT * FROM testOneInOutCall"));;

      // Execute the procedure again with one in-out parameter. Expect a single
      // Result with one rows having the previous value. Expect the IN
      // parameter's value to have been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testOneInOutCallAdd(?); END;")
        .bind(0, Parameters.inOut(4))
        .execute(),
        result ->
          awaitOne(3, result.map(row ->
            row.get(0, Integer.class))));
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
      consumeOne(connection.createStatement(
        "BEGIN testMultiInOutCallAdd(:value1, :value2); END;")
        .bind("value1", Parameters.inOut(R2dbcType.NUMERIC, 1))
        .bind("value2", Parameters.inOut(R2dbcType.NUMERIC, 101))
        .execute(),
        result ->
          awaitNone(result.getRowsUpdated()));
      awaitQuery(asList(asList(1, 101)),
        row -> asList(
          row.get("value1", Integer.class),row.get("value2", Integer.class)),
        connection.createStatement("SELECT * FROM testMultiInOutCall"));

      // Execute the procedure again with one in-out parameter. Expect a single
      // Result with one rows having the previous value. Expect the IN
      // parameter's default value to have been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testMultiInOutCallAdd(?, :value2); END;")
        .bind(0, Parameters.inOut(R2dbcType.NUMERIC, 2))
        .bind("value2", Parameters.inOut(R2dbcType.NUMERIC, 102))
        .execute(),
        result ->
          awaitOne(asList(1, 101), result.map(row ->
            asList(
              row.get(0, Integer.class), row.get("value2", Integer.class)))));
      awaitQuery(asList(asList(2, 102)),
        row ->
          asList(row.get("value1", Integer.class), row.get(1, Integer.class)),
        connection.createStatement("SELECT * FROM testMultiInOutCall"));

      // Execute the procedure with one in-out parameter having an inferred
      // type. Expect a single Result with no update count. Expect the IN
      // parameter's value to have been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testMultiInOutCallAdd(?, ?); END;")
        .bind(0, Parameters.inOut(3))
        .bind(1, Parameters.inOut(103))
        .execute(),
        result -> awaitNone(result.getRowsUpdated()));
      awaitQuery(asList(asList(3, 103)),
        row -> asList(row.get(0, Integer.class), row.get(1, Integer.class)),
        connection.createStatement("SELECT * FROM testMultiInOutCall"));;

      // Execute the procedure again with multiple in-out parameters having
      // the same name. Expect a single Result with one rows having the
      // previous value. Getting the parameter value by name should returned
      // the value of the first parameter. Expect the IN parameter's value to
      // have been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testMultiInOutCallAdd(" +
          "inout_value2 => :value2, inout_value1 => :value1); END;")
        .bind("value1", Parameters.inOut(4))
        .bind("value2", Parameters.inOut(104))
        .execute(),
        result ->
          awaitOne(asList(3, 103), result.map(row ->
            asList(
              row.get("value1", Integer.class), row.get(0, Integer.class)))));
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
      consumeOne(connection.createStatement(
        "BEGIN testOneOutCallAdd(id => ?); END;")
        .bind(0, Parameters.out(R2dbcType.NUMERIC))
        .execute(),
        result -> awaitNone(result.getRowsUpdated()));
      awaitQuery(asList(asList(1, "Default Value")),
        row -> asList(row.get("id", Integer.class), row.get("value")),
        connection.createStatement("SELECT * FROM testOneOutCall"));

      // Execute the procedure again with one in-out parameter. Expect a single
      // Result with one rows Expect the IN parameter's default value to have
      // been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testOneOutCallAdd(id => ?); END;")
        .bind(0, Parameters.out(R2dbcType.NUMERIC))
        .execute(),
        result ->
          awaitOne(2, result.map(row -> row.get(0, Integer.class))));
      awaitQuery(asList(asList(1, "Default Value"), asList(2, "Default Value")),
        row -> asList(row.get("id", Integer.class), row.get("value")),
        connection.createStatement("SELECT * FROM testOneOutCall"));

      // Delete the previously inserted rows
      awaitExecution(connection.createStatement(
        "TRUNCATE TABLE testOneOutCall"));

      // Execute the procedure with one in-out parameter. Expect a single Result
      // with no update count. Expect the an indexed based String bind to
      // have been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testOneOutCallAdd(?, ?); END;")
        .bind(0, "Indexed Bind")
        .bind(1, Parameters.out(R2dbcType.NUMERIC))
        .execute(),
        result -> awaitNone(result.getRowsUpdated()));
      awaitQuery(asList(asList(3, "Indexed Bind")),
        row -> asList(row.get(0, Integer.class), row.get(1)),
        connection.createStatement("SELECT * FROM testOneOutCall"));

      // Execute the procedure with one in-out parameter. Expect a single Result
      // with no update count. Expect the a named String bind to have been
      // inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testOneOutCallAdd(:parameter, :out); END;")
        .bind("parameter", "Named Bind")
        .bind("out", Parameters.out(R2dbcType.NUMERIC))
        .execute(),
        result ->
          awaitOne(4,
            result.map(row -> row.get("out", Integer.class))));
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
      consumeOne(connection.createStatement(
        "BEGIN testOneOutCallAdd(?, ?); END;")
        .bind(0, Parameters.in(R2dbcType.VARCHAR, "Indexed Parameter"))
        .bind(1, Parameters.out(R2dbcType.NUMERIC))
        .execute(),
        result -> awaitNone(result.getRowsUpdated()));
      awaitQuery(asList(asList(5, "Indexed Parameter")),
        row -> asList(row.get(0, Integer.class), row.get(1)),
        connection.createStatement("SELECT * FROM testOneOutCall"));

      // Execute the procedure with one in-out parameter. Expect a single Result
      // with no update count. Expect the a named Parameter bind to have been
      // inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testOneOutCallAdd(:parameter, :out); END;")
        .bind("parameter",
          Parameters.in(R2dbcType.VARCHAR, "Named Parameter"))
        .bind("out", Parameters.out(R2dbcType.NUMERIC))
        .execute(),
        result ->
          awaitOne(6,
            result.map(row -> row.get("out", Integer.class))));
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
      consumeOne(connection.createStatement(
        "BEGIN testOneOutCallAdd(?, ?); END;")
        .bind(0, Parameters.in(R2dbcType.VARCHAR, "Indexed Parameter.In"))
        .bind(1, Parameters.out(R2dbcType.NUMERIC))
        .execute(),
        result -> awaitNone(result.getRowsUpdated()));
      awaitQuery(asList(asList(7, "Indexed Parameter.In")),
        row -> asList(row.get(0, Integer.class), row.get(1)),
        connection.createStatement("SELECT * FROM testOneOutCall"));

      // Execute the procedure with one in-out parameter. Expect a single Result
      // with no update count. Expect the a named Parameter.In bind to have been
      // inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testOneOutCallAdd(:parameter, :out); END;")
        .bind("parameter",
          Parameters.in(R2dbcType.VARCHAR, "Named Parameter.In"))
        .bind("out", Parameters.out(R2dbcType.NUMERIC))
        .execute(),
        result ->
          awaitOne(result.map(row -> row.get("out"))));
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
      consumeOne(connection.createStatement(
        "BEGIN testMultiOutCallAdd(id => ?, new_count => ?); END;")
        .bind(0, Parameters.out(R2dbcType.NUMERIC))
        .bind(1, Parameters.out(R2dbcType.NUMERIC))
        .execute(),
        result -> awaitNone(result.getRowsUpdated()));
      awaitQuery(asList(asList(1, "Default Value")),
        readable -> asList(readable.get("id", Integer.class), readable.get("value")),
        connection.createStatement("SELECT * FROM testMultiOutCall"));

      // Execute the procedure again with two out parameters. Expect a single
      // Result with one readables Expect the IN parameter's default value to have
      // been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testMultiOutCallAdd(id => ?, new_count => ?); END;")
        .bind(0, Parameters.out(R2dbcType.NUMERIC))
        .bind(1, Parameters.out(R2dbcType.NUMERIC))
        .execute(),
        result ->
          awaitOne(asList(2, 2), result.map(readable ->
            asList(readable.get(0, Integer.class), readable.get(1, Integer.class)))));
      awaitQuery(asList(asList(1, "Default Value"), asList(2, "Default Value")),
        readable -> asList(readable.get("id", Integer.class), readable.get("value")),
        connection.createStatement("SELECT * FROM testMultiOutCall"));

      // Delete the previously inserted readables
      awaitExecution(connection.createStatement(
        "TRUNCATE TABLE testMultiOutCall"));

      // Execute the procedure with two out parameters. Expect a single Result
      // with no update count. Expect the an indexed based String bind to
      // have been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testMultiOutCallAdd(?, ?, ?); END;")
        .bind(0, "Indexed Bind")
        .bind(1, Parameters.out(R2dbcType.NUMERIC))
        .bind(2, Parameters.out(R2dbcType.NUMERIC))
        .execute(),
        result -> awaitNone(result.getRowsUpdated()));
      awaitQuery(asList(asList(3, "Indexed Bind")),
        readable -> asList(readable.get(0, Integer.class), readable.get(1)),
        connection.createStatement("SELECT * FROM testMultiOutCall"));

      // Execute the procedure with two out parameters. Expect a single Result
      // with no update count. Expect the a named String bind to have been
      // inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testMultiOutCallAdd(:parameter, :out, :newCount); END;")
        .bind("parameter", "Named Bind")
        .bind("out", Parameters.out(R2dbcType.NUMERIC))
        .bind("newCount", Parameters.out(R2dbcType.NUMERIC))
        .execute(),
        result ->
          awaitOne(asList(4, 2), result.map(readable ->
            asList(readable.get("out", Integer.class),
              readable.get("newCount", Integer.class)))));
      awaitQuery(asList(asList(3, "Indexed Bind"), asList(4, "Named Bind")),
        readable -> asList(readable.get(0, Integer.class), readable.get(1)),
        connection.createStatement(
          "SELECT * FROM testMultiOutCall ORDER BY value"));

      // Delete the previously inserted readables
      awaitExecution(connection.createStatement(
        "TRUNCATE TABLE testMultiOutCall"));

      // Execute the procedure with two out parameters. Expect a single Result
      // with no update count. Expect the an indexed based Parameter bind to
      // have been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testMultiOutCallAdd(?, ?, ?); END;")
        .bind(0, Parameters.in(R2dbcType.VARCHAR, "Indexed Parameter"))
        .bind(1, Parameters.out(R2dbcType.NUMERIC))
        .bind(2, Parameters.out(R2dbcType.NUMERIC))
        .execute(),
        result -> awaitNone(result.getRowsUpdated()));
      awaitQuery(asList(asList(5, "Indexed Parameter")),
        readable -> asList(readable.get(0, Integer.class), readable.get(1)),
        connection.createStatement("SELECT * FROM testMultiOutCall"));

      // Execute the procedure with two out parameters. Expect a single Result
      // with no update count. Expect the a named Parameter bind to have been
      // inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testMultiOutCallAdd(:parameter, :out, :newCount); END;")
        .bind("parameter",
          Parameters.in(R2dbcType.VARCHAR, "Named Parameter"))
        .bind("out", Parameters.out(R2dbcType.NUMERIC))
        .bind("newCount", Parameters.out(R2dbcType.NUMERIC))
        .execute(),
        result ->
          awaitOne(asList(6, 2), result.map(readable ->
            asList(readable.get("out", Integer.class),
              readable.get("newCount", Integer.class)))));
      awaitQuery(asList(
        asList(5, "Indexed Parameter"), asList(6, "Named Parameter")),
          readable -> asList(readable.get(0, Integer.class), readable.get(1)),
        connection.createStatement(
          "SELECT * FROM testMultiOutCall ORDER BY value"));

      // Delete the previously inserted readables
      awaitExecution(connection.createStatement(
        "TRUNCATE TABLE testMultiOutCall"));

      // Execute the procedure with two out parameters. Expect a single Result
      // with no update count. Expect the an indexed based Parameter.In bind to
      // have been inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testMultiOutCallAdd(?, ?, ?); END;")
        .bind(0, Parameters.in(R2dbcType.VARCHAR, "Indexed Parameter.In"))
        .bind(1, Parameters.out(R2dbcType.NUMERIC))
        .bind(2, Parameters.out(R2dbcType.NUMERIC))
        .execute(),
        result -> awaitNone(result.getRowsUpdated()));
      awaitQuery(asList(asList(7, "Indexed Parameter.In")),
        readable -> asList(readable.get(0, Integer.class), readable.get(1)),
        connection.createStatement("SELECT * FROM testMultiOutCall"));

      // Execute the procedure with two out parameters. Expect a single Result
      // with no update count. Expect the a named Parameter.In bind to have been
      // inserted by the call.
      consumeOne(connection.createStatement(
        "BEGIN testMultiOutCallAdd(:parameter, :out, :newCount); END;")
        .bind("parameter",
          Parameters.in(R2dbcType.VARCHAR, "Named Parameter.In"))
        .bind("out", Parameters.out(R2dbcType.NUMERIC))
        .bind("newCount", Parameters.out(R2dbcType.NUMERIC))
        .execute(),
        result -> awaitOne(result.map(readable -> readable.get("out"))));
      awaitQuery(asList(
        asList(7, "Indexed Parameter.In"), asList(8, "Named Parameter.In")),
        readable -> asList(readable.get(0, Integer.class), readable.get(1)),
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
      IntStream.range(0, 100)
        .forEach(i -> insert.bind(0, i).add());
      insert.bind(0, 100);
      awaitOne(101L, Flux.from(insert.execute())
        .flatMap(Result::getRowsUpdated)
        .reduce(0L, (total, updateCount) -> total + updateCount));

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
          .concatMap(result ->
            Flux.from(result.map(row ->
              row.get(0, Integer.class)))
              .collectList()));

      // Expect Implicit Results to have no update counts
      AtomicLong count = new AtomicLong(-9);
      awaitMany(asList(-9L, -10L),
        Flux.from(connection.createStatement("BEGIN countDown; END;")
          .execute())
          .concatMap(result ->
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
      IntStream.range(0, 100)
        .forEach(i -> insert.bind(0, i).add());
      insert.bind(0, 100);
      awaitOne(101L, Flux.from(insert.execute())
        .flatMap(Result::getRowsUpdated)
        .reduce(0L, (total, updateCount) -> total + updateCount));

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
          .concatMap(result ->
            Flux.from(result.map(row ->
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
          .concatMap(result ->
            Flux.from(result.map(row ->
              row.get(0, String.class)))
              .collectList()));

      // Expect Implicit Results to have no update counts
      AtomicLong count = new AtomicLong(-8);
      awaitMany(asList(-8L, -9L, -10L),
        Flux.from(connection.createStatement("BEGIN countDown(?); END;")
          .bind(0, Parameters.out(R2dbcType.VARCHAR))
          .execute())
          .concatMap(result ->
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

  /**
   * Verifies that {@link OracleStatementImpl#execute()} emits a {@link Result}
   * with a {@link OracleR2dbcWarning} segment when the execution results in a
   * warning.
   */
  @Test
  public void testWarningMessage() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {

      // Create a procedure using invalid syntax and expect the Result to
      // have an OracleR2dbcWarning with an R2dbcException having a SQLWarning
      // as it's initial cause. Expect the Result to have an update count of
      // zero as well, indicating that the statement completed after the
      // warning.
      AtomicInteger segmentCount = new AtomicInteger(0);
      R2dbcException r2dbcException =
        awaitOne(Flux.from(connection.createStatement(
          "CREATE OR REPLACE PROCEDURE testWarningMessage" +
            " IS BEGIN;")
          .execute())
          .concatMap(result ->
            result.flatMap(segment -> {
              int index = segmentCount.getAndIncrement();
              if (index == 0) {
                // Expect the first segment to be an update count
                assertTrue(segment instanceof UpdateCount,
                  "Unexpected Segment: " + segment);
                assertEquals(0, ((UpdateCount)segment).value());
                return Mono.empty();
              }
              else if (index == 1) {
                // Expect second segment to be a warning
                assertTrue(segment instanceof OracleR2dbcWarning,
                  "Unexpected Segment: " + segment);
                return Mono.just(((OracleR2dbcWarning)segment).exception());
              }
              else {
                fail("Unexpected Segment: " + segment);
                return Mono.error(new AssertionError("Should not reach here"));
              }
            })));

      // Expect ORA-17110 for an execution that completed with a warning
      assertEquals(17110, r2dbcException.getErrorCode());
      Throwable cause = r2dbcException.getCause();
      assertTrue(cause instanceof SQLWarning, "Unexpected cause: " + cause);
      assertEquals(17110, ((SQLWarning)cause).getErrorCode());
      assertNull(cause.getCause());
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies that concurrent statement execution on a single
   * connection does not cause threads to block when there are many threads
   * available.
   */
  @Test
  public void testConcurrentExecuteManyThreads() throws InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(4);
    try {
      Connection connection = awaitOne(connect(executorService));
      try {
        verifyConcurrentExecute(connection);
      }
      finally {
        tryAwaitNone(connection.close());
      }
    }
    finally {
      executorService.shutdown();
      executorService.awaitTermination(
        sqlTimeout().toSeconds(), TimeUnit.SECONDS);
    }
  }

  /**
   * Verifies that concurrent statement execution on a single
   * connection does not cause threads to block when there is just one thread
   * available.
   */
  @Test
  public void testConcurrentExecuteSingleThread() throws InterruptedException {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    try {
      Connection connection = awaitOne(connect(executorService));
      try {
        verifyConcurrentExecute(connection);
      }
      finally {
        tryAwaitNone(connection.close());
      }
    }
    finally {
      executorService.shutdown();
      executorService.awaitTermination(
        sqlTimeout().toSeconds(), TimeUnit.SECONDS);
    }
  }

  /**
   * Verifies that concurrent statement execution and row fetching on a single
   * connection does not cause threads to block when there is just one thread
   * available.
   */
  @Test
  public void testConcurrentFetchSingleThread() throws InterruptedException {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    try {
      Connection connection = awaitOne(connect(executorService));
      try {
        verifyConcurrentFetch(connection);
      }
      finally {
        tryAwaitNone(connection.close());
      }
    }
    finally {
      executorService.shutdown();
      executorService.awaitTermination(
        sqlTimeout().toSeconds(), TimeUnit.SECONDS);
    }
  }

  /**
   * Verifies that concurrent statement execution and row fetching on a single
   * connection does not cause threads to block when there are many threads
   * available.
   */
  @Test
  public void testConcurrentFetchManyThreads() throws InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(4);
    try {
      Connection connection = awaitOne(connect(executorService));
      try {
        verifyConcurrentFetch(connection);
      }
      finally {
        tryAwaitNone(connection.close());
      }
    }
    finally {
      executorService.shutdown();
      executorService.awaitTermination(
        sqlTimeout().toSeconds(), TimeUnit.SECONDS);
    }
  }

  /**
   * Verifies behavior when commitTransaction() and close() Publishers are
   * subscribed to concurrently due to cancelling a Flux.usingWhen(...)
   * operator.
   */
  @Test
  public void testUsingWhenCancel() {
    Connection connection = awaitOne(sharedConnection());
    try {
      awaitExecution(connection.createStatement(
        "CREATE TABLE testUsingWhenCancel (value NUMBER)"));

      // Use more threads than what the FJP has available
      @SuppressWarnings({"unchecked","rawtypes"})
      Publisher<Boolean>[] publishers =
        new Publisher[ForkJoinPool.getCommonPoolParallelism() * 4];

      for (int i = 0; i < publishers.length; i++) {

        int value = i;

        // The hasElements operator below will cancel its subscription upon
        // receiving onNext. This triggers a subscription to the
        // commitTransaction() publisher, immediately followed by a subscription
        // to the close() publisher. Expect the driver to defer the subscription
        // to the close() publisher until the commitTransaction publisher has
        // completed. If not deferred, then the thread subscribing to the close
        // publisher will block, and this test will deadlock as the
        // commitTransaction publisher has no available thread to complete with.
        Mono<Boolean> mono = Flux.usingWhen(
          newConnection(),
          newConnection ->
            Flux.usingWhen(
              Mono.from(newConnection.beginTransaction())
                .thenReturn(newConnection),
              newConnection0 ->
                Flux.from(newConnection.createStatement(
                  "INSERT INTO testUsingWhenCancel VALUES (?)")
                  .bind(0, value)
                  .execute())
                  .flatMap(Result::getRowsUpdated),
              Connection::commitTransaction),
          Connection::close)
          .hasElements()
          .cache();

        mono.subscribe();
        publishers[i] = mono;
      }

      awaitMany(
        Stream.generate(() -> true)
          .limit(publishers.length)
          .collect(Collectors.toList()),
        Flux.merge(publishers));

    }
    finally {
      // Note that Flux.usingWhen doesn't actually wait for the
      // commitTransaction publisher to complete (because the downstream
      // subscriber has already cancelled the subscription, so it can't
      // receive the result anyway).
      // This means the transactions may not have ended by the time the
      // drop table command executes. Set a DDL wait timeout to avoid a
      // "Resource busy..." error from the database.
      tryAwaitExecution(connection.createStatement(
        "ALTER SESSION SET ddl_lock_timeout=15"));
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testUsingWhenCancel"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies that {@link R2dbcException#getSql()} returns the SQL command
   * that caused an exception.
   */
  @Test
  public void testGetSql() {
    Connection connection = awaitOne(sharedConnection());
    try {
      String badSql = "SELECT 0 FROM dooool";
      Result result = awaitOne(connection.createStatement(badSql).execute());
      R2dbcException r2dbcException = assertThrows(R2dbcException.class, () ->
        awaitOne(result.getRowsUpdated()));
      assertEquals(badSql, r2dbcException.getSql());
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies that a SYS_REFCURSOR out parameter can be consumed as a
   * {@link Result} object.
   */
  @Test
  public void testRefCursorOut() {
    Connection connection = awaitOne(sharedConnection());
    try {
      List<TestRow> rows = createRows(100);

      // Create a table with some rows to query
      awaitExecution(connection.createStatement(
        "CREATE TABLE testRefCursorTable(id NUMBER, value VARCHAR(10))"));
      Statement insertStatement = connection.createStatement(
        "INSERT INTO testRefCursorTable VALUES (:id, :value)");
      awaitUpdate(
        rows.stream()
          .map(row -> 1)
          .collect(Collectors.toList()),
        bindRows(rows, insertStatement));

      // Create a procedure that returns a cursor over the rows
      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE testRefCursorProcedure(" +
          " countCursor OUT SYS_REFCURSOR)" +
          " IS" +
          " BEGIN" +
          " OPEN countCursor FOR " +
          "   SELECT id, value FROM testRefCursorTable" +
          "   ORDER BY id;" +
          " END;"));

      // Call the procedure with the cursor registered as an out parameter, and
      // expect it to map to a Result. Then consume the rows of the Result and
      // verify they have the expected values inserted above.
      awaitMany(
        rows,
        Flux.from(connection.createStatement(
              "BEGIN testRefCursorProcedure(:countCursor); END;")
            .bind("countCursor", Parameters.out(OracleR2dbcTypes.REF_CURSOR))
            .execute())
          .flatMap(result ->
            result.map(outParameters ->
              outParameters.get("countCursor")))
          .cast(Result.class)
          .flatMap(countCursor ->
            countCursor.map(row ->
              new TestRow(
                row.get("id", Integer.class),
                row.get("value", String.class)))));

      // Verify the procedure call again. This time using an explicit
      // Result.class argument to Row.get(...). Also, this time using
      // Result.flatMap to create the publisher within the segment mapping
      // function
      awaitMany(
        rows,
        Flux.from(connection.createStatement(
            "BEGIN testRefCursorProcedure(:countCursor); END;")
          .bind("countCursor", Parameters.out(OracleR2dbcTypes.REF_CURSOR))
          .execute())
          .flatMap(result ->
            result.flatMap(segment ->
              ((Result.OutSegment)segment).outParameters()
                  .get(0, Result.class)
                  .map(row ->
                    new TestRow(
                      row.get("id", Integer.class),
                      row.get("value", String.class))))));
    }
    catch (Exception exception) {
      showErrors(connection);
      throw exception;
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP PROCEDURE testRefCursorProcedure"));
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testRefCursorTable"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies that SYS_REFCURSOR out parameters can be consumed as
   * {@link Result} objects.
   */
  @Test
  public void testMultipleRefCursorOut() {
    Connection connection = awaitOne(sharedConnection());
    try {
      List<TestRow> rows = createRows(100);

      // Create a table with some rows to query
      awaitExecution(connection.createStatement(
        "CREATE TABLE testMultiRefCursorTable(id NUMBER, value VARCHAR(10))"));
      Statement insertStatement = connection.createStatement(
        "INSERT INTO testMultiRefCursorTable VALUES (:id, :value)");
      awaitUpdate(
        rows.stream()
          .map(row -> 1)
          .collect(Collectors.toList()),
        bindRows(rows, insertStatement));

      // Create a procedure that returns a multiple cursors over the rows
      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE testMultiRefCursorProcedure(" +
          " countCursor0 OUT SYS_REFCURSOR," +
          " countCursor1 OUT SYS_REFCURSOR)" +
          " IS" +
          " BEGIN" +
          " OPEN countCursor0 FOR " +
          "   SELECT id, value FROM testMultiRefCursorTable" +
          "   ORDER BY id;" +
          " OPEN countCursor1 FOR " +
          "   SELECT id, value FROM testMultiRefCursorTable" +
          "   ORDER BY id DESC;" +
          " END;"));

      // Call the procedure with the cursors registered as out parameters, and
      // expect them to map to Results. Then consume the rows of each Result and
      // verify they have the expected values inserted above.
      List<TestRow> expectedRows = new ArrayList<>(rows);
      Collections.reverse(rows);
      expectedRows.addAll(rows);
      awaitMany(
        expectedRows,
        Flux.from(connection.createStatement(
              "BEGIN testMultiRefCursorProcedure(:countCursor0, :countCursor1); END;")
            .bind("countCursor0", Parameters.out(OracleR2dbcTypes.REF_CURSOR))
            .bind("countCursor1", Parameters.out(OracleR2dbcTypes.REF_CURSOR))
            .execute())
          .flatMap(result ->
            result.map(outParameters ->
              List.of(
                (Result)outParameters.get("countCursor0"),
                (Result)outParameters.get("countCursor1"))))
          .flatMap(results ->
            Flux.concat(
              results.get(0).map(row ->
                new TestRow(
                  row.get("id", Integer.class),
                  row.get("value", String.class))),
              results.get(1).map(row ->
                new TestRow(
                  row.get("id", Integer.class),
                  row.get("value", String.class))))));

      // Run the same verification, this time with Result.class argument to
      // Row.get(...), and mapping the REF CURSOR Results into a Publisher
      // within the row mapping function
      awaitMany(
        expectedRows,
        Flux.from(connection.createStatement(
              "BEGIN testMultiRefCursorProcedure(:countCursor0, :countCursor1); END;")
            .bind("countCursor0", Parameters.out(OracleR2dbcTypes.REF_CURSOR))
            .bind("countCursor1", Parameters.out(OracleR2dbcTypes.REF_CURSOR))
            .execute())
          .flatMap(result ->
            result.map(outParameters ->
              Flux.concat(
                outParameters.get("countCursor0", Result.class).map(row ->
                  new TestRow(
                    row.get(0, Integer.class),
                    row.get(1, String.class))),
                outParameters.get("countCursor1", Result.class).map(row ->
                  new TestRow(
                    row.get(0, Integer.class),
                    row.get(1, String.class))))))
          .flatMap(Function.identity()));
    }
    catch (Exception exception) {
      showErrors(connection);
      throw exception;
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP PROCEDURE testMultiRefCursorProcedure"));
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testMultiRefCursorTable"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies behavior for a PL/SQL call having {@code ARRAY} type IN bind
   */
  @Test
  public void testInArrayCall() {
    Connection connection = awaitOne(sharedConnection());
    try {
      awaitExecution(connection.createStatement(
        "CREATE TYPE TEST_IN_ARRAY AS ARRAY(8) OF NUMBER"));
      awaitExecution(connection.createStatement(
        "CREATE TABLE testInArrayCall(id NUMBER, value TEST_IN_ARRAY)"));
      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE testInArrayProcedure (" +
          " id IN NUMBER," +
          " inArray IN TEST_IN_ARRAY)" +
          " IS" +
          " BEGIN" +
          " INSERT INTO testInArrayCall VALUES(id, inArray);" +
          " END;"));

      class TestRow {
        Long id;
        int[] value;
        TestRow(Long id, int[] value) {
          this.id = id;
          this.value = value;
        }
        @Override
        public boolean equals(Object other) {
          return other instanceof TestRow
            && Objects.equals(((TestRow) other).id, id)
            && Objects.deepEquals(((TestRow)other).value, value);
        }

        @Override
        public String toString() {
          return id + ", " + Arrays.toString(value);
        }
      }

      TestRow row0 = new TestRow(0L, new int[]{1, 2, 3});
      OracleR2dbcTypes.ArrayType arrayType =
        OracleR2dbcTypes.arrayType("TEST_IN_ARRAY");
      Statement callStatement = connection.createStatement(
        "BEGIN testInArrayProcedure(:id, :value); END;");
      awaitExecution(
        callStatement
          .bind("id", row0.id)
          .bind("value", Parameters.in(arrayType, row0.value)));

      awaitQuery(
        List.of(row0),
        row ->
          new TestRow(
            row.get("id", Long.class),
            row.get("value", int[].class)),
        connection.createStatement(
          "SELECT id, value FROM testInArrayCall ORDER BY id"));

      TestRow row1 = new TestRow(1L, new int[]{4, 5, 6});
      awaitExecution(
        callStatement
          .bind("id", row1.id)
          .bind("value", Parameters.in(arrayType, row1.value)));

      awaitQuery(
        List.of(row0, row1),
        row ->
          new TestRow(
            row.get("id", Long.class),
            row.get("value", int[].class)),
        connection.createStatement(
          "SELECT id, value FROM testInArrayCall ORDER BY id"));
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testInArrayCall"));
      tryAwaitExecution(connection.createStatement(
        "DROP PROCEDURE testInArrayProcedure"));
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE TEST_IN_ARRAY"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies behavior for a PL/SQL call having {@code ARRAY} type OUT bind
   */
  @Test
  public void testOutArrayCall() {
    Connection connection = awaitOne(sharedConnection());
    try {
      awaitExecution(connection.createStatement(
        "CREATE TYPE TEST_OUT_ARRAY AS ARRAY(8) OF NUMBER"));
      awaitExecution(connection.createStatement(
        "CREATE TABLE testOutArrayCall(id NUMBER, value TEST_OUT_ARRAY)"));
      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE testOutArrayProcedure (" +
          "   inId IN NUMBER," +
          "   outArray OUT TEST_OUT_ARRAY)" +
          " IS" +
          " BEGIN" +
          "   SELECT value INTO outArray" +
          "     FROM testOutArrayCall" +
          "     WHERE id = inId;" +
          " EXCEPTION" +
          "   WHEN NO_DATA_FOUND THEN" +
          "     outArray := NULL;" +
          " END;"));

      class TestRow {
        Long id;
        Integer[] value;
        TestRow(Long id, Integer[] value) {
          this.id = id;
          this.value = value;
        }

        @Override
        public boolean equals(Object other) {
          return other instanceof TestRow
            && Objects.equals(((TestRow) other).id, id)
            && Objects.deepEquals(((TestRow)other).value, value);
        }

        @Override
        public String toString() {
          return id + ", " + Arrays.toString(value);
        }
      }

      OracleR2dbcTypes.ArrayType arrayType =
        OracleR2dbcTypes.arrayType("TEST_OUT_ARRAY");
      Statement callStatement = connection.createStatement(
        "BEGIN testOutArrayProcedure(:id, :value); END;");

      // Expect a NULL out parameter before any rows have been inserted
      awaitQuery(
        List.of(Optional.empty()),
        outParameters -> {
          assertNull(outParameters.get("value"));
          assertNull(outParameters.get("value", int[].class));
          assertNull(outParameters.get("value", Integer[].class));
          return Optional.empty();
        },
        callStatement
          .bind("id", -1)
          .bind("value", Parameters.out(arrayType)));

      // Insert a row and expect an out parameter with the value
      TestRow row0 = new TestRow(0L, new Integer[]{1, 2, 3});
      awaitUpdate(1, connection.createStatement(
          "INSERT INTO testOutArrayCall VALUES (:id, :value)")
        .bind("id", row0.id)
        .bind("value", Parameters.in(arrayType, row0.value)));
      awaitQuery(
        List.of(row0),
        outParameters ->
          new TestRow(row0.id, outParameters.get("value", Integer[].class)),
        callStatement
          .bind("id", row0.id)
          .bind("value", Parameters.out(arrayType)));

      // Insert another row and expect an out parameter with the value
      TestRow row1 = new TestRow(1L, new Integer[]{4, 5, 6});
      awaitUpdate(1, connection.createStatement(
          "INSERT INTO testOutArrayCall VALUES (:id, :value)")
        .bind("id", row1.id)
        .bind("value", Parameters.in(arrayType, row1.value)));
      awaitQuery(
        List.of(row1),
        outParameters ->
          new TestRow(row1.id, outParameters.get("value", Integer[].class)),
        callStatement
          .bind("id", row1.id)
          .bind("value", Parameters.out(arrayType)));
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testOutArrayCall"));
      tryAwaitExecution(connection.createStatement(
        "DROP PROCEDURE testOutArrayProcedure"));
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE TEST_OUT_ARRAY"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies behavior for a PL/SQL call having {@code ARRAY} type OUT bind
   */
  @Test
  public void testInOutArrayCall() {
    Connection connection = awaitOne(sharedConnection());
    try {
      awaitExecution(connection.createStatement(
        "CREATE TYPE TEST_IN_OUT_ARRAY AS ARRAY(8) OF NUMBER"));
      awaitExecution(connection.createStatement(
        "CREATE TABLE testInOutArrayCall(id NUMBER, value TEST_IN_OUT_ARRAY)"));
      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE testInOutArrayProcedure (" +
          "   inId IN NUMBER," +
          "   inOutArray IN OUT TEST_IN_OUT_ARRAY)" +
          " IS" +
          " newValue TEST_IN_OUT_ARRAY;" +
          " BEGIN" +
          "" +
          " newValue := TEST_IN_OUT_ARRAY();" +
          " newValue.extend(inOutArray.count);" +
          " FOR i IN 1 .. inOutArray.count LOOP" +
          "   newValue(i) := inOutArray(i);" +
          " END LOOP;" +
          "" +
          " BEGIN" +
          "   SELECT value INTO inOutArray" +
          "     FROM testInOutArrayCall" +
          "     WHERE id = inId;" +
          "   DELETE FROM testInOutArrayCall WHERE id = inId;" +
          "   EXCEPTION" +
          "     WHEN NO_DATA_FOUND THEN" +
          "       inOutArray := NULL;" +
          " END;" +
          "" +
          " INSERT INTO testInOutArrayCall VALUES (inId, newValue);" +
          "" +
          " END;"));

      class TestRow {
        Long id;
        Integer[] value;
        TestRow(Long id, Integer[] value) {
          this.id = id;
          this.value = value;
        }

        @Override
        public boolean equals(Object other) {
          return other instanceof TestRow
            && Objects.equals(((TestRow) other).id, id)
            && Objects.deepEquals(((TestRow)other).value, value);
        }

        @Override
        public String toString() {
          return id + ", " + Arrays.toString(value);
        }
      }

      OracleR2dbcTypes.ArrayType arrayType =
        OracleR2dbcTypes.arrayType("TEST_IN_OUT_ARRAY");
      Statement callStatement = connection.createStatement(
        "BEGIN testInOutArrayProcedure(:id, :value); END;");

      // Expect a NULL out parameter the first time a row is inserted
      TestRow row = new TestRow(0L, new Integer[]{1, 2, 3});
      awaitQuery(
        List.of(Optional.empty()),
        outParameters -> {
          assertNull(outParameters.get("value"));
          assertNull(outParameters.get("value", int[].class));
          assertNull(outParameters.get("value", Integer[].class));
          return Optional.empty();
        },
        callStatement
          .bind("id", row.id)
          .bind("value", Parameters.inOut(arrayType, row.value)));

      // Update the row and expect an out parameter with the previous value
      TestRow row1 = new TestRow(row.id, new Integer[]{4, 5, 6});
      awaitQuery(
        List.of(row),
        outParameters ->
          new TestRow(
            row.id,
            outParameters.get("value", Integer[].class)),
        callStatement
          .bind("id", row.id)
          .bind("value", Parameters.inOut(arrayType, row1.value)));

      // Update the row again and expect an out parameter with the previous
      // value
      TestRow row2 = new TestRow(row.id, new Integer[]{7, 8, 9});
      awaitQuery(
        List.of(row1),
        outParameters ->
          new TestRow(
            row.id,
            outParameters.get("value", Integer[].class)),
        callStatement
          .bind("id", row.id)
          .bind("value", Parameters.inOut(arrayType, row2.value)));
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testInOutArrayCall"));
      tryAwaitExecution(connection.createStatement(
        "DROP PROCEDURE testInOutArrayProcedure"));
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE TEST_IN_OUT_ARRAY"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies behavior for a PL/SQL call having {@code OBJECT} type IN bind
   */
  @Test
  public void testInObjectCall() {
    Connection connection = awaitOne(sharedConnection());
    try {
      awaitExecution(connection.createStatement(
        "CREATE TYPE TEST_IN_OBJECT AS OBJECT(x NUMBER, y NUMBER, z NUMBER)"));
      awaitExecution(connection.createStatement(
        "CREATE TABLE testInObjectCall(id NUMBER, value TEST_IN_OBJECT)"));
      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE testInObjectProcedure (" +
          " id IN NUMBER," +
          " inObject IN TEST_IN_OBJECT)" +
          " IS" +
          " BEGIN" +
          " INSERT INTO testInObjectCall VALUES(id, inObject);" +
          " END;"));

      TestObjectRow row0 = new TestObjectRow(0L, new Integer[]{1, 2, 3});
      OracleR2dbcTypes.ObjectType objectType =
        OracleR2dbcTypes.objectType("TEST_IN_OBJECT");
      Statement callStatement = connection.createStatement(
        "BEGIN testInObjectProcedure(:id, :value); END;");
      awaitExecution(
        callStatement
          .bind("id", row0.id)
          .bind("value", Parameters.in(objectType, row0.value)));

      awaitQuery(
        List.of(row0),
        TestObjectRow::fromReadable,
        connection.createStatement(
          "SELECT id, value FROM testInObjectCall ORDER BY id"));

      TestObjectRow row1 = new TestObjectRow(1L, new Integer[]{4, 5, 6});
      awaitExecution(
        callStatement
          .bind("id", row1.id)
          .bind("value", constructObject(connection, objectType, row1.value)));

      awaitQuery(
        List.of(row0, row1),
        TestObjectRow::fromReadable,
        connection.createStatement(
          "SELECT id, value FROM testInObjectCall ORDER BY id"));
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testInObjectCall"));
      tryAwaitExecution(connection.createStatement(
        "DROP PROCEDURE testInObjectProcedure"));
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE TEST_IN_OBJECT"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies behavior for a PL/SQL call having {@code OBJECT} type OUT bind
   */
  @Test
  public void testOutObjectCall() {
    Connection connection = awaitOne(sharedConnection());
    try {
      awaitExecution(connection.createStatement(
        "CREATE TYPE TEST_OUT_OBJECT AS OBJECT(x NUMBER, y NUMBER, z NUMBER)"));
      awaitExecution(connection.createStatement(
        "CREATE TABLE testOutObjectCall(id NUMBER, value TEST_OUT_OBJECT)"));
      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE testOutObjectProcedure (" +
          "   inId IN NUMBER," +
          "   outObject OUT TEST_OUT_OBJECT)" +
          " IS" +
          " BEGIN" +
          "   SELECT value INTO outObject" +
          "     FROM testOutObjectCall" +
          "     WHERE id = inId;" +
          " EXCEPTION" +
          "   WHEN NO_DATA_FOUND THEN" +
          "     outObject := NULL;" +
          " END;"));


      OracleR2dbcTypes.ObjectType objectType =
        OracleR2dbcTypes.objectType("TEST_OUT_OBJECT");
      Statement callStatement = connection.createStatement(
        "BEGIN testOutObjectProcedure(:id, :value); END;");

      // Expect a NULL out parameter before any rows have been inserted
      awaitQuery(
        List.of(Optional.empty()),
        outParameters -> {
          assertNull(outParameters.get("value"));
          assertNull(outParameters.get("value", Object[].class));
          assertNull(outParameters.get("value", Map.class));
          assertNull(outParameters.get("value", OracleR2dbcObject.class));
          return Optional.empty();
        },
        callStatement
          .bind("id", -1)
          .bind("value", Parameters.out(objectType)));

      // Insert a row and expect an out parameter with the value
      TestObjectRow row0 = new TestObjectRow(0L, new Integer[]{1, 2, 3});
      awaitUpdate(1, connection.createStatement(
          "INSERT INTO testOutObjectCall VALUES (:id, :value)")
        .bind("id", row0.id)
        .bind("value", Parameters.in(objectType, row0.value)));
      awaitQuery(
        List.of(row0),
        outParameters ->
          new TestObjectRow(
            row0.id,
            outParameters.get("value", OracleR2dbcObject.class)),
        callStatement
          .bind("id", row0.id)
          .bind("value", Parameters.out(objectType)));

      // Insert another row and expect an out parameter with the value
      TestObjectRow row1 = new TestObjectRow(1L, new Integer[]{4, 5, 6});
      awaitUpdate(1, connection.createStatement(
          "INSERT INTO testOutObjectCall VALUES (:id, :value)")
        .bind("id", row1.id)
        .bind("value", constructObject(connection, objectType, row1.value)));
      awaitQuery(
        List.of(row1),
        outParameters ->
          new TestObjectRow(
            row1.id,
            outParameters.get("value", OracleR2dbcObject.class)),
        callStatement
          .bind("id", row1.id)
          .bind("value", Parameters.out(objectType)));
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testOutObjectCall"));
      tryAwaitExecution(connection.createStatement(
        "DROP PROCEDURE testOutObjectProcedure"));
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE TEST_OUT_OBJECT"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies behavior for a PL/SQL call having {@code OBJECT} type OUT bind
   */
  @Test
  public void testInOutObjectCall() {
    Connection connection = awaitOne(sharedConnection());
    try {
      awaitExecution(connection.createStatement(
        "CREATE TYPE TEST_IN_OUT_OBJECT AS OBJECT(x NUMBER, y NUMBER, z NUMBER)"));
      awaitExecution(connection.createStatement(
        "CREATE TABLE testInOutObjectCall(id NUMBER, value TEST_IN_OUT_OBJECT)"));
      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE PROCEDURE testInOutObjectProcedure (" +
          "   inId IN NUMBER," +
          "   inOutObject IN OUT TEST_IN_OUT_OBJECT)" +
          " IS" +
          " newValue TEST_IN_OUT_OBJECT;" +
          " BEGIN" +
          "" +
          /*
          " newValue := TEST_IN_OUT_OBJECT();" +
          " newValue.extend(inOutObject.count);" +
          " FOR i IN 1 .. inOutObject.count LOOP" +
          "   newValue(i) := inOutObject(i);" +
          " END LOOP;" +
          "" +
           */
          " newValue := inOutObject;" +
          " BEGIN" +
          "   SELECT value INTO inOutObject" +
          "     FROM testInOutObjectCall" +
          "     WHERE id = inId;" +
          "   DELETE FROM testInOutObjectCall WHERE id = inId;" +
          "   EXCEPTION" +
          "     WHEN NO_DATA_FOUND THEN" +
          "       inOutObject := NULL;" +
          " END;" +
          "" +
          " INSERT INTO testInOutObjectCall VALUES (inId, newValue);" +
          "" +
          " END;"));

      OracleR2dbcTypes.ObjectType objectType =
        OracleR2dbcTypes.objectType("TEST_IN_OUT_OBJECT");
      Statement callStatement = connection.createStatement(
        "BEGIN testInOutObjectProcedure(:id, :value); END;");

      // Expect a NULL out parameter the first time a row is inserted
      TestObjectRow row = new TestObjectRow(0L, new Integer[]{1, 2, 3});
      awaitQuery(
        List.of(Optional.empty()),
        outParameters -> {
          assertNull(outParameters.get("value"));
          assertNull(outParameters.get("value", Object[].class));
          assertNull(outParameters.get("value", Map.class));
          assertNull(outParameters.get("value", OracleR2dbcObject.class));
          return Optional.empty();
        },
        callStatement
          .bind("id", row.id)
          .bind("value", Parameters.inOut(objectType, row.value)));

      // Update the row and expect an out parameter with the previous value
      TestObjectRow row1 = new TestObjectRow(row.id, new Integer[]{4, 5, 6});
      awaitQuery(
        List.of(row),
        outParameters ->
          new TestObjectRow(
            row.id,
            outParameters.get("value", OracleR2dbcObject.class)),
        callStatement
          .bind("id", row.id)
          .bind("value", Parameters.inOut(objectType, constructObject(
            connection, objectType, row1.value))));

      // Update the row again and expect an out parameter with the previous
      // value
      TestObjectRow row2 = new TestObjectRow(row.id, new Integer[]{7, 8, 9});
      awaitQuery(
        List.of(row1),
        outParameters ->
          new TestObjectRow(
            row.id,
            outParameters.get("value", OracleR2dbcObject.class)),
        callStatement
          .bind("id", row.id)
          .bind("value", Parameters.inOut(objectType, Map.of(
            "x", row2.value[0],
            "y", row2.value[1],
            "z", row2.value[2]))));
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testInOutObjectCall"));
      tryAwaitExecution(connection.createStatement(
        "DROP PROCEDURE testInOutObjectProcedure"));
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE TEST_IN_OUT_OBJECT"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Connect to the database configured by {@link DatabaseConfig}, with a
   * the connection configured to use a given {@code executor} for async
   * callbacks.
   * @param executor Executor for async callbacks
   * @return Connection that uses the {@code executor}
   */
  private static Publisher<? extends Connection> connect(Executor executor) {
    return ConnectionFactories.get(connectionFactoryOptions()
      .mutate()
      .option(OracleR2dbcOptions.EXECUTOR, executor)
      .build())
      .create();
  }

  /**
   * Verifies concurrent statement execution the given {@code connection}
   * @param connection Connection to verify
   */
  private void verifyConcurrentExecute(Connection connection) {

    // Create many statements and execute them in parallel.
    @SuppressWarnings({"unchecked","rawtypes"})
    Publisher<Integer>[] publishers =
      new Publisher[Runtime.getRuntime().availableProcessors() * 4];

    for (int i = 0; i < publishers.length; i++) {
      Flux<Integer> flux = Flux.from(connection.createStatement(
            "SELECT " + i + " FROM sys.dual")
          .execute())
        .flatMap(result ->
          result.map(row -> row.get(0, Integer.class)))
        .cache();

      flux.subscribe();
      publishers[i] = flux;
    }

    awaitMany(
      IntStream.range(0, publishers.length)
        .boxed()
        .collect(Collectors.toList()),
      Flux.concat(publishers));
  }

  /**
   * Verifies concurrent row fetching with the given {@code connection}
   * @param connection Connection to verify
   */
  private void verifyConcurrentFetch(Connection connection) {
    try {
      awaitExecution(connection.createStatement(
        "CREATE TABLE testConcurrentFetch (value NUMBER)"));

      // Create many statements and execute them in parallel.
      @SuppressWarnings({"unchecked","rawtypes"})
      Publisher<Long>[] publishers =
        new Publisher[Runtime.getRuntime().availableProcessors() * 4];

      for (int i = 0; i < publishers.length; i++) {

        Statement statement = connection.createStatement(
          "INSERT INTO testConcurrentFetch VALUES (?)");

        // Each publisher batch inserts a range of 10 values
        int start = i * 10;
        statement.bind(0, start);
        IntStream.range(start + 1, start + 10)
          .forEach(value -> {
            statement.add().bind(0, value);
          });

        Mono<Long> mono = Flux.from(statement.execute())
          .flatMap(Result::getRowsUpdated)
          .collect(Collectors.summingLong(Long::longValue))
          .cache();

        // Execute in parallel, and retain the result for verification later
        mono.subscribe();
        publishers[i] = mono;
      }

      // Expect each publisher to emit an update count of 100
      awaitMany(
        Stream.generate(() -> 10L)
          .limit(publishers.length)
          .collect(Collectors.toList()),
        Flux.merge(publishers));

      // Create publishers that fetch rows in parallel
      @SuppressWarnings({"unchecked","rawtypes"})
      Publisher<List<Integer>>[] fetchPublishers =
        new Publisher[publishers.length];

      for (int i = 0; i < fetchPublishers.length; i++) {
        Mono<List<Integer>> mono = Flux.from(connection.createStatement(
              "SELECT value FROM testConcurrentFetch ORDER BY value")
            .execute())
          .flatMap(result ->
            result.map(row -> row.get(0, Integer.class)))
          .sort()
          .collect(Collectors.toList())
          .cache();

        // Execute in parallel, and retain the result for verification later
        mono.subscribe();
        fetchPublishers[i] = mono;
      }

      // Expect each fetch publisher to get the same result
      List<Integer> expected = IntStream.range(0, publishers.length * 10)
        .boxed()
        .collect(Collectors.toList());

      for (Publisher<List<Integer>> publisher : fetchPublishers)
        awaitOne(expected, publisher);
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testConcurrentFetch"));
    }
  }

  /**
   * Creates list of a specified length of test table rows
   */
  private static List<TestRow> createRows(int length) {
    return IntStream.range(0, 100)
      .mapToObj(id -> new TestRow(id, String.valueOf(id)))
      .collect(Collectors.toList());
  }

  /** Binds a list of rows to a batch statement */
  private Statement bindRows(List<TestRow> rows, Statement statement) {
    rows.stream()
      .limit(rows.size() - 1)
      .forEach(row ->
        statement
          .bind("id", row.id)
          .bind("value", row.value)
          .add());

    statement
      .bind("id", rows.get(rows.size() - 1).id)
      .bind("value", rows.get(rows.size() - 1).value);

    return statement;
  }

  /**
   * A row of a test table.
   */
  private static class TestRow {
    final int id;
    final String value;

    TestRow(int id, String value) {
      this.id = id;
      this.value = value;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof TestRow
        && id == ((TestRow)other).id
        && Objects.equals(value, ((TestRow)other).value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, value);
    }

    @Override
    public String toString() {
      return "[id=" + id + ", value=" + value + "]";
    }
  }

  private static class TestObjectRow {
    Long id;
    Object[] value;
    TestObjectRow(Long id, OracleR2dbcObject object) {
      this(id, new Integer[] {
        object.get("x", Integer.class),
        object.get("y", Integer.class),
        object.get("z", Integer.class)
      });
    }

    TestObjectRow(Long id, Object[] value) {
      this.id = id;
      this.value = value;
    }

    static TestObjectRow fromReadable(io.r2dbc.spi.Readable row) {
      return new TestObjectRow(
        row.get("id", Long.class),
        row.get("value", OracleR2dbcObject.class));
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof TestObjectRow
        && Objects.equals(((TestObjectRow) other).id, id)
        && Arrays.equals(((TestObjectRow)other).value, value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, value);
    }

    @Override
    public String toString() {
      return id + ", " + Arrays.toString(value);
    }
  }
}
