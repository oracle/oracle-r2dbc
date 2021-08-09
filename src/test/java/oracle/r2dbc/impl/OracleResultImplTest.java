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
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Result.Message;
import io.r2dbc.spi.Result.UpdateCount;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static oracle.r2dbc.test.DatabaseConfig.connectTimeout;
import static oracle.r2dbc.test.DatabaseConfig.sharedConnection;
import static oracle.r2dbc.test.DatabaseConfig.sqlTimeout;
import static oracle.r2dbc.util.Awaits.awaitError;
import static oracle.r2dbc.util.Awaits.awaitExecution;
import static oracle.r2dbc.util.Awaits.awaitMany;
import static oracle.r2dbc.util.Awaits.awaitNone;
import static oracle.r2dbc.util.Awaits.awaitOne;
import static oracle.r2dbc.util.Awaits.consumeOne;
import static oracle.r2dbc.util.Awaits.tryAwaitExecution;
import static oracle.r2dbc.util.Awaits.tryAwaitNone;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that
 * {@link OracleResultImpl} implements behavior that is specified in it's
 * class and method level javadocs.
 */
public class OracleResultImplTest {

  /**
   * Verifies the implementation of
   * {@link OracleResultImpl#getRowsUpdated()}
   */
  @Test
  public void testGetRowsUpdated() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Verify update counts from INSERT, UPDATE, and DELETE statements made
      // on this table
      awaitExecution(connection.createStatement(
        "CREATE TABLE testGetRowsUpdated (x NUMBER, y NUMBER)"));

      // Expect update count of 1 from each INSERT.
      Iterator<? extends Result> insertResults =
        Flux.from(connection.createBatch()
          .add("INSERT INTO testGetRowsUpdated (x, y) VALUES (0, 0)")
          .add("INSERT INTO testGetRowsUpdated (x, y) VALUES (0, 1)")
          .execute())
          .toIterable()
          .iterator();
      Result insertResult0 = insertResults.next();
      Publisher<Integer> insertCountPublisher0 =
        insertResult0.getRowsUpdated();
      awaitOne(1, insertCountPublisher0);

      // Expect IllegalStateException from multiple Result consumptions.
      assertThrows(IllegalStateException.class,
        insertResult0::getRowsUpdated);
      assertThrows(IllegalStateException.class,
        () -> insertResult0.map((row, metadata) -> "unexpected"));

      // Expect update count publisher to support multiple subscribers
      awaitOne(1, insertCountPublisher0);

      Result insertResult1 = insertResults.next();
      Publisher<Integer> insertCountPublisher1 =
        insertResult1.getRowsUpdated();
      awaitOne(1, insertCountPublisher1);

      // Expect IllegalStateException from multiple Result consumptions.
      assertThrows(IllegalStateException.class,
        insertResult1::getRowsUpdated);
      assertThrows(IllegalStateException.class,
        () -> insertResult1.map((row, metadata) -> "unexpected"));

      // Expect update count publisher to support multiple subscribers
      awaitOne(1, insertCountPublisher1);

      // Expect an update count of zero from UPDATE of zero rows
      consumeOne(connection.createStatement(
        "UPDATE testGetRowsUpdated SET y = 99 WHERE x = 99")
        .execute(),
        noUpdateResult -> {
          Publisher<Integer> noUpdateCountPublisher =
            noUpdateResult.getRowsUpdated();
          awaitOne(0, noUpdateCountPublisher);

          // Expect IllegalStateException from multiple Result consumptions.
          assertThrows(IllegalStateException.class,
            () -> noUpdateResult.map((row, metadata) -> "unexpected"));
          assertThrows(IllegalStateException.class, noUpdateResult::getRowsUpdated);

          // Expect update count publisher to support multiple subscribers
          awaitOne(0, noUpdateCountPublisher);
        });

      // Expect update count of 2 from UPDATE of 2 rows
      consumeOne(connection.createStatement(
        "UPDATE testGetRowsUpdated SET y = 2 WHERE x = 0")
        .execute(),
        updateResult -> {
        Publisher<Integer> updateCountPublisher = updateResult.getRowsUpdated();
        awaitOne(2, updateCountPublisher);

        // Expect IllegalStateException from multiple Result consumptions.
        assertThrows(IllegalStateException.class,
          () -> updateResult.map((row, metadata) -> "unexpected"));
        assertThrows(IllegalStateException.class, updateResult::getRowsUpdated);

        // Expect update count publisher to support multiple subscribers
        awaitOne(2, updateCountPublisher);
      });

      // Expect no update count from SELECT
      awaitNone(Mono.from(connection.createStatement(
        "SELECT x,y FROM testGetRowsUpdated")
        .execute())
        .flatMapMany(selectResult -> {
          Publisher<Integer> selectCountPublisher =
            selectResult.getRowsUpdated();

          // Expect update count publisher to support multiple subscribers
          Publisher<Integer> result = Flux.concat(
            Mono.from(selectCountPublisher).cache(),
            Mono.from(selectCountPublisher).cache());

          // Expect IllegalStateException from multiple Result consumptions.
          assertThrows(IllegalStateException.class,
            () -> selectResult.map((row, metadata) -> "unexpected"));
          assertThrows(IllegalStateException.class, selectResult::getRowsUpdated);

          return result;
        }));

      // Expect update count of 2 from DELETE of 2 rows
      consumeOne(connection.createStatement(
        "DELETE FROM testGetRowsUpdated WHERE x = :x")
        .bind("x", 0)
        .execute(),
        deleteResult -> {
          Publisher<Integer> deleteCountPublisher = deleteResult.getRowsUpdated();
          awaitOne(2, deleteCountPublisher);

          // Expect IllegalStateException from multiple Result consumptions.
          assertThrows(IllegalStateException.class,
            () -> deleteResult.map((row, metadata) -> "unexpected"));
          assertThrows(IllegalStateException.class, deleteResult::getRowsUpdated);

          // Expect update count publisher to support multiple subscribers
          awaitOne(2, deleteCountPublisher);
        });
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testGetRowsUpdated"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleResultImpl#map(BiFunction)}
   */
  @Test
  public void testMap() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Verify row data from SELECT statements made on this table
      awaitExecution(connection.createStatement(
        "CREATE TABLE testMap (x NUMBER, y NUMBER)"));
      // Expect no row data from each INSERT.
      Iterator<? extends Result> insertResults =
        Flux.from(connection.createBatch()
          .add("INSERT INTO testMap (x, y) VALUES (0, 0)")
          .add("INSERT INTO testMap (x, y) VALUES (0, 1)")
          .execute())
          .toIterable()
          .iterator();
      Result insertResult0 = insertResults.next();
      Publisher<Object> insertRowPublisher0 =
        insertResult0.map((row, metadata) -> row.get(0));
      awaitNone(insertRowPublisher0);

      // Expect IllegalStateException from multiple Result consumptions.
      assertThrows(IllegalStateException.class,
        insertResult0::getRowsUpdated);
      assertThrows(IllegalStateException.class,
        () -> insertResult0.map((row, metadata) -> "unexpected"));

      // Expect row data publisher to reject multiple subscribers
      // TODO: Is it necessary to verify this for an empty publisher?
      // awaitError(IllegalStateException.class, insertRowPublisher0);

      Result insertResult1 = insertResults.next();
      Publisher<Object> insertRowPublisher1 =
        insertResult1.map((row, metadata) -> row.get(0));
      awaitNone(insertRowPublisher1);

      // Expect IllegalStateException from multiple Result consumptions.
      assertThrows(IllegalStateException.class,
        insertResult1::getRowsUpdated);
      assertThrows(IllegalStateException.class,
        () -> insertResult1.map((row, metadata) -> "unexpected"));

      // Expect row data publisher to reject multiple subscribers
      // TODO: Is it necessary to verify this for an empty publisher?
      //awaitError(IllegalStateException.class, insertRowPublisher1);

      // Expect no rows from UPDATE
      consumeOne(connection.createStatement(
        "UPDATE testMap SET y=y+:delta WHERE x=0")
        .bind("delta", 1)
        .execute(),
        updateResult -> {
          Publisher<Object> updateRowPublisher =
            updateResult.map((row, metadata) -> row.get(0));
          awaitNone(updateRowPublisher);

          // Expect IllegalStateException from multiple Result consumptions.
          assertThrows(IllegalStateException.class,
            () -> updateResult.map((row, metadata) -> "unexpected"));
          assertThrows(IllegalStateException.class, updateResult::getRowsUpdated);

          // Expect row data publisher to reject multiple subscribers
          // TODO: Is it necessary to verify this for an empty publisher?
          // awaitError(IllegalStateException.class, updateRowPublisher);
        });

      // Expect no rows from SELECT of zero rows
      consumeOne(connection.createStatement(
        "SELECT x, y FROM testMap WHERE x = 99")
        .execute(),
        noRowsResult -> {
          Publisher<Object> noRowsPublisher =
            noRowsResult.map((row, metadata) -> row.get(0));
          awaitNone(noRowsPublisher);

          // Expect IllegalStateException from multiple Result consumptions.
          assertThrows(IllegalStateException.class,
            () -> noRowsResult.map((row, metadata) -> "unexpected"));
          assertThrows(IllegalStateException.class, noRowsResult::getRowsUpdated);

          // Expect row data publisher to reject multiple subscribers
          awaitError(IllegalStateException.class, noRowsPublisher);
        });

      // Expect 2 rows from SELECT of 2 rows
      awaitMany(asList(asList(0, 1), asList(0, 2)),
        Mono.from(connection.createStatement(
          "SELECT x,y FROM testMap WHERE x = 0 ORDER BY y")
          .execute())
          .flatMapMany(selectResult -> {
            // Expect IllegalArgumentException for a null mapping function
            assertThrows(IllegalArgumentException.class,
              () -> selectResult.map((BiFunction<Row, RowMetadata, ?>)null));

            Publisher<List<Integer>> selectRowPublisher =
              selectResult.map((row, metadata) ->
                asList(
                  row.get("x", Integer.class),
                  row.get("y", Integer.class)));

            // Expect IllegalStateException from multiple Result consumptions.
            assertThrows(IllegalStateException.class,
              () -> selectResult.map((row, metadata) -> "unexpected"));
            assertThrows(IllegalStateException.class, selectResult::getRowsUpdated);

            return Flux.from(selectRowPublisher)
              .doFinally(signalType ->
                // Expect row data publisher to reject multiple subscribers
                awaitError(IllegalStateException.class, selectRowPublisher));
          }));

      // Expect a Row to not be valid outside of the mapping function
      List<Row> rows = awaitMany(Flux.from(connection.createStatement(
        "SELECT x, y FROM testMap")
        .execute())
        .flatMap(result -> result.map((row, metatdata) -> row)));
      Row row0 = rows.get(0);
      assertThrows(IllegalStateException.class, () -> row0.get("x"));
      assertThrows(IllegalStateException.class, () -> row0.get(1));
      Row row1 = rows.get(1);
      assertThrows(IllegalStateException.class, () -> row1.get(0));
      assertThrows(IllegalStateException.class, () -> row1.get("y"));;

      // Expect onError for a mapping function that throws
      RuntimeException thrown = new RuntimeException("Expected");
      awaitMany(asList(Signal.next(asList(0, 1)), Signal.error(thrown)),
        Mono.from(connection.createStatement(
        "SELECT x,y FROM testMap WHERE x = 0 ORDER BY y")
        .execute())
        .flatMapMany(select3Result ->
          Flux.from(select3Result.map((row, metadata) -> {
              if (row.get("y", Integer.class) == 1) {
                return asList(
                  row.get("x", Integer.class), row.get("y", Integer.class));
              }
              else {
                throw thrown;
              }
            }))
            .materialize()));

      // Expect onError for a mapping function that outputs null
      List<Signal<List<Integer>>> signals = awaitMany(Mono.from(
        connection.createStatement(
          "SELECT x,y FROM testMap WHERE x = 0 ORDER BY y")
          .execute())
          .flatMapMany(select4Result ->
              Flux.from(select4Result.map((row, metadata) -> {
                if (row.get("y", Integer.class) == 1) {
                  return asList(
                    row.get("x", Integer.class), row.get("y", Integer.class));
                }
                else {
                  return null;
                }
              })).materialize()));
      assertEquals(signals.get(0).get(), asList(0, 1));
      assertEquals(
        signals.get(1).getThrowable().getClass(),
        NullPointerException.class);

      // Expect no rows from DELETE
      awaitNone(Mono.from(connection.createStatement(
        "DELETE FROM testMap WHERE x <>:y")
        .bind("y", 99)
        .execute())
        .flatMap(deleteResult -> {

          Publisher<Object> deleteRowPublisher =
            deleteResult.map((row, metatdata) -> row.get("z"));

          // Expect IllegalStateException from multiple Result consumptions.
          assertThrows(IllegalStateException.class,
            () -> deleteResult.map((row, metadata) -> "unexpected"));
          assertThrows(IllegalStateException.class, deleteResult::getRowsUpdated);

          return Mono.from(deleteRowPublisher);
            // TODO: Is it necessary to verify this for an empty publisher?
          /*
            .doOnTerminate(() ->
              // Expect row data publisher to reject multiple subscribers
              awaitError(IllegalStateException.class, deleteRowPublisher));

           */
        }));
    }
    finally {
      tryAwaitExecution(connection.createStatement("DROP TABLE testMap"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies {@link Result#flatMap(Function)} for a batch DML statement that
   * updates some rows and then fails. Expect the {@code Result} to emit
   * counts for updates that succeeded, and then emit an {@link Message}
   * segment with the failure
   */
  @Test
  public void testBatchUpdateError() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Batch insert two rows with the same ID number. Expect the result to
      // emit an update count of 1, and then emit a primary key violation
      // message
      awaitExecution(connection.createStatement(
        "CREATE TABLE testBatchUpdateError (id NUMBER PRIMARY KEY)"));
      AtomicInteger segmentIndex = new AtomicInteger(0);
      awaitNone(Mono.from(connection.createStatement(
        "INSERT INTO testBatchUpdateError VALUES (?)")
        .bind(0, 0).add()
        .bind(0, 0).add()
        .execute())
        .flatMapMany(result ->
          result.flatMap(segment -> {
            int current = segmentIndex.getAndIncrement();
            if (current == 0) {
              assertTrue(segment instanceof UpdateCount,
                "Unexpected Segment: " + segment);
              assertEquals(1, ((UpdateCount)segment).value());
            }
            else if (current == 1) {
              assertTrue(segment instanceof Message,
                "Unexpected Segment: " + segment);
              // Expect ORA-00001 for primary key constraint violation
              assertEquals(1, ((Message)segment).errorCode());
            }
            else {
              fail("Unexpected Segment: " + segment + " count: " + current);
            }
            return Mono.empty();
          })));
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testBatchUpdateError"));
      tryAwaitNone(connection.close());
    }
  }
}
