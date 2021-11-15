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
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.RowMetadata;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.IntFunction;

import static java.util.Arrays.asList;
import static oracle.r2dbc.test.DatabaseConfig.connectTimeout;
import static oracle.r2dbc.test.DatabaseConfig.sharedConnection;
import static oracle.r2dbc.util.Awaits.awaitError;
import static oracle.r2dbc.util.Awaits.awaitExecution;
import static oracle.r2dbc.util.Awaits.awaitNone;
import static oracle.r2dbc.util.Awaits.awaitOne;
import static oracle.r2dbc.util.Awaits.awaitUpdate;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that
 * {@link ReadablesMetadata} implements behavior that is specified in it's
 * class and method level javadocs.
 */
public class OracleRowMetadataImplTest {

  /**
   * Verifies the implementation of
   * {@link ReadablesMetadata#getColumnMetadata(int)}
   */
  @Test
  public void testGetColumnMetadataByIndex() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // INSERT and SELECT rows from this table.  Use the precision of the
      // columns to distinguish their metadata; Some tests will use duplicate
      // column names, so the name is not distinct.
      awaitExecution(connection.createStatement(
        "CREATE TABLE testGetColumnMetadataByIndex" +
          " (x NUMBER(1), y NUMBER(2))"));
      try {
        int xPrecision = 1;
        int yPrecision = 2;
        awaitUpdate(1, connection.createStatement(
          "INSERT INTO testGetColumnMetadataByIndex (x,y) VALUES (0,0)"));

        // Expect IllegalArgumentException for an index less than 0
        awaitError(IndexOutOfBoundsException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetColumnMetadataByIndex")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) ->
                metadata.getColumnMetadata(-1).getPrecision())));

        // Expect IllegalArgumentException for an index greater than or equal
        // to the number of columns
        awaitError(IndexOutOfBoundsException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetColumnMetadataByIndex")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) ->
                metadata.getColumnMetadata(2).getPrecision())));

        // Expect valid indexes to return the column metadata
        awaitOne(asList(xPrecision, yPrecision),
          Flux.from(connection.createStatement(
            "SELECT x,y FROM testGetColumnMetadataByIndex")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  metadata.getColumnMetadata(0).getPrecision(),
                  metadata.getColumnMetadata(1).getPrecision()))
            ));
        awaitOne(asList(yPrecision, xPrecision),
          Flux.from(connection.createStatement(
            "SELECT y,x FROM testGetColumnMetadataByIndex")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  metadata.getColumnMetadata(0).getPrecision(),
                  metadata.getColumnMetadata(1).getPrecision()))
            ));
        awaitOne(asList(xPrecision, yPrecision),
          Flux.from(connection.createStatement(
            "SELECT * FROM testGetColumnMetadataByIndex")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  metadata.getColumnMetadata(0).getPrecision(),
                  metadata.getColumnMetadata(1).getPrecision()))
            ));
      }
      finally {
        awaitExecution(connection.createStatement(
          "DROP TABLE testGetColumnMetadataByIndex"));
      }
    }
    finally {
      awaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link ReadablesMetadata#getColumnMetadata(String)}
   */
  @Test
  public void testGetColumnMetadataByName() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // INSERT and SELECT rows from this table. Use the precision of the
      // columns to distinguish their metadata; Some tests will use duplicate
      // column names, so the name is not distinct.
      int xPrecision = 1;
      int yPrecision = 2;
      awaitExecution(connection.createStatement(
        "CREATE TABLE testGetColumnMetadataByName (x NUMBER(1), y NUMBER(2))"));
      try {
        awaitUpdate(1, connection.createStatement(
          "INSERT INTO testGetColumnMetadataByName (x,y) VALUES (0,0)"));

        // Expect IllegalArgumentException for a null name
        awaitError(IllegalArgumentException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetColumnMetadataByName")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> metadata.getColumnMetadata(null))
            ));

        // Expect NoSuchElementException for unmatched names
        awaitError(NoSuchElementException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetColumnMetadataByName")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> metadata.getColumnMetadata("z"))
            ));
        awaitError(NoSuchElementException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetColumnMetadataByName")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> metadata.getColumnMetadata("xx"))
            ));
        awaitError(NoSuchElementException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetColumnMetadataByName")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> metadata.getColumnMetadata("x "))
            ));
        awaitError(NoSuchElementException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetColumnMetadataByName")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> metadata.getColumnMetadata(" x"))
            ));
        awaitError(NoSuchElementException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetColumnMetadataByName")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> metadata.getColumnMetadata(" "))
            ));
        awaitError(NoSuchElementException.class,
          Flux.from(connection.createStatement(
            "SELECT x, y FROM testGetColumnMetadataByName")
            .execute())
            .concatMap(result ->
              result.map((row, metadata) -> metadata.getColumnMetadata(""))
            ));

        // Expect valid names to return the column metadata
        awaitOne(asList(xPrecision, yPrecision),
          Flux.from(connection.createStatement(
            "SELECT x,y FROM testGetColumnMetadataByName")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  metadata.getColumnMetadata("x").getPrecision(),
                  metadata.getColumnMetadata("y").getPrecision()))
            ));
        awaitOne(asList(yPrecision, yPrecision),
          Flux.from(connection.createStatement(
            "SELECT x,y FROM testGetColumnMetadataByName")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  metadata.getColumnMetadata("y").getPrecision(),
                  metadata.getColumnMetadata("y").getPrecision()))
            ));
        awaitOne(asList(yPrecision, xPrecision),
          Flux.from(connection.createStatement(
            "SELECT y,x FROM testGetColumnMetadataByName")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  metadata.getColumnMetadata("y").getPrecision(),
                  metadata.getColumnMetadata("x").getPrecision()))
            ));
        awaitOne(asList(xPrecision, yPrecision),
          Flux.from(connection.createStatement(
            "SELECT * FROM testGetColumnMetadataByName")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  metadata.getColumnMetadata("x").getPrecision(),
                  metadata.getColumnMetadata("y").getPrecision()))
            ));

        // Expect case-insensitive column name matching
        awaitOne(asList(xPrecision, yPrecision, "PRODUCT"),
          Flux.from(connection.createStatement(
            "SELECT x, y, (x * y) AS product FROM testGetColumnMetadataByName")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  metadata.getColumnMetadata("X").getPrecision(),
                  metadata.getColumnMetadata("Y").getPrecision(),
                  metadata.getColumnMetadata("pRoDuCt").getName()))
            ));

        // Expect aliased column name matching
        awaitOne(asList(xPrecision, yPrecision),
          Flux.from(connection.createStatement(
            "SELECT x AS width, y AS height FROM testGetColumnMetadataByName")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  metadata.getColumnMetadata("width").getPrecision(),
                  metadata.getColumnMetadata("height").getPrecision()))
            ));
        awaitOne(asList(xPrecision, yPrecision, "AREA"),
          Flux.from(connection.createStatement(
            "SELECT " +
              "x AS width, " +
              "y AS height, " +
              "(x * y) AS area " +
              "FROM testGetColumnMetadataByName")
            .execute())
            .flatMap(result ->
              result.map((row, metadata) ->
                asList(
                  metadata.getColumnMetadata("width").getPrecision(),
                  metadata.getColumnMetadata("height").getPrecision(),
                  metadata.getColumnMetadata("area").getName()))
            ));

        // Expect value from lowest column index for a duplicate column name
        awaitOne(yPrecision,
          Flux.from(connection.createStatement(
            "SELECT y AS x, x FROM testGetColumnMetadataByName")
            .execute())
            .flatMap(result -> result.map((row, metadata) ->
              metadata.getColumnMetadata("x").getPrecision())));

        // Create a table for a duplicate column name with a join
        awaitExecution(connection.createStatement(
          "CREATE TABLE testGetColumnMetadataByName2" +
            " (x NUMBER(3), y NUMBER(4))"));
        try {
          awaitUpdate(1, connection.createStatement(
            "INSERT INTO testGetColumnMetadataByName2 (x, y) VALUES (1,1)"));
          awaitOne(asList(3, 4),
            Flux.from(connection.createStatement(
              "SELECT l.x, l.y, r.x, r.y"
                + " FROM testGetColumnMetadataByName2 l,"
                + " testGetColumnMetadataByName r"
                + " WHERE l.x = 1 AND r.x = 0")
              .execute())
              .flatMap(result -> result.map((row, metadata) ->
                asList(
                  metadata.getColumnMetadata("x").getPrecision(),
                  metadata.getColumnMetadata("y").getPrecision()))
              ));
        }
        finally {
          awaitExecution(connection.createStatement(
            "DROP TABLE testGetColumnMetadataByName2"));
        }

      }
      finally {
        awaitExecution(connection.createStatement(
          "DROP TABLE testGetColumnMetadataByName"));
      }
    }
    finally {
      awaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link ReadablesMetadata#getColumnMetadatas()}
   */
  @Test
  public void testGetColumnMetadatas() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
        // INSERT and SELECT rows from this table. Use the precision of the
        // columns to distinguish their metadata; Some tests will use duplicate
        // column names, so the name is not distinct.
        int xPrecision = 1;
        int yPrecision = 2;
        awaitExecution(connection.createStatement(
          "CREATE TABLE testGetColumnMetadatas (x NUMBER(1), y NUMBER(2))"));
        try {
          awaitUpdate(1, connection.createStatement(
            "INSERT INTO testGetColumnMetadatas (x,y) VALUES (0,0)"));

          Iterable<? extends ColumnMetadata> metadatas =
            awaitOne(Flux.from(connection.createStatement(
              "SELECT y, x, y AS x FROM testGetColumnMetadatas")
              .execute())
              .flatMap(result ->
                result.map((row, metadata) -> metadata))
              .map(RowMetadata::getColumnMetadatas));

          // Expect forEach to consume each column's metadata, in order of the
          // SELECT statement's column list
          List<ColumnMetadata> metadataList = new ArrayList<>(3);
          metadatas.forEach(metadataList::add);

          assertEquals("Y", metadataList.get(0).getName());
          assertEquals(yPrecision, metadataList.get(0).getPrecision());
          assertEquals("X", metadataList.get(1).getName());
          assertEquals(xPrecision, metadataList.get(1).getPrecision());
          assertEquals("X", metadataList.get(2).getName());
          assertEquals(yPrecision, metadataList.get(2).getPrecision());

          // Expect for-each loops to iterate over each column's metadata, in
          // order of the SELECT statement's column list. Expect the same
          // ColumnMetadata object instances that were added by the forEach
          // method.
          for (ColumnMetadata metadata : metadatas)
            assertEquals(metadataList.remove(0), metadata);

          // Expect for-each the loop to have iterated once for each column.
          assertTrue(metadataList.isEmpty());

          Iterator<? extends ColumnMetadata> metadataIterator =
            metadatas.iterator();

          // Expect the Iterator to not support remove()
          assertThrows(
            UnsupportedOperationException.class, metadataIterator::remove);

          // Expect forEachRemaining to consume each column's metadata, in
          // order of the SELECT statement's column list
          metadataIterator.forEachRemaining(metadataList::add);

          assertEquals("Y", metadataList.get(0).getName());
          assertEquals(yPrecision, metadataList.get(0).getPrecision());
          assertEquals("X", metadataList.get(1).getName());
          assertEquals(xPrecision, metadataList.get(1).getPrecision());
          assertEquals("X", metadataList.get(2).getName());
          assertEquals(yPrecision, metadataList.get(2).getPrecision());

          // Expect forEachRemaining to reach the iterator's terminal state
          assertFalse(metadataIterator.hasNext());
          assertThrows(NoSuchElementException.class, metadataIterator::next);

          // Expect the iterator to iterate over each column's metadata, in
          // order of the SELECT statement's column list. Expect the same
          // ColumnMetadata object instances that were added by the
          // forEachRemaining  method.
          metadataIterator = metadatas.iterator();
          while (metadataIterator.hasNext())
            assertEquals(metadataList.remove(0), metadataIterator.next());

          // TODO: Verify spliterator()
        }
        finally {
          awaitExecution(connection.createStatement(
            "DROP TABLE testGetColumnMetadatas"));
        }
    }
    finally {
      awaitNone(connection.close());
    }
  }

}
