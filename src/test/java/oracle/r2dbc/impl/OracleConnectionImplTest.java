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

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.r2dbc.spi.TransactionDefinition.*;
import static java.util.Collections.emptyMap;
import static oracle.r2dbc.test.DatabaseConfig.sharedConnection;
import static oracle.r2dbc.test.DatabaseConfig.connectTimeout;
import static oracle.r2dbc.test.DatabaseConfig.newConnection;
import static oracle.r2dbc.test.DatabaseConfig.sqlTimeout;
import static oracle.r2dbc.test.DatabaseConfig.user;
import static oracle.r2dbc.util.Awaits.awaitError;
import static oracle.r2dbc.util.Awaits.awaitExecution;
import static oracle.r2dbc.util.Awaits.awaitMany;
import static oracle.r2dbc.util.Awaits.awaitNone;
import static oracle.r2dbc.util.Awaits.awaitOne;
import static oracle.r2dbc.util.Awaits.awaitQuery;
import static oracle.r2dbc.util.Awaits.awaitUpdate;
import static oracle.r2dbc.util.Awaits.tryAwaitExecution;
import static oracle.r2dbc.util.Awaits.tryAwaitNone;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Verifies that
 * {@link OracleConnectionImpl} implements behavior that is specified in it's
 * class and method level javadocs.
 */
public class OracleConnectionImplTest {

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#beginTransaction()}
   */
  @Test
  public void testBeginTransaction() {
    Connection sessionA =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      verifyReadCommittedIsolation(sessionA, sessionA.beginTransaction());
    }
    finally {
      awaitNone(sessionA.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#beginTransaction(TransactionDefinition)}
   */
  @Test
  public void testBeginTransactionDefined() {
    Connection sessionA =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Expect IllegalArgumentException to be thrown for unsupported
      // descriptions
      assertThrows(IllegalArgumentException.class,
        () -> sessionA.beginTransaction(null));
      assertThrows(IllegalArgumentException.class,
        () -> sessionA.beginTransaction(transactionDefinition(emptyMap())));
      assertThrows(IllegalArgumentException.class,
        () -> sessionA.beginTransaction(IsolationLevel.SERIALIZABLE));
      assertThrows(IllegalArgumentException.class,
        () -> sessionA.beginTransaction(IsolationLevel.READ_UNCOMMITTED));
      assertThrows(IllegalArgumentException.class,
        () -> sessionA.beginTransaction(IsolationLevel.REPEATABLE_READ));
      assertThrows(IllegalArgumentException.class,
        () -> sessionA.beginTransaction(transactionDefinition(Map.of(
          ISOLATION_LEVEL, IsolationLevel.READ_COMMITTED,
          READ_ONLY, true))));
      assertThrows(IllegalArgumentException.class,
        () -> sessionA.beginTransaction(transactionDefinition(Map.of(
          ISOLATION_LEVEL, IsolationLevel.READ_COMMITTED,
          READ_ONLY, false))));
      assertThrows(IllegalArgumentException.class,
        () -> sessionA.beginTransaction(transactionDefinition(Map.of(
          LOCK_WAIT_TIMEOUT, Duration.ofSeconds(10)))));

      verifyReadCommittedIsolation(sessionA,
        sessionA.beginTransaction(IsolationLevel.READ_COMMITTED));
    }
    finally {
      awaitNone(sessionA.close());
    }
  }

  /**
   * Verifies
   * {@link OracleConnectionImpl#beginTransaction(TransactionDefinition)} with
   * {@link TransactionDefinition#NAME} begins a transaction with the
   * specified name.
   */
  @Test
  public void testBeginTransactionName() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());

    try {

      // Check if the test user has access to v$transaction
      assumeTrue(awaitOne(Flux.from(connection.createStatement(
        "SELECT name FROM v$transaction FETCH FIRST ROW ONLY")
          .execute())
          .flatMap(result -> result.getRowsUpdated())
          .then(Mono.just(true))
          .onErrorResume(R2dbcException.class, r2dbcException ->
            // ORA-00942 is raised when the user doesn't have access
            942 == r2dbcException.getErrorCode()
              ? Mono.just(false)
              : Mono.error(r2dbcException))),
        "V$TRANSACTION is not accessible to the test user. " +
          "Grant access as SYSDBA with: " +
          "\"GRANT SELECT ON v_$transaction TO "+user()+"\"");

      // Insert into this table after beginning a transaction
      awaitExecution(connection.createStatement(
        "CREATE TABLE testBeginTransactionName (value VARCHAR(10))"));

      // Expect auto-commit to be true before the transaction begins
      assertTrue(connection.isAutoCommit(),
        "Unexpected return value from isAutoCommit() before" +
          " beginTransaction(TransactionDefinition)");

      // Try to use a unique transaction name
      String name = "testBeginTransactionName : " + System.nanoTime();
      awaitNone(connection.beginTransaction(transactionDefinition(Map.of(
        NAME, name))));

      // Expect auto-commit to be false after the transaction begins
      assertFalse(connection.isAutoCommit(),
        "Unexpected return value from isAutoCommit() after" +
          " beginTransaction(TransactionDefinition)");

      // Verify the transaction name appears in v$transaction. The name
      // will only appear after executing DML within the transaction, so
      // execute an INSERT first.
      awaitUpdate(1, connection.createStatement(
        "INSERT INTO testBeginTransactionName VALUES ('A')"));
      awaitQuery(List.of(name),
        row -> row.get("name", String.class),
        connection.createStatement(
          "SELECT name FROM v$transaction WHERE name = :name")
          .bind("name", name));

      awaitNone(connection.rollbackTransaction());
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testBeginTransactionName"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies
   * {@link OracleConnectionImpl#beginTransaction(TransactionDefinition)} with
   * {@link TransactionDefinition#READ_ONLY} having a value of {@code true}
   */
  @Test
  public void testBeginTransactionReadOnly() {
    Connection sessionA =
      Mono.from(sharedConnection()).block(connectTimeout());

    try {
      Connection sessionB =
        Mono.from(newConnection()).block(connectTimeout());

      try {
        awaitExecution(sessionA.createStatement(
          "CREATE TABLE testBeginTransactionReadOnly (value VARCHAR(10))"));

        // Workaround a known database limitation in which an ORA-01466 error
        // results from querying a table during a READ ONLY transaction when
        // the transaction begins less than one second after the table was
        // created; The database incorrectly detects that the table was altered
        // after the transaction began.
        try{Thread.sleep(2_000L);}catch(Throwable e){e.printStackTrace();}

        // Commit one insert into a table with a different session before
        // sessionA's transaction begins
        awaitNone(sessionB.beginTransaction());
        awaitUpdate(1, sessionB.createStatement(
          "INSERT INTO testBeginTransactionReadOnly VALUES ('a')"));
        awaitNone(sessionB.commitTransaction());

        // Insert one more row without committing
        awaitNone(sessionB.beginTransaction());
        awaitUpdate(1, sessionB.createStatement(
          "INSERT INTO testBeginTransactionReadOnly VALUES ('b')"));

        // Begin a READ ONLY transaction with sessionA and expect a query result
        // having only the committed row, and not the uncomitted row
        awaitNone(sessionA.commitTransaction());
        awaitNone(sessionA.beginTransaction(transactionDefinition(Map.of(
          READ_ONLY, true))));
        awaitQuery(List.of("a"), row -> row.get(0, String.class),
          sessionA.createStatement(
            "SELECT value FROM testBeginTransactionReadOnly"));

        // Commit the uncommitted row on sessionB, and expect it to not be
        // visible in sessionA's READ ONLY transaction
        awaitNone(sessionB.commitTransaction());
        awaitQuery(List.of("a"), row -> row.get(0, String.class),
          sessionA.createStatement(
            "SELECT value FROM testBeginTransactionReadOnly"));

        // End sessionA's transaction and expect both inserts to become visible
        awaitNone(sessionA.commitTransaction());
        awaitQuery(List.of("a", "b"), row -> row.get(0, String.class),
          sessionA.createStatement(
            "SELECT value FROM testBeginTransactionReadOnly"));
      }
      catch (Throwable e) {e.printStackTrace();}
      finally {
        tryAwaitExecution(sessionB.createStatement(
          "DROP TABLE testBeginTransactionReadOnly"));
        awaitNone(sessionB.close());
      }
    }
    finally {
      awaitNone(sessionA.close());
    }
  }

  /**
   * Verifies
   * {@link OracleConnectionImpl#beginTransaction(TransactionDefinition)} with
   * {@link TransactionDefinition#READ_ONLY} having a value of {@code false}
   */
  @Test
  public void testBeginTransactionReadWrite() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      verifyReadCommittedIsolation(connection,
        connection.beginTransaction(transactionDefinition(Map.of(
          READ_ONLY, false))));
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies
   * {@link OracleConnectionImpl#beginTransaction(TransactionDefinition)} with
   * {@link TransactionDefinition#NAME} begins a transaction with the default
   * isolation level, READ COMMITTED.
   */
  @Test
  public void testBeginTransactionNameIsolationLevel() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Try to use a unique transaction name
      String name =
        "testBeginTransactionNameIsolationLevel : " + System.nanoTime();
      verifyReadCommittedIsolation(connection,
        connection.beginTransaction(transactionDefinition(Map.of(NAME, name))));
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies that a {@code beginTransactionPublisher} begins a transaction
   * with the READ COMMITTED isolation level for {@code sessionA}
   * @param sessionA Database session
   * @param beginTransactionPublisher Publishes {@code onComplete} when a
   * READ COMMITTED transaction begins for {@code sessionA}
   */
  private static void verifyReadCommittedIsolation(
    Connection sessionA, Publisher<Void> beginTransactionPublisher) {

    // Expect the publisher to set auto-commit false when the first
    // subscriber subscribes
    assertTrue(sessionA.isAutoCommit(),
      "Unexpected return value from isAutoCommit() before" +
        " beginTransaction()");

    try {
      // Insert into this table after beginning a transaction
      awaitExecution(sessionA.createStatement(
        "CREATE TABLE verifyReadCommittedIsolation (value VARCHAR(10))"));

      awaitNone(beginTransactionPublisher);
      assertFalse(
        sessionA.isAutoCommit(),
        "Unexpected return value from isAutoCommit() after" +
          " beginTransaction()");

      // Expect the publisher to NOT repeatedly set auto-commit to false
      // for each subscriber
      awaitNone(sessionA.setAutoCommit(true));
      awaitNone(beginTransactionPublisher);
      assertTrue(
        sessionA.isAutoCommit(),
        "Unexpected return value from isAutoCommit() after multiple " +
          "subscriptions to a beginTransaction() publisher");

      // Now begin a transaction and verify that a table INSERT is not visible
      // until the transaction is committed.
      awaitNone(sessionA.beginTransaction());
      assertFalse(
        sessionA.isAutoCommit(),
        "Unexpected return value from isAutoCommit() after" +
          " beginTransaction()");
      awaitUpdate(1, sessionA.createStatement(
        "INSERT INTO verifyReadCommittedIsolation VALUES ('A')"));

      // sessionB doesn't see the INSERT made in sessionA's open transaction
      Connection sessionB =
        Mono.from(newConnection()).block(connectTimeout());
      try {
        Statement selectInSessionB = sessionB.createStatement(
          "SELECT value FROM verifyReadCommittedIsolation");
        awaitQuery(
          Collections.emptyList(), row -> 0, selectInSessionB);

        // Now sessionA COMMITs and sessionB can now see the INSERT
        awaitNone(sessionA.commitTransaction());
        awaitQuery(List.of("A"), row -> row.get("value"), selectInSessionB);
      }
      finally {
        awaitNone(sessionB.close());
      }
    }
    finally {
      tryAwaitExecution(sessionA.createStatement(
        "DROP TABLE verifyReadCommittedIsolation"));
    }
  }

  /**
   * Returns a {@code TransactionDefinition} having a set of {@code Option} to
   * value mappings specified as {@code optionValues}.
   * @param optionValues {@code Option} to value mappings. Not null.
   * @return {@code TransactionDefintion} specified by {@code optionValues}.
   */
  private static TransactionDefinition transactionDefinition(
    Map<Option<?>, Object> optionValues) {
    return new TransactionDefinition() {
      @Override
      public <T> T getAttribute(Option<T> option) {
        @SuppressWarnings("unchecked")
        T value = (T) optionValues.get(option);
        return value;
      }
    };
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#close()}
   * TODO: Verify resource release upon Subscription.request(long). Consider
   * querying V$SESSION to check if JDBC connections are being closed correctly.
   */
  @Test
  public void testClose() {

    // Expect the connection to remain open until the close publisher is
    // subscribed to.
    Connection connection =
      Mono.from(newConnection()).block(connectTimeout());
    Publisher<Void> closePublisher = connection.close();

    // Expect the connection and objects it creates to be valid when the
    // connection is open
    awaitOne(true, connection.validate(ValidationDepth.LOCAL));
    awaitOne(true, connection.validate(ValidationDepth.REMOTE));
    Statement statement = connection.createStatement("SELECT 1 FROM sys.dual");
    awaitQuery(List.of(1), row -> row.get(0, Integer.class), statement);
    Batch batch = connection.createBatch()
      .add("SELECT 2 FROM sys.dual");
    awaitOne(2, Flux.from(batch.execute())
      .flatMap(result ->
        result.map((row, metadata) -> row.get(0, Integer.class))));

    // Expect the close() Publisher to repeat signals to multiple subscribers
    awaitNone(closePublisher);

    // Expect the validate(ValidationDepth) Publisher to emit false
    awaitOne(false, connection.validate(ValidationDepth.LOCAL));
    awaitOne(false, connection.validate(ValidationDepth.REMOTE));

    // Expect the closed connection and objects it created to throw
    // IllegalStateException when methods requiring an open connection are
    // called
    assertThrows(
      IllegalStateException.class,
      () -> statement.fetchSize(100));
    assertThrows(
      IllegalStateException.class,
      statement::execute);
    assertThrows(
      IllegalStateException.class,
      () -> batch.add("SELECT 100 FROM sys.dual"));
    assertThrows(
      IllegalStateException.class,
      batch::execute);
    assertThrows(
      IllegalStateException.class,
      () -> connection.createStatement("SELECT 2 FROM dual"));
    assertThrows(
      IllegalStateException.class,
      () -> connection.isAutoCommit());
    assertThrows(
      IllegalStateException.class,
      () -> connection.createBatch());
    assertThrows(
      IllegalStateException.class,
      () -> connection.getMetadata());
    assertThrows(
      IllegalStateException.class,
      () -> connection.getTransactionIsolationLevel());
    assertThrows(IllegalStateException.class,
      () -> connection.beginTransaction());
    assertThrows(IllegalStateException.class,
      () -> connection.beginTransaction(IsolationLevel.READ_COMMITTED));
    assertThrows(IllegalStateException.class,
      () -> connection.commitTransaction());
    assertThrows(IllegalStateException.class,
      () -> connection.createBatch());
    assertThrows(IllegalStateException.class,
      () -> connection.createSavepoint("test"));
    assertThrows(IllegalStateException.class,
      () -> connection.createStatement("SELECT 2 FROM sys.daul"));
    assertThrows(IllegalStateException.class,
      () -> connection.getMetadata());
    assertThrows(IllegalStateException.class,
      () -> connection.getTransactionIsolationLevel());
    assertThrows(IllegalStateException.class,
      () -> connection.isAutoCommit());
    assertThrows(IllegalStateException.class,
      () -> connection.releaseSavepoint("test"));
    assertThrows(IllegalStateException.class,
      () -> connection.rollbackTransaction());
    assertThrows(IllegalStateException.class,
      () -> connection.rollbackTransactionToSavepoint("test"));
    assertThrows(IllegalStateException.class,
      () -> connection.setAutoCommit(true));
    assertThrows(IllegalStateException.class,
      () ->
        connection.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED));

    // Expect multiple subscribers to see same the signal from the close()
    // publisher
    awaitNone(closePublisher);
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#commitTransaction()}
   */
  @Test
  public void testCommitTransaction() {
    Connection sessionA =
      Mono.from(sharedConnection()).block(connectTimeout());

    // Expect commit to be a no-op when auto-commit is enabled
    assertTrue(sessionA.isAutoCommit(),
      "Expected isAutoCommit() to return true for a new connection");
    awaitNone(sessionA.commitTransaction());
    awaitNone(sessionA.setAutoCommit(false));

    try {
      // Insert into this table during a transaction
      awaitExecution(sessionA.createStatement(
        "CREATE TABLE testCommitTransaction (value VARCHAR(10))"));

      try {
        // Verify that table INSERTs are not visible until the transaction is
        // committed.
        Statement insertInSessionA = sessionA.createStatement(
          "INSERT INTO testCommitTransaction VALUES ('A')");
        awaitUpdate(1, insertInSessionA);
        awaitUpdate(1, insertInSessionA);

        // Expect the commit publisher to defer the commit until a
        // subscriber subscribes.
        Publisher<Void> commitInSessionA = sessionA.commitTransaction();

        // sessionB doesn't see the INSERT made in sessionA's open transaction
        Connection sessionB =
          Mono.from(newConnection()).block(connectTimeout());
        try {
          Statement selectInSessionB = sessionB.createStatement(
            "SELECT value FROM testCommitTransaction");
          awaitQuery(
            Collections.emptyList(), row -> row.get(0), selectInSessionB);

          // Now sessionA COMMITs and sessionB can now see the INSERTs
          awaitNone(commitInSessionA);
          awaitQuery(
            List.of("A", "A"), row -> row.get(0), selectInSessionB);

          // Expect the commit publisher to NOT repeatedly commit for each
          // subscriber
          awaitUpdate(1, insertInSessionA);
          awaitNone(commitInSessionA);
          awaitQuery(
            List.of("A", "A"), row -> row.get(0), selectInSessionB);
        }
        finally {
          awaitNone(sessionB.close());
        }
      }
      finally {
        tryAwaitExecution(sessionA.createStatement(
          "DROP TABLE testCommitTransaction"));
      }
    }
    finally {
      awaitNone(sessionA.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#createBatch()}
   */
  @Test
  public void testCreateBatch() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      awaitMany(
        List.of(0, 1, 2),
        Flux.from(connection.createBatch()
          .add("SELECT 0 FROM dual")
          .add("SELECT 1 FROM dual")
          .add("SELECT 2 FROM dual")
          .execute())
          .flatMap(result ->
            result.map((row, metadata) -> row.get(0, Integer.class))));
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#createStatement(String)}
   */
  @Test
  public void testCreateStatement() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());

    assertThrows(
      IllegalArgumentException.class,
      () -> connection.createStatement(null));

    try {
      awaitQuery(
        List.of("Hello, Oracle"),
        row -> row.get(0),
        connection.createStatement(
          "SELECT 'Hello, Oracle' AS greeting FROM dual"));
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#isAutoCommit()}
   */
  @Test
  public void testIsAutoCommit() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());

    try {
      assertTrue(connection.isAutoCommit(),
        "Unexpected value returned by isAutoCommit() for newly a created" +
          " connection.");
      awaitNone(connection.setAutoCommit(false));
      assertFalse(connection.isAutoCommit(),
        "Unexpected value returned by isAutoCommit() after" +
          " setAutoCommit(false).");

      awaitNone(connection.setAutoCommit(true));
      assertTrue(connection.isAutoCommit(),
        "Unexpected value returned by isAutoCommit() after" +
          " setAutoCommit(true).");
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#getMetadata()}
   */
  @Test
  public void testGetMetadata() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());

    try {
      assertNotNull(connection.getMetadata());
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#createSavepoint(String)}
   */
  @Test
  public void testCreateSavepoint() {
    // TODO: Oracle R2DBC does not implement
    // Connection.createSavepoint(String)
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#releaseSavepoint(String)}
   */
  @Test
  public void testReleaseSavepoint() {
    // TODO: Oracle R2DBC does not implement
    // Connection.releaseSavepoint(String)
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#rollbackTransaction()}
   */
  @Test
  public void testRollbackTransaction() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());

    // Expect a no-op when auto-commit is enabled
    assertTrue(connection.isAutoCommit(),
      "Unexpected value returned by isAutoCommit() for newly a created" +
        " connection.");
    awaitNone(connection.rollbackTransaction());

    try {
      // INSERT rows into this table, verify they are visible to the current
      // session prior to a rollback, and then not visible after a rollback.
      awaitExecution(connection.createStatement(
        "CREATE TABLE testRollbackTransaction (value VARCHAR(100))"));
      try {
        // INSERT values
        awaitNone(connection.setAutoCommit(false));
        Statement insert = connection.createStatement(
          "INSERT INTO testRollbackTransaction VALUES (:value)");
        List<String> values = List.of(
          // Alphabetical order matches order of ORDER BY value in SQL
          "Bonjour, Oracle",
          "Hello, Oracle",
          "Hola, Oracle",
          "Namaste, Oracle",
          "Ni hao, Oracle");
        values.forEach(value -> insert.bind("value", value).add());
        awaitUpdate(
          values.stream().map(value -> 1).collect(Collectors.toList()),
          insert);

        // Expect the rollback publisher to defer execution
        Publisher<Void> rollbackPublisher = connection.rollbackTransaction();
        Statement select = connection.createStatement(
          "SELECT value FROM testRollbackTransaction ORDER BY value");
        awaitQuery(values, row -> row.get("value", String.class), select);
        awaitNone(rollbackPublisher);
        awaitQuery(
          Collections.emptyList(), row -> (String) row.get("value"), select);

        // Expect rollback publisher to not repeat execution for each
        // subscriber.
        awaitUpdate(1, insert.bind(0, "Aloha, Oracle"));
        awaitQuery(List.of("Aloha, Oracle"), row -> row.get(0), select);
        awaitNone(rollbackPublisher);
        awaitQuery(List.of("Aloha, Oracle"), row -> row.get(0), select);
      }
      finally {
        tryAwaitExecution(connection.createStatement(
          "DROP TABLE testRollbackTransaction"));
      }
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#rollbackTransactionToSavepoint(String)}
   */
  @Test
  public void testRollbackTransactionToSavepoint() {
    // TODO: Oracle R2DBC does not implement
    // Connection.rollbackTransactionToSavepoint(String)
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#setAutoCommit(boolean)}
   */
  @Test
  public void testSetAutoCommit() {
    Connection sessionA =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Insert into this table with auto-commit enabled/disabled
      awaitExecution(sessionA.createStatement(
        "CREATE TABLE testSetAutoCommit (value VARCHAR(10))"));
      try {
        // sessionB doesn't see uncommitted INSERTs made in sessionA's
        // transaction
        Connection sessionB =
          Mono.from(newConnection()).block(connectTimeout());
        try {
          // Statement executions are automatically committed when auto-commit
          // is enabled. Expect sessionB to see sessionA's INSERT
          assertTrue(sessionA.isAutoCommit(),
            "Unexpected value returned by isAutoCommit() for newly a created" +
              " connection.");
          Statement insertInSessionA = sessionA.createStatement(
            "INSERT INTO testSetAutoCommit VALUES (?)");
          awaitUpdate(1, insertInSessionA.bind(0, "A"));
          Statement selectInSessionB = sessionB.createStatement(
            "SELECT value FROM testSetAutoCommit ORDER BY value");
          awaitQuery(List.of("A"), row -> row.get(0), selectInSessionB);

          // Expect the setAutoCommitPublisher to not change the auto-commit
          // mode until it has been subscribed to. Auto-commit remains enabled
          // for sessionA's next INSERT
          Publisher<Void> disableAutoCommitPublisher =
            Mono.from(sessionA.setAutoCommit(false));
          assertTrue(sessionA.isAutoCommit(),
            "Unexpected value returned by isAutoCommit() for newly a created" +
              " connection.");
          awaitUpdate(1, insertInSessionA.bind(0, "A"));
          awaitQuery(List.of("A", "A"), row -> row.get(0), selectInSessionB);

          // Disable auto-commit, and expect sessionA's INSERT to not be visible
          // in sessionB
          awaitNone(disableAutoCommitPublisher);
          awaitUpdate(1, insertInSessionA.bind(0, "B"));
          awaitQuery(List.of("A", "A"), row -> row.get(0), selectInSessionB);

          // Expect a no-op when auto-commit isn't changed
          awaitNone(sessionA.setAutoCommit(false));
          awaitUpdate(1, insertInSessionA.bind(0, "B"));
          awaitQuery(List.of("A", "A"), row -> row.get(0), selectInSessionB);

          // Expect commit to make INSERTs visible in sessionB
          awaitNone(sessionA.commitTransaction());
          awaitQuery(
            List.of("A", "A", "B", "B"), row -> row.get(0), selectInSessionB);

          // Expect INSERTs to be commited when auto-commit is re-enabled. The
          // auto-commit mode doesn't change until the setAutoCommit publisher is
          // subscribed to.
          Publisher<Void> enableAutoCommitPublisher =
            sessionA.setAutoCommit(true);
          assertFalse(sessionA.isAutoCommit(),
            "Unexpected value returned by isAutoCommit() before subscribing to"
              + " setAutoCommit(true) publisher");
          awaitMany(
            List.of(1, 1),
            Flux.from(sessionA.createBatch()
              .add("INSERT INTO testSetAutoCommit VALUES ('C')")
              .add("INSERT INTO testSetAutoCommit VALUES ('C')")
              .execute())
              .flatMap(Result::getRowsUpdated));
          awaitQuery(
            List.of("A", "A", "B", "B"), row -> row.get(0), selectInSessionB);
          awaitNone(enableAutoCommitPublisher);
          assertTrue(sessionA.isAutoCommit(),
            "Unexpected value returned by isAutoCommit() after subscribing to"
              + " setAutoCommit(true) publisher");
          awaitQuery(
            List.of("A", "A", "B", "B", "C", "C"), row -> row.get(0), selectInSessionB);

          // Expect setAutoCommit(false) publisher to not repeat its action
          // for each subscriber.
          awaitNone(disableAutoCommitPublisher);
          assertTrue(sessionA.isAutoCommit(),
            "Unexpected value returned by isAutoCommit() after subscribing to"
              + " setAutoCommit(false) publisher multiple times");

          // Expect setAutoCommit(true) publisher to not repeat its action
          // for each subscriber.
          awaitNone(sessionA.setAutoCommit(false));
          assertFalse(sessionA.isAutoCommit(),
            "Unexpected value returned by isAutoCommit() after subscribing to"
              + " setAutoCommit(false)");
          awaitUpdate(
            List.of(1, 1),
            insertInSessionA
              .bind(0, "D").add()
              .bind(0, "D").add());
          awaitNone(enableAutoCommitPublisher);
          assertFalse(sessionA.isAutoCommit(),
            "Unexpected value returned by isAutoCommit() after subscribing to"
              + " setAutoCommit(true) publisher multiple times");
          awaitQuery(
            List.of("A", "A", "B", "B", "C", "C"), row -> row.get(0),
            selectInSessionB);

          // Expect rolled back changes to not be visible when auto-commit is
          // reenabled
          awaitNone(sessionA.rollbackTransaction());
          awaitNone(sessionA.setAutoCommit(true));
          awaitQuery(
            List.of("A", "A", "B", "B", "C", "C"), row -> row.get(0),
            selectInSessionB);
        }
        finally {
          awaitNone(sessionB.close());
        }
      }
      finally {
        tryAwaitExecution(sessionA.createStatement(
          "DROP TABLE testSetAutoCommit"));
      }
    }
    finally {
      awaitNone(sessionA.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#getTransactionIsolationLevel()}
   */
  @Test
  public void testGetTransactionIsolationLevel() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Expect the initial isolation level to be READ_COMMITTED
      assertEquals(
        IsolationLevel.READ_COMMITTED,
        connection.getTransactionIsolationLevel(),
        "Unexpected return value of getTransactionIsolationLevel() for a" +
          " newly created connection");
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#setTransactionIsolationLevel(IsolationLevel)}
   * when an unsupported isolation level is provided as input.
   */
  @Test
  public void testSetTransactionIsolationLevelUnsupported() {
    Connection sessionA =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {

      // Verify isolation levels by reading inserts made into this table. The
      // table name is an abbreviation of
      // testSetTransactionIsolationLevelUnsupported because the full name
      // might exceed the maximum identifier length on some databases
      awaitExecution(sessionA.createStatement(
        "CREATE TABLE tstilUnsupported" +
          " (value VARCHAR(10))"));

      try {
        // Set the READ COMMITTED level, and expect the sessionA to remain at
        // this level after setting unsupported levels. Expect setting any
        // level other than READ COMMITTED to result in onError
        awaitNone(
          sessionA.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED));
        awaitError(
          R2dbcException.class,
          sessionA.setTransactionIsolationLevel(
            IsolationLevel.READ_UNCOMMITTED));
        awaitError(
          R2dbcException.class,
          sessionA.setTransactionIsolationLevel(
            IsolationLevel.REPEATABLE_READ));
        awaitError(
          R2dbcException.class,
          sessionA.setTransactionIsolationLevel(
            IsolationLevel.SERIALIZABLE));
        assertEquals(
          IsolationLevel.READ_COMMITTED,
          sessionA.getTransactionIsolationLevel(),
          "Unexpected return value of getTransactionIsolationLevel() after " +
            "setting an unsupported isolation level");

        // Verify the READ COMMITTED level remains set by checking if a phantom
        // read is possible. READ COMMITTED is the only isolation level
        // supported by Oracle Database that allows phantom reads.
        Connection sessionB =
          Mono.from(newConnection()).block(connectTimeout());
        try {
          awaitNone(sessionB.beginTransaction());
          awaitUpdate(1, sessionB.createStatement(
            "INSERT INTO tstilUnsupported" +
              " VALUES('A')"));
          awaitNone(sessionA.beginTransaction());
          awaitNone(sessionB.commitTransaction());
          awaitQuery(
            List.of("A"),
            row -> row.get("value"),
            sessionA.createStatement(
              "SELECT value FROM tstilUnsupported"));
        }
        finally {
          awaitNone(sessionB.close());
        }
      }
      finally {
        tryAwaitExecution(sessionA.createStatement(
          "DROP TABLE tstilUnsupported"));
      }
    }
    finally {
      assertNull(
        Mono.from(sessionA.close()).block(connectTimeout()),
        "Unexpected onNext signal from close()");
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#setTransactionIsolationLevel(IsolationLevel)}
   * when {@link IsolationLevel#READ_COMMITTED} is supplied as an argument.
   */
  @Test
  public void testSetTransactionIsolationLevelReadCommitted() {
    Connection sessionA =
      Mono.from(sharedConnection()).block(connectTimeout());
    awaitNone(
      sessionA.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED));

    try {
      // Verify isolation levels by reading inserts made into this table. The
      // table name is an abbreviation of
      // testSetTransactionIsolationLevelReadCommitted because the full name
      // might exceed the maximum identifier length on some databases
      awaitExecution(sessionA.createStatement(
        "CREATE TABLE tstilReadCommited" +
          " (value VARCHAR(10))"));

      try {
        // sessionB executes writes to the test table, and sessionA reads
        // from the test table.
        Connection sessionB =
          Mono.from(newConnection()).block(connectTimeout());
        assertEquals(
          IsolationLevel.READ_COMMITTED,
          sessionB.getTransactionIsolationLevel(),
          "Unexpected return value of getTransactionIsolationLevel() for a"
            + " newly created connection");
        try {
          Statement insertInSessionB = sessionB.createStatement(
            "INSERT INTO tstilReadCommited" +
              " VALUES (?)");
          Statement updateInSessionB = sessionB.createStatement(
            "UPDATE tstilReadCommited" +
              " SET value = :newValue WHERE value = :oldValue");
          Statement selectInSessionA = sessionA.createStatement(
            "SELECT value FROM tstilReadCommited" +
              " ORDER BY value");

          // sessionB INSERTs a row and commits before sessionA begins a
          // READ COMMITTED transaction. The row is visible to sessionA because
          // it was committed before the transaction began.
          awaitNone(sessionB.beginTransaction());
          awaitUpdate(1, insertInSessionB.bind(0, "A"));
          awaitNone(sessionB.commitTransaction());
          awaitNone(sessionA.beginTransaction());
          awaitQuery(List.of("A"), row -> row.get(0), selectInSessionA);

          // Expect setting the READ COMMITTED level to prevent dirty reads.
          // sessionB UPDATEs a row and doesn't commit. The updated row is not
          // visible to sessionA.
          awaitUpdate(
            1, updateInSessionB.bind("oldValue", "A").bind("newValue", "B"));
          awaitQuery(List.of("A"), row -> row.get(0), selectInSessionA);

          // Expect setting the READ COMMITTED level to allow non-repeatable
          // reads. The UPDATE is committed in sessionB. The updated row is
          // visible to sessionA.
          awaitNone(sessionB.commitTransaction());
          awaitQuery(List.of("B"), row -> row.get(0), selectInSessionA);

          // Expect setting the READ COMMITTED level to allow Phantom reads.
          // sessionB INSERTs a new row and commits. The INSERTed row is
          // visible to sessionA.
          awaitNone(sessionB.beginTransaction());
          awaitUpdate(1, insertInSessionB.bind(0, "C"));
          awaitNone(sessionB.commitTransaction());
          awaitQuery(List.of("B", "C"), row -> row.get(0), selectInSessionA);

          // sessionA completes it's transaction and all changes from sessionB
          // become visible.
          awaitUpdate(1, sessionA.createStatement(
            "INSERT INTO tstilReadCommited" +
              " VALUES('D')"));
          awaitNone(sessionA.commitTransaction());
          awaitQuery(
            List.of("B", "C", "D"), row -> row.get(0), selectInSessionA);
        }
        finally {
          awaitNone(sessionB.close());
        }
      }
      finally {
        tryAwaitExecution(sessionA.createStatement(
          "DROP TABLE tstilReadCommited"));
      }
    }
    finally {
      awaitNone(sessionA.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleConnectionImpl#validate(ValidationDepth)}.
   * TODO: Verify REMOTE validation by killing the database session as SYSDBA
   * and then expecting the validation to fail.
   */
  @Test
  public void testValidate() {
    Connection connection = Mono.from(newConnection()).block(connectTimeout());
    try {
      Publisher<Boolean> validateLocalPublisher =
        connection.validate(ValidationDepth.LOCAL);
      Publisher<Boolean> validateRemotePublisher =
        connection.validate(ValidationDepth.REMOTE);

      // Expect validation publishers to emit true when the connection is open.
      awaitOne(true, validateLocalPublisher);
      awaitOne(true, validateRemotePublisher);

      // Expect unsubscribed validation publishers to emit false when the
      // connection is closed
      tryAwaitNone(connection.close());
      awaitOne(false, connection.validate(ValidationDepth.LOCAL));
      awaitOne(false, connection.validate(ValidationDepth.REMOTE));

      // Expect validation publishers to not repeat the validation for each
      // subscriber.
      awaitOne(true, validateLocalPublisher);
      awaitOne(true, validateRemotePublisher);
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link Connection#setStatementTimeout(Duration)}
   */
  @Test
  public void testSetStatementTimeout() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {

      // Create a table is locked by one session, and updated by another
      awaitExecution(connection.createStatement(
        "CREATE TABLE testSetStatementTimeout(value NUMBER)"));

      // Expect IllegalArgumentException for a null or negative timeout
      assertThrows(IllegalArgumentException.class, () ->
        connection.setStatementTimeout(null));
      assertThrows(IllegalArgumentException.class, () ->
        connection.setStatementTimeout(Duration.ofSeconds(-1)));

      // Create another session that locks a table
      Connection sessionB = Mono.from(newConnection()).block(connectTimeout());
      try {
        awaitUpdate(1, sessionB.createStatement(
          "INSERT INTO testSetStatementTimeout VALUES (0)"));
        Result lockingResult = Mono.from(sessionB.createStatement(
          "SELECT * FROM testSetStatementTimeout FOR UPDATE")
          .execute())
          .block(sqlTimeout());

        // Configure the connection with a 2 second timeout and create a
        // statement that updates the locked table. Expect the statement's
        // execution to result in an R2dbcTimeoutException no sooner than 2
        // seconds, and no later than 12 seconds (allow 10 seconds of leeway to
        // account for testing on slow systems).
        awaitNone(connection.setStatementTimeout(Duration.ofSeconds(2)));
        Duration start = Duration.ofNanos(System.nanoTime());
        awaitError(R2dbcTimeoutException.class,
          Mono.from(connection.createStatement(
            "UPDATE testSetStatementTimeout SET value = 1 WHERE value = 0")
          .execute())
          .flatMapMany(Result::getRowsUpdated));
        Duration actual = Duration.ofNanos(System.nanoTime()).minus(start);
        assertTrue(actual.toSeconds() >= 2,
          "Timeout triggered too soon: " + actual);
        assertTrue(actual.toSeconds() < 12,
          "Timeout triggered too late: " + actual);
      }
      finally {
        tryAwaitNone(sessionB.close());
      }
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testSetStatementTimeout"));
      tryAwaitNone(connection.close());
    }
  }

}
