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

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
import oracle.r2dbc.OracleR2dbcObject;
import oracle.r2dbc.OracleR2dbcTypes;
import oracle.r2dbc.test.DatabaseConfig;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Arrays.asList;
import static oracle.r2dbc.test.DatabaseConfig.connectTimeout;
import static oracle.r2dbc.test.DatabaseConfig.sharedConnection;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the Oracle R2DBC Driver implements behavior related to {@link Blob}
 * and {@link Clob} types that is specified in its class and method level
 * javadocs, in the javadocs of {@code Blob} and {@code Clob}, and in Section
 * 12 of the R2DBC 0.8.2 Specification.
 */
public class OracleLargeObjectsTest {

  private static final AtomicInteger nextByte = new AtomicInteger(0);

  /**
   * Returns a byte array holding a {@code count} of bytes. Each byte value
   * is within the inclusive range of single byte UTF-8 character encodings
   * between 'a' and 'z' (ASCII characters). This allows the byte[] to be
   * converted to character values for CLOB tests. The byte array values are
   * sourced from a repeating sequence of 26 characters from 'a' to 'z'.
   * @param count Number of bytes in the returned array
   * @return An array containing a {@code count} of bytes
   */
  private static byte[] getBytes(int count) {
    byte[] bytes = new byte[count];
    for (int i = 0; i < bytes.length; i++)
      bytes[i] = (byte)('a' + (nextByte.getAndIncrement() % 26));
    return bytes;
  }

  /**
   * <p>
   * Constructs a Clob that publishes the {@code content} of a byte array.
   * The content is encoded as UTF-8 characters.
   * </p><p>
   * The publisher returned by {@link Clob#stream()} will repeatedly emit
   * {@code onNext} with the a String that wraps a region of the provided {@code
   * content}. The publisher emits {@code onComplete} after all regions
   * within the {@code content} have been emitted to {@code onNext}.
   * </p>
   * @param content Bytes to be published
   */
  private static Clob createClob(byte[] content) {
    AtomicInteger limit = new AtomicInteger(0);
    return Clob.from(Flux.generate(sink -> {
      int position = limit.getAndAdd(2048);

      if (position < content.length) {
        int length = Math.min(content.length - position, 2048);
        sink.next(new String(
          content, position, length, StandardCharsets.UTF_8));
      }
      else {
        sink.complete();
      }
    }));
  }

  /**
   * <p>
   * Constructs a Clob that publishes the {@code content} of byte array.
   * </p><p>
   * The publisher returned by {@link Blob#stream()} will repeatedly emit
   * {@code onNext} with the same instance of a {@code ByteBuffer} that
   * wraps the provided {@code content}. On each invocation of {@code
   * onNext}, the {@code ByteBuffer's} position and limit designate a
   * region of the {@code content}. The publisher emits {@code onComplete}
   * after all regions within the {@code content} have been emitted to
   * {@code onNext}.
   * </p><p>
   * Expect the Oracle R2DBC Driver to write only bytes within the
   * {@code ByteBuffer's} current region on each {@code onNext} signal.
   * </p><p>
   * Expect the Oracle R2DBC Driver to not mutate the position or length of
   * a {@code ByteBuffer} emitted to {@code onNext}.
   * </p>
   * @param content Bytes to be published
   */
  private static Blob createBlob(byte[] content) {
    ByteBuffer bytesBuffer = ByteBuffer.wrap(content).limit(0);
    return Blob.from(Flux.generate(sink -> {
      bytesBuffer.position(bytesBuffer.limit())
        .limit(Math.min(content.length, bytesBuffer.limit() + 2048));

      if (bytesBuffer.hasRemaining())
        sink.next(bytesBuffer);
      else
        sink.complete();
    }));
  }

  /**
   * Blocks until {@code blob's} {@link Blob#stream()} emits
   * {@code onComplete/onError}. Verifies that the {@code blob} emits an
   * {@code expected} sequence of bytes. The blob's {@link Blob#discard()}
   * method is invoked before this method returns or throws.
   * @param expected Bytes expected to be emitted by the blob
   * @param blob A blob that emits bytes.
   */
  private static void awaitBytes(byte[] expected, Blob blob) {
    try {
      byte[] actual = new byte[expected.length];
      awaitOne(Flux.from(blob.stream()).reduce(
        ByteBuffer.wrap(actual), ByteBuffer::put));
      assertEquals(-1, Arrays.mismatch(expected, actual));
    }
    finally {
      // TODO: Verify that the temporary BLOB is actually being freed on the
      //  database session.
      awaitNone(blob.discard());
    }
  }

  /**
   * Blocks until {@code clob's} {@link Clob#stream()} emits
   * {@code onComplete/onError}. Verifies that the {@code clob} emits an
   * {@code expected} sequence of bytes as UTF-8 encoded characters. The clob's
   * {@link Clob#discard()} method is invoked before this method returns or
   * throws.
   * @param expected Bytes expected to be emitted by the clob
   * @param clob A clob that emits bytes.
   */
  private static void awaitBytes(byte[] expected, Clob clob) {
    try {
      byte[] actual = new byte[expected.length];
      awaitOne(Flux.from(clob.stream())
        .map(charSequence ->
          charSequence.toString().getBytes(StandardCharsets.UTF_8))
        .reduce(ByteBuffer.wrap(actual), ByteBuffer::put));
      assertEquals(-1, Arrays.mismatch(expected, actual));
    }
    finally {
      // TODO: Verify that the temporary CLOB is actually being freed on the
      //  database session.
      awaitNone(clob.discard());
    }
  }

  /**
   * Verify behavior of when binding a {@link Blob} to a {@link Statement},
   * and when getting a {@code Blob} from a {@link Row}.
   */
  @Test
  public void testBlobInsert() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Verify BLOB bind values used in INSERT statements on this table
      awaitExecution(connection.createStatement(
        "CREATE TABLE testBlobInsert (id NUMBER, x BLOB, y BLOB)"));
      byte[] xBytes = getBytes(64 * 1024);
      byte[] yBytes = getBytes(64 * 1024);

      // Verify asynchronous materialization of blob binds. The blob lengths
      // are each large enough to require multiple blob writing network calls
      // by the Oracle JDBC Driver.
      awaitUpdate(1, connection.createStatement(
        "INSERT INTO testBlobInsert (id, x, y) VALUES (:id, :x, :y)")
        .bind("id", 0)
        .bind("x", createBlob(xBytes))
        .bind("y", createBlob(yBytes)));

      // Expect row.get(int/String) to use Blob as the default Java type
      // mapping for BLOB type columns.
      List<Blob> blobs = awaitOne(Flux.from(connection.createStatement(
        "SELECT x,y FROM testBlobInsert WHERE id = 0")
        .execute())
        .flatMap(result -> result.map((row, metadata) ->
          asList(row.get("x", Blob.class), row.get("y", Blob.class))))
        .single());

      // Expect bytes written to INSERTed Blobs to match the bytes read from
      // SELECTed Blobs
      awaitBytes(xBytes, blobs.get(0));
      awaitBytes(yBytes, blobs.get(1));
    }
    finally {
      tryAwaitExecution(connection.createStatement("DROP TABLE testBlobInsert"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verify behavior of when binding a batch of {@link Blob} values to a
   * {@link Statement}, and when getting a {@code Blob} from a {@link Row}.
   */
  @Test
  public void testBlobBatchInsert() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Verify BLOB bind values used in INSERT statements on this table
      awaitExecution(connection.createStatement(
        "CREATE TABLE testBlobInsert (id NUMBER, x BLOB, y BLOB)"));
      byte[] xBytes0 = getBytes(64 * 1024);
      byte[] yBytes0 = getBytes(64 * 1024);
      byte[] xBytes1 = getBytes(64 * 1024);
      byte[] yBytes1 = getBytes(64 * 1024);

      // Verify asynchronous materialization of blob binds. The blob lengths
      // are each large enough to require multiple blob writing network calls
      // by the Oracle JDBC Driver.
      awaitUpdate(asList(1, 1), connection.createStatement(
        "INSERT INTO testBlobInsert (id, x, y) VALUES (:id, :x, :y)")
        .bind("id", 0)
        .bind("x", createBlob(xBytes0))
        .bind("y", createBlob(yBytes0))
        .add()
        .bind("id", 1)
        .bind("x", createBlob(xBytes1))
        .bind("y", createBlob(yBytes1)));

      // Expect row.get(int/String) to use Blob as the default Java type
      // mapping for BLOB type columns.
      List<List<Blob>> blobs = awaitMany(Flux.from(connection.createStatement(
        "SELECT x,y FROM testBlobInsert ORDER BY id")
        .execute())
        .flatMap(result -> result.map((row, metadata) ->
          asList(row.get("x", Blob.class), row.get("y", Blob.class)))));

      // Expect bytes written to INSERTed Blobs to match the bytes read from
      // SELECTed Blobs
      awaitBytes(xBytes0, blobs.get(0).get(0));
      awaitBytes(yBytes0, blobs.get(0).get(1));
      awaitBytes(xBytes1, blobs.get(1).get(0));
      awaitBytes(yBytes1, blobs.get(1).get(1));
    }
    finally {
      tryAwaitExecution(connection.createStatement("DROP TABLE testBlobInsert"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verify behavior of when binding a {@link Clob} to a {@link Statement},
   * and when getting a {@code Clob} from a {@link Row}.
   */
  @Test
  public void testClobInsert() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Verify Clob bind values used in INSERT statements on this table
      awaitExecution(connection.createStatement(
        "CREATE TABLE testClobInsert (id NUMBER, x Clob, y Clob)"));
      byte[] xBytes = getBytes(64 * 1024);
      byte[] yBytes = getBytes(64 * 1024);

      // Verify asynchronous materialization of Clob binds. The Clob lengths
      // are each large enough to require multiple Clob writing network calls
      // by the Oracle JDBC Driver.
      awaitUpdate(1, connection.createStatement(
        "INSERT INTO testClobInsert (id, x, y) VALUES (:id, :x, :y)")
        .bind("id", 0)
        .bind("x", createClob(xBytes))
        .bind("y", createClob(yBytes)));

      // Expect row.get(int/String) to support Clob as a Java type mapping
      List<Clob> Clobs = awaitOne(Flux.from(connection.createStatement(
        "SELECT x,y FROM testClobInsert WHERE id = 0")
        .execute())
        .flatMap(result -> result.map((row, metadata) ->
          asList(row.get("x", Clob.class), row.get("y", Clob.class))))
        .single());

      // Expect bytes written to INSERTed Clobs to match the bytes read from
      // SELECTed Clobs
      awaitBytes(xBytes, Clobs.get(0));
      awaitBytes(yBytes, Clobs.get(1));
    }
    finally {
      tryAwaitExecution(connection.createStatement("DROP TABLE testClobInsert"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verify behavior of when binding a batch of {@link Clob} values to a
   * {@link Statement}, and when getting a {@code Clob} from a {@link Row}.
   */
  @Test
  public void testClobBatchInsert() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Verify Clob bind values used in INSERT statements on this table
      awaitExecution(connection.createStatement(
        "CREATE TABLE testClobInsert (id NUMBER, x Clob, y Clob)"));
      byte[] xBytes0 = getBytes(64 * 1024);
      byte[] yBytes0 = getBytes(64 * 1024);
      byte[] xBytes1 = getBytes(64 * 1024);
      byte[] yBytes1 = getBytes(64 * 1024);

      // Verify asynchronous materialization of Clob binds. The Clob lengths
      // are each large enough to require multiple Clob writing network calls
      // by the Oracle JDBC Driver.
      awaitUpdate(asList(1, 1), connection.createStatement(
        "INSERT INTO testClobInsert (id, x, y) VALUES (:id, :x, :y)")
        .bind("id", 0)
        .bind("x", createClob(xBytes0))
        .bind("y", createClob(yBytes0))
        .add()
        .bind("id", 1)
        .bind("x", createClob(xBytes1))
        .bind("y", createClob(yBytes1)));

      // Expect row.get(int/String) to use Clob as the default Java type
      // mapping for CLOB type columns.
      List<List<Clob>> clobs = awaitMany(Flux.from(connection.createStatement(
        "SELECT x,y FROM testClobInsert ORDER BY id")
        .execute())
        .flatMap(result -> result.map((row, metadata) ->
          asList(row.get("x", Clob.class), row.get("y", Clob.class)))));

      // Expect bytes written to INSERTed Clobs to match the bytes read from
      // SELECTed Clobs
      awaitBytes(xBytes0, clobs.get(0).get(0));
      awaitBytes(yBytes0, clobs.get(0).get(1));
      awaitBytes(xBytes1, clobs.get(1).get(0));
      awaitBytes(yBytes1, clobs.get(1).get(1));

    }
    finally {
      tryAwaitExecution(connection.createStatement("DROP TABLE testClobInsert"));
      tryAwaitNone(connection.close());
    }
  }

  @Test
  public void testBlobObject() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    OracleR2dbcTypes.ObjectType objectType =
      OracleR2dbcTypes.objectType("BLOB_OBJECT");
    try {
      awaitExecution(connection.createStatement(
        "CREATE TYPE BLOB_OBJECT AS OBJECT(x BLOB, y BLOB)"));
      awaitExecution(connection.createStatement(
        "CREATE TABLE testBlobObject(id NUMBER, blobs BLOB_OBJECT)"));
      byte[] xBytes0 = getBytes(64 * 1024);
      byte[] yBytes0 = getBytes(64 * 1024);
      byte[] xBytes1 = getBytes(64 * 1024);
      byte[] yBytes1 = getBytes(64 * 1024);

      // Verify asynchronous materialization of Blob binds. The Blob lengths
      // are each large enough to require multiple Blob writing network calls
      // by the Oracle JDBC Driver.
      awaitUpdate(asList(1, 1), connection.createStatement(
          "INSERT INTO testBlobObject (id, blobs) VALUES (:id, :blobs)")
        .bind("id", 0)
        .bind("blobs", Parameters.in(objectType, new Object[]{
          createBlob(xBytes0),
          createBlob(yBytes0),
        }))
        .add()
        .bind("id", 1)
        .bind("blobs", Parameters.in(objectType, Map.of(
          "x", createBlob(xBytes1),
          "y", createBlob(yBytes1)))));

      // Expect OracleR2dbcObject.get(int/String) to support Blob as a Java type
      // mapping for BLOB type attributes.
      List<List<Blob>> blobs =
        awaitMany(Flux.from(connection.createStatement(
              "SELECT blobs FROM testBlobObject ORDER BY id")
            .execute())
          .flatMap(result ->
            result.map(row ->
              row.get("blobs", OracleR2dbcObject.class)))
          .map(blobObject -> List.of(
            blobObject.get("x", Blob.class),
            blobObject.get("y", Blob.class))));

      // Expect bytes written to INSERTed Blobs to match the bytes read from
      // SELECTed Blobs
      awaitBytes(xBytes0, blobs.get(0).get(0));
      awaitBytes(yBytes0, blobs.get(0).get(1));
      awaitBytes(xBytes1, blobs.get(1).get(0));
      awaitBytes(yBytes1, blobs.get(1).get(1));

    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testBlobObject"));
      tryAwaitExecution(connection.createStatement(
        "DROP type " + objectType.getName()));
      tryAwaitNone(connection.close());
    }
  }

  @Test
  public void testClobObject() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    OracleR2dbcTypes.ObjectType objectType =
      OracleR2dbcTypes.objectType("CLOB_OBJECT");
    try {
      awaitExecution(connection.createStatement(
        "CREATE TYPE CLOB_OBJECT AS OBJECT(x CLOB, y CLOB)"));
      awaitExecution(connection.createStatement(
        "CREATE TABLE testClobObject(id NUMBER, clobs CLOB_OBJECT)"));
      byte[] xBytes0 = getBytes(64 * 1024);
      byte[] yBytes0 = getBytes(64 * 1024);
      byte[] xBytes1 = getBytes(64 * 1024);
      byte[] yBytes1 = getBytes(64 * 1024);

      // Verify asynchronous materialization of Clob binds. The Clob lengths
      // are each large enough to require multiple Clob writing network calls
      // by the Oracle JDBC Driver.
      awaitUpdate(asList(1, 1), connection.createStatement(
          "INSERT INTO testClobObject (id, clobs) VALUES (:id, :clobs)")
        .bind("id", 0)
        .bind("clobs", Parameters.in(objectType, new Object[]{
          createClob(xBytes0),
          createClob(yBytes0),
        }))
        .add()
        .bind("id", 1)
        .bind("clobs", Parameters.in(objectType, Map.of(
          "x", createClob(xBytes1),
          "y", createClob(yBytes1)))));

      // Expect OracleR2dbcObject.get(int/String) to support Clob as a Java type
      // mapping for CLOB type attributes.
      List<List<Clob>> clobs =
        awaitMany(Flux.from(connection.createStatement(
          "SELECT clobs FROM testClobObject ORDER BY id")
          .execute())
          .flatMap(result ->
            result.map(row ->
              row.get("clobs", OracleR2dbcObject.class)))
          .map(clobObject -> List.of(
            clobObject.get("x", Clob.class),
            clobObject.get("y", Clob.class))));

      // Expect bytes written to INSERTed Clobs to match the bytes read from
      // SELECTed Clobs
      awaitBytes(xBytes0, clobs.get(0).get(0));
      awaitBytes(yBytes0, clobs.get(0).get(1));
      awaitBytes(xBytes1, clobs.get(1).get(0));
      awaitBytes(yBytes1, clobs.get(1).get(1));

    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testClobObject"));
      tryAwaitExecution(connection.createStatement(
        "DROP type " + objectType.getName()));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies behavior around discarding LOBs.
   *
   * A Blob/Clob bind should not be discarded until a user calls discard() and
   * subscribes. Oracle R2DBC used to call discard() itself on Blob/Clob binds,
   * which is not correct.
   *
   * When Connection.close() is called and subscribed to, it should not fail
   * due to Oracle JDBC bug #37160069 which requires all LOBs to be freed before
   * closeAsyncOracle is called.
   */
  @Test
  public void testLobDiscard() {
    byte[] data = getBytes(64 * 1024);

    class TestBlob implements Blob {
      final ByteBuffer blobData = ByteBuffer.wrap(data);

      boolean isDiscarded = false;

      @Override
      public Publisher<ByteBuffer> stream() {
        return Mono.just(blobData);
      }

      @Override
      public Publisher<Void> discard() {
        return Mono.<Void>empty()
          .doOnSuccess(nil -> isDiscarded = true);
      }
    }

    class TestClob implements Clob {
      final CharBuffer clobData =
        CharBuffer.wrap(new String(data, US_ASCII));

      boolean isDiscarded = false;

      @Override
      public Publisher<CharSequence> stream() {
        return Mono.just(clobData);
      }

      @Override
      public Publisher<Void> discard() {
        return Mono.<Void>empty()
          .doOnSuccess(nil -> isDiscarded = true);
      }
    }

    Connection connection =
      Mono.from(DatabaseConfig.newConnection()).block(connectTimeout());
    try {
      awaitExecution(
        connection.createStatement(
          "CREATE TABLE testLobDiscard (blobValue BLOB, clobValue CLOB)"));

      // Verify that LOBs are not discarded until discard() is subscribed to
      TestBlob testBlob = new TestBlob();
      TestClob testClob = new TestClob();
      awaitUpdate(1, connection.createStatement(
        "INSERT INTO testLobDiscard VALUES (?, ?)")
        .bind(0, testBlob)
        .bind(1, testClob));
      assertFalse(testBlob.isDiscarded);
      assertFalse(testClob.isDiscarded);
      awaitNone(testBlob.discard());
      awaitNone(testClob.discard());
      assertTrue(testBlob.isDiscarded);
      assertTrue(testClob.isDiscarded);

      // Query temporary LOBs, and don't discard them
      Object[] blobAndClob =
        awaitOne(Flux.from(connection.createStatement(
          "SELECT TO_BLOB(HEXTORAW('ABCDEF')), TO_CLOB('ABCDEF') FROM sys.dual")
          .execute())
          .flatMap(result ->
            result.map(row ->
              new Object[]{
                row.get(0, Blob.class),
                row.get(1, Clob.class)})));

      awaitExecution(connection.createStatement(
        "DROP TABLE testLobDiscard"));

      // Close the connection. It should not fail due to Oracle JDBC bug
      // #37160069.
      awaitNone(connection.close());
    }
    catch (Exception exception) {
      try {
        tryAwaitExecution(connection.createStatement(
          "DROP TABLE testLobDiscard"));
        tryAwaitNone(connection.close());
      }
      catch (Exception closeException) {
        exception.addSuppressed(closeException);
      }
      throw exception;
    }
  }

  /**
   * Verifies inserts and selects on NULL valued BLOBs and CLOBs
   */
  @Test
  public void testNullLob() {
    Connection connection = awaitOne(sharedConnection());
    try {
      awaitExecution(connection.createStatement(
        "CREATE TABLE testNullLob(blobValue BLOB, clobValue CLOB)"));

      awaitUpdate(1, connection.createStatement(
        "INSERT INTO testNullLob VALUES (?, ?)")
        .bindNull(0, Blob.class)
        .bindNull(1, Clob.class));

      awaitQuery(
        List.of(
          Arrays.asList(null, null)),
        row -> Arrays.asList(
          row.get(0, Blob.class),
          row.get(1, Clob.class)),
        connection.createStatement(
          "SELECT blobValue, clobValue FROM testNullLob"));

    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TABLE testNullLob"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies that the default LOB prefetch size is at least large enough to
   * fully prefetch 1MB of data.
   */
  @Test
  public void testDefaultLobPrefetch() throws Exception {
    Assumptions.assumeTrue(
      null == DatabaseConfig.protocol(), "Test requires TCP protocol");

    // A local server will monitor network I/O
    try (ServerSocketChannel localServer = ServerSocketChannel.open()) {
      localServer.configureBlocking(true);
      localServer.bind(null);

      class TestThread extends Thread {

        /** Count of bytes exchanged between JDBC and the database */
        int ioCount = 0;

        @Override
        public void run() {
          InetSocketAddress databaseAddress =
            new InetSocketAddress(DatabaseConfig.host(), DatabaseConfig.port());

          try (
            SocketChannel jdbcChannel = localServer.accept();
            SocketChannel databaseChannel =
              SocketChannel.open(databaseAddress)){

            jdbcChannel.configureBlocking(false);
            databaseChannel.configureBlocking(false);

            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(8192);
            while (true) {

              byteBuffer.clear();
              if (-1 == jdbcChannel.read(byteBuffer))
                break;
              byteBuffer.flip();
              ioCount += byteBuffer.remaining();

              while (byteBuffer.hasRemaining())
                databaseChannel.write(byteBuffer);

              byteBuffer.clear();
              databaseChannel.read(byteBuffer);
              byteBuffer.flip();
              ioCount += byteBuffer.remaining();

              while (byteBuffer.hasRemaining())
                jdbcChannel.write(byteBuffer);
            }
          }
          catch (Exception exception) {
            exception.printStackTrace();
          }
        }
      }

      TestThread testThread = new TestThread();
      testThread.start();


      int lobSize = 99 + (1024 * 1024); // <-- 99 + 1MB
      Connection connection = awaitOne(ConnectionFactories.get(
        DatabaseConfig.connectionFactoryOptions()
          .mutate()
          .option(HOST, "localhost")
          .option(PORT,
            ((InetSocketAddress)localServer.getLocalAddress()).getPort())
          .build())
        .create());
      try {
        awaitExecution(connection.createStatement(
          "CREATE TABLE testLobPrefetch ("
            + " id NUMBER GENERATED ALWAYS AS IDENTITY,"
            + " blobValue BLOB,"
            + " clobValue CLOB,"
            + " PRIMARY KEY(id))"));

        // Insert two rows of LOBs larger than 1MB
        byte[] bytes = new byte[lobSize];
        ByteBuffer blobValue = ByteBuffer.wrap(bytes);
        Arrays.fill(bytes, (byte)'a');
        String clobValue = new String(bytes, US_ASCII);
        awaitUpdate(List.of(1,1), connection.createStatement(
          "INSERT INTO testLobPrefetch (blobValue, clobValue)"
            + " VALUES (:blobValue, :clobValue)")
          .bind("blobValue", blobValue)
          .bind("clobValue", clobValue)
          .add()
          .bind("blobValue", blobValue)
          .bind("clobValue", clobValue));

        // Query two rows of LOBs larger than 1MB
        awaitQuery(
          List.of(
            List.of(blobValue, clobValue),
            List.of(blobValue, clobValue)),
          row -> {
            try {
              // Expect no I/O to result from mapping a fully prefetched BLOB or
              // CLOB:
              int ioCount = testThread.ioCount;
              var result = List.of(row.get("blobValue"), row.get("clobValue"));
              assertEquals(ioCount, testThread.ioCount);
              return result;
            }
            catch (Exception exception) {
              throw new RuntimeException(exception);
            }
          },
          connection.createStatement(
            "SELECT blobValue, clobValue FROM testLobPrefetch ORDER BY id")
            .fetchSize(1));
      }
      finally {
        tryAwaitExecution(connection.createStatement(
          "DROP TABLE testLobPrefetch"));
        tryAwaitNone(connection.close());
        testThread.join(10_000);
        testThread.interrupt();
      }
    }
  }

}
