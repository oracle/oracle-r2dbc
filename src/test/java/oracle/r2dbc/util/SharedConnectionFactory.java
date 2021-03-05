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

package oracle.r2dbc.util;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 * A {@link ConnectionFactory} which caches a single connection that is
 * shared by each {@code Publisher} returned by {@link #create()}. This
 * factory is used by test cases in order to eliminate the latency of opening a
 * new database connection. Rather than open a new connection for each test,
 * many tests can instead share a single connection.
 * </p><p>
 * This class implements a queue in which callers of {@link #create()} wait
 * to borrow the shared connection. A call to {@code create()} returns a
 * {@link Publisher} that emits the borrowed connection after the previous
 * borrower has returned it. A borrowed connection is returned when it's
 * {@link Connection#close()} method is called.
 * </p>
 */
public class SharedConnectionFactory implements ConnectionFactory {

  /** Metadata of the factory which created the shared connection */
  private final ConnectionFactoryMetadata metadata;

  /**
   * The tail of the borrowers queue. The current borrower completes the
   * current tail when {@link Connection#close()} is invoked on the borrowed
   * connection.
   */
  private final AtomicReference<CompletionStage<Connection>> borrowQueueTail;

  /**
   * Constructs a new connection factory that caches the connection emitted
   * by the {@code connectionPublisher}. The shared connection is emitted by the
   * publisher returned from {@link #create()}.
   *
   * @param connectionPublisher
   * @param metadata
   */
  public SharedConnectionFactory(
    Publisher<? extends Connection> connectionPublisher,
    ConnectionFactoryMetadata metadata) {
    CompletableFuture<Connection> initialTail = new CompletableFuture<>();
    Mono.from(connectionPublisher)
      .doOnNext(initialTail::complete)
      .doOnError(initialTail::completeExceptionally)
      .subscribe();

    borrowQueueTail = new AtomicReference<>(initialTail);
    this.metadata = metadata;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Borrows the shared connection. The returned publisher emits the
   * borrowed {@code Connection} once the the previous borrower has returned it.
   * </p><p>
   * The caller of this method claims the current the tail of the borrowers
   * queue, and leaves a new tail that the next borrower can claim. The caller
   * attaches a callback to their claimed tail. The caller's callback is invoked
   * when the previous borrower returns the connection.
   * </p><p>
   * Invocation of the caller's callback has the returned publisher emit the
   * connection, which is wrapped as a {@link BorrowedConnection}. When
   * {@link BorrowedConnection#close()} will invoke the callback attached to
   * the new tail that the next borrower claims.
   * </p><p>
   * This method can safely be called by multiple threads in parallel.
   * </p>
   * @return
   */
  @Override
  public Publisher<? extends Connection> create() {
    CompletableFuture<Connection> nextTail = new CompletableFuture<>();
    CompletionStage<Connection> currentTail =
      borrowQueueTail.getAndSet(nextTail);

    return Mono.fromCompletionStage(
      currentTail.thenApply(connection ->
        new BorrowedConnection(connection, nextTail)));
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return metadata;
  }

  /**
   * Wraps a {@link Connection} in order to intercept calls to
   * {@link Connection#close()}. A call to {@code close()} will reset
   * the {@link #connection} to the original state it was in when
   * {@link ConnectionFactory#create()} created it. After the state is reset,
   * the {@link #returnedFuture} is completed to indicate that the connection is
   * ready for the next borrower to use.
   */
  private final class BorrowedConnection implements Connection {

    /** Borrowed connection that this connection delegates to */
    private final Connection connection;

    /** Future completed when the borrowed connection is returned */
    private final CompletableFuture<Connection> returnedFuture;

    /** Set to true when this wrapper is closed */
    private volatile boolean isClosed = false;
    private IsolationLevel initialIsolationLevel;

    private BorrowedConnection(
      Connection connection,
      CompletableFuture<Connection> returnedFuture) {
      this.connection = connection;
      this.returnedFuture = returnedFuture;
      this.initialIsolationLevel = connection.getTransactionIsolationLevel();
    }

    /**
     * Resets the connection's auto-commit state, setting it to {@code true}
     * as it was when {@link ConnectionFactory#create()} created the
     * connection.
     * @return
     */
    @Override
    public Publisher<Void> close() {
      if (isClosed)
        return Mono.empty();

      if (connection.isAutoCommit()) {
        returnedFuture.complete(connection);
      }
      else {
        Mono.from(connection.rollbackTransaction())
          .concatWith(connection.setAutoCommit(true))
          .concatWith(connection.setTransactionIsolationLevel(
            initialIsolationLevel))
          .doOnTerminate(() -> returnedFuture.complete(connection))
          .subscribe();
      }

      return Mono.empty();
    }

    @Override
    public Publisher<Void> beginTransaction() {
      requireNotClosed();
      return connection.beginTransaction();
    }

    @Override
    public Publisher<Void> commitTransaction() {
      requireNotClosed();
      return connection.commitTransaction();
    }

    @Override
    public Batch createBatch() {
      requireNotClosed();
      return connection.createBatch();
    }

    @Override
    public Publisher<Void> createSavepoint(String name) {
      requireNotClosed();
      return connection.createSavepoint(name);
    }

    @Override
    public Statement createStatement(String sql) {
      requireNotClosed();
      return connection.createStatement(sql);
    }

    @Override
    public boolean isAutoCommit() {
      requireNotClosed();
      return connection.isAutoCommit();
    }

    @Override
    public ConnectionMetadata getMetadata() {
      requireNotClosed();
      return connection.getMetadata();
    }

    @Override
    public IsolationLevel getTransactionIsolationLevel() {
      requireNotClosed();
      return connection.getTransactionIsolationLevel();
    }

    @Override
    public Publisher<Void> releaseSavepoint(String name) {
      requireNotClosed();
      return connection.releaseSavepoint(name);
    }

    @Override
    public Publisher<Void> rollbackTransaction() {
      requireNotClosed();
      return connection.rollbackTransaction();
    }

    @Override
    public Publisher<Void> rollbackTransactionToSavepoint(String name) {
      requireNotClosed();
      return connection.rollbackTransactionToSavepoint(name);
    }

    @Override
    public Publisher<Void> setAutoCommit(boolean autoCommit) {
      requireNotClosed();
      return connection.setAutoCommit(autoCommit);
    }

    @Override
    public Publisher<Void> setTransactionIsolationLevel(
      IsolationLevel isolationLevel) {
      requireNotClosed();
      return connection.setTransactionIsolationLevel(isolationLevel);
    }

    @Override
    public Publisher<Boolean> validate(ValidationDepth depth) {
      requireNotClosed();
      return connection.validate(depth);
    }

    private void requireNotClosed() {
      if (isClosed)
        throw new IllegalStateException("Connection is closed");
    }
  }
}
