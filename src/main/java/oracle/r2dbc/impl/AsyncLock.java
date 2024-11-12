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
package oracle.r2dbc.impl;

import oracle.r2dbc.impl.OracleR2dbcExceptions.JdbcRunnable;
import oracle.r2dbc.impl.OracleR2dbcExceptions.JdbcSupplier;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * <p>
 * A lock that is acquired and unlocked asynchronously. An instance of this lock
 * is used to guard access to the Oracle JDBC Connection, without blocking
 * threads that contend for it.
 * </p><p><i>
 * Since the 23.1 release, Oracle JDBC no longer blocks threads during
 * asynchronous calls. The remainder of this JavaDoc describes behavior which
 * was present in 21.x releases of Oracle JDBC. If a 23.1 or newer version
 * of Oracle JDBC is installed, then Oracle R2DBC will use the
 * {@link NoOpAsyncLock} rather than {@link AsyncLockImpl}.
 * </i></p><p>
 * Any time Oracle R2DBC invokes a synchronous API of Oracle JDBC, it will
 * acquire an instance of this lock before doing so. Synchronous method calls
 * will block a thread if JDBC has a database call in progress, and this can
 * lead a starvation of the thread pool. If no pooled threads are available,
 * then JDBC can not execute callbacks that complete the database call, and
 * so JDBC will never release it's blocking lock.
 * </p><p>
 * Any time Oracle R2DBC creates a publisher that is implemented by the
 * Oracle JDBC driver, it will acquire an instance of this lock before
 * subscribing to that publisher. The lock is only released if it is known
 * that neither onNext nor onSubscribe signals are pending. If these signals
 * are not pending, then the driver should not be executing any database
 * call; As long as no database call is in progress, JDBC should not block
 * threads that call any method of its API.
 * </p>
 * <h3>Locking for Asynchronous JDBC Calls</h3>
 * <p>
 * Wrapping a JDBC Publisher with {@link #lock(Publisher)} will have signals
 * from a downstream subscriber proxied, such that the lock is held whenever
 * {@code onSubscribe} or {@code onNext} signals are pending.
 * </p><p>
 * Invoking {@link #lock(Runnable)} will have a {@code Runnable} executed
 * after the lock becomes available. The {@code Runnable} is executed
 * immediately, before {@code lock(Runnable)} returns if the lock is
 * available. Otherwise, the {@code Runnable} is executed asynchronously
 * after the lock becomes available.
 * </p>
 * <h3>Locking for Synchronous JDBC Calls</h3>
 * <p>
 * If the lock simply needs to be acquired before making a synchronous call,
 * and then released after that call returns, then methods
 * {@link #run(JdbcRunnable)}, {@link #get(JdbcSupplier)}, or
 * {@link #flatMap(JdbcSupplier)} may be used. These methods return a
 * {@code Publisher} that completes after the lock is acquired and the provided
 * task has been run. These methods will automatically release the lock after
 * the provided task has run.
 * </p><p>
 * Rather than invoke the {@code get/run} methods for each and every JDBC
 * method call, it is preferable to invoke them with a single task that
 * performs many synchronous JDBC API calls. This will reduce the computational
 * and memory costs of creating and subscribing to publishers. It will also
 * allow most of the code base to be written in a synchronous style, with the
 * assumption that it is executing within a task provided to the {@code get/run}
 * methods.
 * </p>
 */
interface AsyncLock {

  /**
   * Acquires this lock, executes a {@code callback}, and then releases this
   * lock. The {@code callback} may be executed before this method returns if
   * this lock is currently available. Otherwise, the {@code callback} is
   * executed asynchronously after this lock becomes available.
   *
   * @param callback Task to be executed with lock ownership. Not null.
   */
  void lock(Runnable callback);

  /**
   * Returns a {@code Publisher} that acquires this lock and executes a
   * {@code jdbcRunnable} when a subscriber subscribes. The {@code Publisher}
   * emits {@code onComplete} if the runnable completes normally, or emits
   * {@code onError} if the runnable throws an exception.
   *
   * @param jdbcRunnable Runnable to execute. Not null.
   * @return A publisher that emits the result of the {@code jdbcRunnable}.
   */
  Publisher<Void> run(JdbcRunnable jdbcRunnable);

  /**
   * Returns a {@code Publisher} that acquires this lock and executes a
   * {@code jdbcSupplier} when a subscriber subscribes. The {@code Publisher}
   * emits {@code onNext} if the supplier returns a non-null value, and then
   * emits {@code onComplete}. Or, the {@code Publisher} emits {@code onError}
   * with any {@code Throwable} thrown by the supplier.
   *
   * @param <T> The class of object output by the {@code jdbcSupplier}
   * @param jdbcSupplier Supplier to execute. Not null.
   * @return A publisher that emits the result of the {@code jdbcSupplier}.
   */
  <T> Publisher<T> get(JdbcSupplier<T> jdbcSupplier);

  /**
   * Returns a {@code Publisher} that acquires this lock and executes a
   * {@code publisherSupplier} when a subscriber subscribes. The
   * {@code Publisher} output by the {@code publisherSupplier} is flat mapped
   * into the {@code Publisher} returned by this method. If the supplier outputs
   * {@code null}, the returned publisher just emits {@code onComplete}. If the
   * supplier throws an error, the returned publisher emits that as
   * {@code onError}.
   *
   * @param <T> The class of object emitted by the supplied publisher
   * @param publisherSupplier Supplier to execute. Not null.
   * @return A flat-mapping of the publisher output by the 
   * {@code publisherSupplier}.
   */
  <T> Publisher<T> flatMap(JdbcSupplier<Publisher<T>> publisherSupplier);

  /**
   * Returns a {@code Publisher} that proxies signals to and from a
   * provided {@code publisher} in order to guard access to the JDBC
   * {@code Connection} associated with this adapter. Invocations of
   * {@link Publisher#subscribe(Subscriber)} and
   * {@link Subscription#request(long)} will only occur when the JDBC connection
   * is not being used by another thread or another publisher.
   *
   * @param <T> The class of object emitted by the {@code publisher}
   * @param publisher A publisher that uses the JDBC connection
   * @return A Publisher that
   */
  <T> Publisher<T> lock(Publisher<T> publisher);

}
