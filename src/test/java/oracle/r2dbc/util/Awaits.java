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

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import oracle.r2dbc.test.DatabaseConfig;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static oracle.r2dbc.test.DatabaseConfig.sqlTimeout;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * This class implements methods that await, or block, until an asynchronous
 * operation has completed. Test cases use these methods to execute asynchronous
 * operations in a consistent order. The maximum time that a method will spend
 * blocked is configured by {@link DatabaseConfig#sqlTimeout()}.
 */
public final class Awaits {

  private Awaits() {/* This class only defines static methods*/}

  /**
   * <p>
   * Subscribes to an {@code emptyPublisher} and tries to block until the
   * publisher emits {@code onComplete}. This method verifies that the
   * publisher does not emit {@code onNext}. If {@code emptyPublisher} emits
   * {@code onError}, this method invokes {@link Throwable#printStackTrace()}
   * on the error and then returns normally
   * <p>
   * This method is useful in the scope of a {@code finally} block, where a
   * throwing an exception will obtrude the processing of any {@code Throwable}
   * thrown from the {@code try} block, like this:
   * </p><pre>
   *   try {
   *     throw new RuntimeException("Try Block Throws");
   *   }
   *   finally {
   *     throw new RuntimeException("Finally Block Throws");
   *   }
   * </pre><p>
   * If the code above is executed, only the finally block's RuntimeException
   * will be thrown. When a test cases fails within a {@code try} block, it
   * throws an {@link AssertionError} with useful information; That
   * information is lost if the {@code finally} block throws as well.
   * </p>
   * @param emptyPublisher A publisher that emits no values.
   */
  public static void tryAwaitNone(Publisher<?> emptyPublisher) {
    try {
      awaitNone(emptyPublisher);
    }
    catch (Throwable throwable) {
      throwable.printStackTrace();
    }
  }

  /**
   * Subscribes to an {@code emptyPublisher} and blocks until the publisher
   * emits {@code onComplete}. This method verifies that the publisher does
   * not emit {@code onNext}.
   * @param emptyPublisher A publisher that emits no values.
   * @throws Throwable If the publisher emits {@code onError}.
   */
  public static void awaitNone(Publisher<?> emptyPublisher) {
    assertNull(
      Mono.from(emptyPublisher).block(sqlTimeout()),
      "Unexpected onNext signal from Publisher of no values");
  }

  /**
   * Subscribes to an {@code errorPublisher} and blocks until the publisher
   * emits {@code onError} with an {@code errorType}. This method verifies that
   * the publisher does not emit {@code onComplete}.
   * @param errorPublisher A publisher that emits an error.
   * @throws Throwable If the publisher emits {@code onError} with a
   * {@code Throwable} that is not an instance of {@code errorType}.
   */
  public static void awaitError(
    Class<? extends Throwable> errorType, Publisher<?> errorPublisher) {
    assertThrows(
      errorType,
      () -> Mono.from(errorPublisher).block(sqlTimeout()),
      "Unexpected signal from Publisher of an error");
  }

  /**
   * Subscribes to an {@code singlePublisher} and blocks until the publisher
   * emits {@code onNext} and then {@code onComplete}. This method verifies
   * that the publisher does not more than one {@code onNext} signal with the
   * {@code expectedValue}.
   * @param expectedValue Value that the publisher emits
   * @param singlePublisher A publisher that emits one value.
   * @throws Throwable If the publisher emits {@code onError} or does not
   * emit one {@code onNext} signal.
   */
  public static <T> void awaitOne(T expectedValue, Publisher<T> singlePublisher) {
    assertEquals(expectedValue, awaitOne(singlePublisher),
      "Unexpected onNext signal from Publisher of a single value");
  }

  /**
   * Subscribes to an {@code singlePublisher} and blocks until the publisher
   * emits {@code onNext} and then {@code onComplete}.
   * @param singlePublisher A publisher that emits one value.
   * @return An item emitted with {@code onNext}, or null if the publisher
   * emits no item.
   * @throws Throwable If the publisher emits {@code onError} or does not
   * emit one {@code onNext} signal.
   */
  public static <T> T awaitOne(Publisher<T> singlePublisher) {
    AtomicReference<T> result = new AtomicReference<>(null);
    consumeOne(singlePublisher, result::set);
    return result.get();
  }

  /**
   * Subscribes to an {@code singlePublisher} and blocks until the publisher
   * emits {@code onNext} and then {@code onComplete}. Values emitted to
   * {@code onNext} are input to a {@code consumer}. This is for cases where
   * a {@code Publisher} does not emit {@code onNext} or {@code onComplete}
   * until a previous value has been consumed, which is the case for
   * {@code OracleStatementImpl.execute()}.
   * @param singlePublisher A publisher that emits one value.
   * @return An item emitted with {@code onNext}, or null if the publisher
   * emits no item.
   * @throws Throwable If the publisher emits {@code onError} or does not
   * emit one {@code onNext} signal.
   */
  public static <T> void consumeOne(
    Publisher<T> singlePublisher, Consumer<T> consumer) {
    assertEquals(1, Flux.from(singlePublisher)
      .doOnNext(consumer)
      .collectList()
      .block(sqlTimeout())
      .size());
  }

  /**
   * Subscribes to an {@code multiPublisher} and blocks until the publisher
   * emits 0 or more {@code onNext} signals and then {@code onComplete}. This
   * method verifies that the publisher emits {@code onNext} signals with the
   * same values in the same order as the list of {@code expectedValues}.
   * @param expectedValues Values that the publisher emits
   * @param multiPublisher A publisher that emits 0 or more values.
   * @throws Throwable If the publisher emits {@code onError}.
   */
  public static <T> void awaitMany(
    List<T> expectedValues, Publisher<T> multiPublisher) {
    assertEquals(expectedValues, awaitMany(multiPublisher),
      "Unexpected onNext signals from Publisher of multiple values");
  }

  /**
   * Subscribes to an {@code multiPublisher} and blocks until the publisher
   * emits 0 or more {@code onNext} signals and then {@code onComplete}.
   * @param multiPublisher A publisher that emits 0 or more values.
   * @return A list of items emitted with {@code onNext}.
   * @throws Throwable If the publisher emits {@code onError}.
   */
  public static <T> List<T> awaitMany(Publisher<T> multiPublisher) {
    return Flux.from(multiPublisher).collectList().block(sqlTimeout());
  }

  /**
   * <p>
   * Executes a {@code statement} and tries to blocks until the execution
   * completes. This method verifies that the execution produces a
   * {@link Result} with a count of zero updated rows. If
   * {@link Statement#execute()} emits {@code onError}, this method invokes
   * {@link Throwable#printStackTrace()} on the error and then returns normally
   * <p>
   * This method is useful in the scope of a {@code finally} block, where a
   * throwing an exception will obtrude the processing of any {@code Throwable}
   * thrown from the {@code try} block, like this:
   * </p><pre>
   *   try {
   *     throw new RuntimeException("Try Block Throws");
   *   }
   *   finally {
   *     throw new RuntimeException("Finally Block Throws");
   *   }
   * </pre><p>
   * If the code above is executed, only the finally block's RuntimeException
   * will be thrown. When a test cases fails within a {@code try} block, it
   * throws an {@link AssertionError} with useful information; That
   * information is lost if the {@code finally} block throws as well.
   * </p>
   * @param statement A statement that updates zero rows.
   */
  public static void tryAwaitExecution(Statement statement) {
    try {
      awaitExecution(statement);
    }
    catch (Throwable throwable) {
      throwable.printStackTrace();
    }
  }

  /**
   * Executes a {@code statement} and blocks until the execution
   * completes. This method verifies that the execution produces a
   * {@link Result} with a count of zero updated rows.
   * @param statement A statement that updates zero rows.
   * @throws Throwable If the statement execution results in an error.
   */
  public static void awaitExecution(Statement statement) {
    awaitNone(Flux.from(statement.execute())
      .flatMap(Result::getRowsUpdated)
      // Don't emit an update count of 0, which is the expected return value of
      // Oracle JDBC's implementation of Statement.getUpdateCount() after
      // executing a DDL statement. Oracle R2DBC relies on getUpdateCount()
      // to determine the value emitted by getRowsUpdated.
      .filter(updateCount -> updateCount != 0));
  }

  /**
   * Executes a {@code statement} and blocks until the execution
   * completes. This method verifies that the execution produces a
   * {@link Result} with an update count that matches an {@code expectedCount}.
   * @param expectedCount Expected count of updated rows
   * @param statement A statement that updates rows.
   * @throws Throwable If the statement execution results in an error.
   */
  public static void awaitUpdate(int expectedCount, Statement statement) {
    awaitUpdate(List.of(expectedCount), statement);
  }

  /**
   * Executes a {@code statement} and blocks until the execution
   * completes. This method verifies that the execution produces a
   * {@link Result} with update counts that match a list of
   * {@code expectedCounts}.
   * @param expectedCounts Expected counts of updated rows
   * @param statement A statement that updates rows.
   * @throws Throwable If the statement execution results in an error.
   */
  public static void awaitUpdate(
    List<Integer> expectedCounts, Statement statement) {
    assertEquals(
      expectedCounts,
      Flux.from(statement.execute())
        .flatMap(result -> Flux.from(result.getRowsUpdated()))
        .map(Math::toIntExact)
        .collectList()
        .block(sqlTimeout()),
      "Unexpected update counts");
  }

  /**
   * Executes a {@code statement} and blocks until the execution
   * completes. This method verifies that the execution produces a
   * {@link Result} with row data that matches a sequence of
   * {@code expectedRows}.
   * @param expectedRows List of expected row data.
   * @param rowMapper Maps {@code Rows} to objects that are expected to match
   *                  the {@code expectedRows}.
   * @param statement A statement that queries expectedRows.
   * @throws Throwable If the statement execution results in an error.
   */
  public static <T> void awaitQuery(
    List<T> expectedRows, Function<io.r2dbc.spi.Readable, T> rowMapper,
    Statement statement) {
    assertEquals(
      expectedRows,
      Flux.from(statement.execute())
        .concatMap(result -> Flux.from(result.map(rowMapper)))
        .collectList()
        .block(sqlTimeout()),
      "Unexpected row data");
  }

}
