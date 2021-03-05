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
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
import oracle.r2dbc.DatabaseConfig;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Function;

import static oracle.r2dbc.DatabaseConfig.sqlTimeout;
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
    return Flux.from(singlePublisher).single().cache().block(sqlTimeout());
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
   * Executes a {@code statement} and blocks until the execution
   * completes. This method verifies that the execution produces a
   * {@link Result} with no count of updated rows.
   * @param statement A statement that does not update rows.
   * @throws Throwable If the statement execution results in an error.
   */
  public static void awaitExecution(Statement statement) {
    assertNull(
      Mono.from(statement.execute())
        .flatMap(result -> Mono.from(result.getRowsUpdated()))
        .block(sqlTimeout()),
      "Expected no update count when not updating rows");
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
    List<T> expectedRows, Function<Row, T> rowMapper, Statement statement) {
    assertEquals(
      expectedRows,
      Flux.from(statement.execute())
        .concatMap(result ->
          Flux.from(result.map((row, metadata) -> rowMapper.apply(row))))
        .collectList()
        .block(sqlTimeout()),
      "Unexpected row data");
  }

}
