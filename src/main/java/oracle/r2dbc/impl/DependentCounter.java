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

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * A count of resources that depend on another resource to remain open. A
 * dependent resource registers itself by incrementing the count, and
 * deregisters itself by decrementing the count. The last dependent to
 * deregister has the responsibility of subscribing to a {@code Publisher} that
 * closes the resource it depended upon.
 * </p><p>
 * This class is conceptually similar to a {@code java.util.concurrent.Phaser}.
 * Parties register by calling {@link #increment()}, and deregister by calling
 * {@link #decrement()}. Asynchronous "phase advancement" is then handled by
 * the {@code Publisher} which {@code decrement} returns.
 * </p><p>
 * This class offers a solution for tracking the consumption of
 * {@link io.r2dbc.spi.Result} objects that depend on a JDBC statement to remain
 * open until each result is consumed. Further explanations can be found in the
 * JavaDocs of {@link OracleStatementImpl} and {@link OracleResultImpl}.
 * </p>
 */
class DependentCounter {

  /** Count of dependents */
  private final AtomicInteger count = new AtomicInteger(0);

  /** Publisher that closes the depended upon resource */
  private final Publisher<Void> closePublisher;

  /**
   * Constructs a new counter that returns a resource closing publisher to the
   * last dependent which unregisters. The counter is initialized with a count
   * of zero.
   * @param closePublisher Publisher that closes a resource. Not null.
   */
  DependentCounter(Publisher<Void> closePublisher) {
    this.closePublisher = closePublisher;
  }

  /**
   * Increments the count of dependents by one.
   * <em>
   * A corresponding call to {@link #decrement()} MUST occur by the dependent
   * which has called {@code increment()}
   * </em>
   */
  void increment() {
    count.incrementAndGet();
  }

  /**
   * <p>
   * Returns a publisher that decrements the count of dependents by one when
   * subscribed to.
   * <em>
   * A corresponding call to {@link #increment()} MUST have previously occurred
   * by the dependent which has called {@code decrement()}
   * </em>
   * </p><p>
   * The dependent which has called this method MUST subscribe to the returned
   * published. If the dependent that calls this method is the last dependent to
   * do so, then the returned publisher will close the depended upon resource.
   * Otherwise, if more dependents remain, the returned publisher does nothing.
   * The caller of this method has no way to tell which is the case, so it must
   * subscribe to be safe.
   * </p>
   * @return A publisher that closes the depended upon resource after no
   * dependents remain. Not null.
   */
  Publisher<Void> decrement() {
    return Mono.defer(() ->
      count.decrementAndGet() == 0
        ? Mono.from(closePublisher)
        : Mono.empty());
  }

}
