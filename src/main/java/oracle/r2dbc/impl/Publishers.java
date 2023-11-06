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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Factory methods that create a {@code Publisher}. These methods cover special
 * cases which are not already supported by Project Reactor.
 */
class Publishers {

  private Publishers() {}

  /**
   * A publisher that immediately emits onNext and onComplete to subscribers
   */
  private static final Publisher<Object> COMPLETED_PUBLISHER =
    Mono.just(new Object());

  /**
   * <p>
   * Returns a publisher that emits the concatenated signals of a
   * {@code publisher} and {@code onTerminationPublisher}. If the
   * {@code onTerminationPublisher} emits an error, it will suppress any error
   * emitted by the first {@code publisher}. If a subscription to the returned
   * publisher is cancelled, the {@code onTerminationPublisher} is subscribed to
   * but it can not emit any error through the cancelled subscription.
   * </p><p>
   * The returned publisher behaves similarly to: <pre>{@code
   * Flux.concatDelayError(
   *   publisher,
   *   onTerminationPublisher)
   *   .doOnCancel(onTerminationPublisher::subscribe)
   * }</pre><p>
   * However, the code above can result in:
   * </p><pre>
   *   reactor.core.Exceptions$StaticThrowable: Operator has been terminated
   * </pre><p>
   * This seems to happen when the concatDelayError publisher receives a cancel
   * from a downstream subscriber after it has already received onComplete from
   * a upstream publisher.
   * </p>
   * @param publisher First publisher which is subscribed to.
   * @param onTerminationPublisher Publisher which is subscribed to when the
   * first publisher terminates, or a subcription is cancelled.
   * @return The concatenated publisher.
   * @param <T> Type of objects emitted to onNext
   */
  static <T> Publisher<T> concatTerminal(
    Publisher<T> publisher, Publisher<Void> onTerminationPublisher) {
    return Flux.usingWhen(
      COMPLETED_PUBLISHER,
      ignored -> publisher,
      ignored -> onTerminationPublisher);
  }
}
