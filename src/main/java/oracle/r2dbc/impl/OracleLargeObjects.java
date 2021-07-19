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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Factory for implementations of the R2DBC {@link Blob} and {@link Clob}
 * SPIs. The Oracle R2DBC Driver uses this factory to produce column mappings
 * for the BLOB and CLOB database types.
 * @since 0.1.0
 * @author michael-a-mcmahon
 */
final class OracleLargeObjects {

  /**
   * This class has no instance methods, so this constructor should never be
   * called.
   */
  private OracleLargeObjects(){ }

  /**
   * Creates a new {@code Blob} that streams binary data from a
   * {@code contentPublisher} and subscribes to a {@code releasePublisher},
   * when the {@code Blob} is discarded. The {@code contentPublisher} is
   * subscribed to one or zero times. The {@code releasePublisher} must
   * support multiple subscribers, and must publish the same result to each
   * subscriber.
   * @param contentPublisher Publishes the contents of a BLOB
   * @param releasePublisher Publishes the result of releasing a BLOB.
   * @return A new {@code Blob}
   */
  static Blob createBlob(
    Publisher<ByteBuffer> contentPublisher, Publisher<Void> releasePublisher) {

    Publisher<ByteBuffer> streamPublisher =
      createStreamPublisher(contentPublisher, releasePublisher);

    return new Blob() {
      @Override
      public Publisher<ByteBuffer> stream() {
        return streamPublisher;
      }

      @Override
      public Publisher<Void> discard() {
        return releasePublisher;
      }
    };
  }

  /**
   * Creates a new {@code Clob} that streams character data from a
   * {@code contentPublisher} and subscribes to a {@code releasePublisher},
   * when the {@code Clob} is discarded. The {@code contentPublisher} is
   * subscribed to one or zero times. The {@code releasePublisher} must
   * support multiple subscribers, and must publish the same result to each
   * subscriber.
   * @param contentPublisher Publishes the contents of a CLOB
   * @param releasePublisher Publishes the result of releasing a CLOB.
   * @return A new {@code Clob}
   */
  static Clob createClob(
    Publisher<? extends CharSequence> contentPublisher,
    Publisher<Void> releasePublisher) {

    Publisher<CharSequence> streamPublisher =
      createStreamPublisher(contentPublisher, releasePublisher);

    return new Clob() {
      @Override
      public Publisher<CharSequence> stream() {
        return streamPublisher;
      }

      @Override
      public Publisher<Void> discard() {
        return releasePublisher;
      }
    };
  }

  /**
   * <p>
   * Returns a publisher of LOB content that implements the behavior specified
   * for {@link Blob#stream()} and {@link Clob#stream()} R2DBC SPIs.
   * </p><p>
   * This publisher will publish LOB content to the first subscriber that
   * subscribes, and will throw an {@link IllegalStateException} if
   * subscribed to more than once.
   * </p><p>
   * If a subscription to this publisher is cancelled, this publisher will
   * initiate the release of LOB resources.
   * </p><p>
   * If LOB content publishing terminates with an {@code onComplete} or
   * {@code onError}, this publisher will initiate the release of LOB resources.
   * This behavior is specified in the R2DBC 0.8.1 Specification,
   * Section 12.2.5:
   * <pre>
   * Applications may release Blob and Clob by either consuming the
   * content stream or disposing of resources by calling the discard()
   * method.
   * </pre>
   * </p><p>
   * When, upon it's termination, the returned publisher subscribes to the
   * {@code releasePublisher}, the result emitted by the release publisher is
   * not emitted to the subscriber of the returned publisher. To process signals
   * that result from releasing the LOB, the {@code releasePublisher} must
   * emit the same result to subscribers of {@link Blob#discard()} or
   * {@link Clob#discard()}.
   * </p>
   * @param contentPublisher Publishes a LOB's contents
   * @param releasePublisher Publishes the result of releasing a LOB.
   * @param <T> The type of published content
   * @return A LOB content publisher that releases resources upon it's
   * termination.
   */
  private static <T> Publisher<T> createStreamPublisher(
    Publisher<? extends T> contentPublisher, Publisher<Void> releasePublisher) {

    AtomicBoolean isSubscribed = new AtomicBoolean(false);

    return subscriber -> {
      Objects.requireNonNull(subscriber, "Subscriber is null");

      if (isSubscribed.compareAndSet(false, true)) {
        Flux.from(contentPublisher)
          // Call to free the LOB should happen *after* the LOB content
          // publisher terminates, so that calls to Blob/Clob.freeAsyncOracle
          // do not block.
          .doFinally(signalType -> Mono.from(releasePublisher).subscribe())
          .subscribe(subscriber);
      }
      else {
        throw new IllegalStateException(
          "A content stream can not be consumed more than once");
      }
    };
  }
}