package oracle.r2dbc.impl;

import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Counts the number of results that depend on a statement. When the count
 * reaches zero, the statement is closed.
 */
class DependentResults {


  private final AtomicInteger count = new AtomicInteger(0);

  private final Publisher<Void> closePublisher;

  DependentResults(Publisher<Void> closePublisher) {
    this.closePublisher = closePublisher;
  }

  /**
   * Add a dependent result. The statement will not be closed until all results
   * are removed.
   * @param result
   */
  void increment() {
    count.incrementAndGet();
  }

  /**
   * Removes a dependent result. This method closes the statement if all results
   * have been removed.
   *
   * @return A publisher that completes after closing the statement if the last
   *   result has been removed. If more results remain, the returned publisher
   *   does nothing and emits onComplete.
   */
  Publisher<Void> decrement() {
    return Mono.defer(() ->
      count.decrementAndGet() == 0
        ? Mono.from(closePublisher)
        : Mono.empty());
  }
}
