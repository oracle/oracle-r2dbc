package oracle.r2dbc.impl;

import oracle.r2dbc.impl.OracleR2dbcExceptions.JdbcRunnable;
import oracle.r2dbc.impl.OracleR2dbcExceptions.JdbcSupplier;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * A lock that is acquired asynchronously. Acquiring threads invoke {@link
 * #lock(Runnable)} with a {@code Runnable} that will access a guarded resource.
 * The {@code Runnable} <i>MUST</i> ensure that a single invocation of {@link
 * #unlock()} will occur after its {@code run()} method has been invoked. The
 * call to {@code unlock} may occur asynchronously on a thread other than the
 * one invoking {@code run}.
 * </p><p>
 * An instance of this lock is used to guard access to the Oracle JDBC
 * Connection, without blocking threads that contend for it.
 * </p>
 */
final class AsyncLock {

  /**
   * Count that is incremented for invocation of {@link #lock(Runnable)}, and is
   * decremented by each invocation of {@link #unlock()}. This lock is unlocked
   * when the count is 0.
   */
  private final AtomicInteger waitCount = new AtomicInteger(0);

  /**
   * Dequeue of {@code Runnable} callbacks enqueued each time an invocation of
   * {@link #lock(Runnable)} is not able to acquire this lock. The head of this
   * dequeue is dequeued and executed by an invocation of {@link #unlock()}.
   */
  private final ConcurrentLinkedDeque<Runnable> waitQueue =
    new ConcurrentLinkedDeque<>();

  /**
   * Returns a {@code Publisher} that emits {@code onComplete} when this lock is
   * acquired.
   */
  void lock(Runnable callback) {
    assert waitCount.get() >= 0 : "Wait count is less than 0: " + waitCount;

    // Acquire this lock and invoke the callback immediately, if possible
    if (waitCount.compareAndSet(0, 1)) {
      callback.run();
    }
    else {
      // Enqueue the callback to be invoked asynchronously when this
      // lock is unlocked
      waitQueue.addLast(callback);

      // Another thread may have unlocked this lock while this thread was
      // enqueueing the callback. Dequeue and execute the head of the deque
      // if this is the case.
      if (0 == waitCount.getAndIncrement())
        waitQueue.removeFirst().run();
    }
  }

  void unlock() {
    assert waitCount.get() > 0 : "Wait count is less than 1: " + waitCount;

    // Decrement the count. Assuming that lock was called before this
    // method, the count is guaranteed to be 1 or greater. If it greater
    // than 1 after being decremented, then another invocation of lock has
    // enqueued a callback
    if (0 != waitCount.decrementAndGet())
      waitQueue.removeFirst().run();
  }

  /**
   * Returns a {@code Publisher} that acquires this lock and executes a
   * {@code jdbcRunnable} when a subscriber subscribes. The {@code Publisher}
   * emits {@code onComplete} if the runnable completes normally, or emits
   * {@code onError} if the runnable throws an exception.
   * @param jdbcRunnable Runnable to execute. Not null.
   * @return A publisher that emits the result of the {@code jdbcRunnable}.
   */
  Publisher<Void> run(JdbcRunnable jdbcRunnable) {
    return Mono.create(monoSink ->
      lock(() -> {
        try {
          jdbcRunnable.runOrThrow();
          monoSink.success();
        }
        catch (Throwable throwable) {
          monoSink.error(throwable);
        }
        finally {
          unlock();
        }
      }));
  }

  /**
   * Returns a {@code Publisher} that acquires this lock and executes a
   * {@code jdbcSupplier} when a subscriber subscribes. The {@code Publisher}
   * emits {@code onNext} and {@code onComplete} if the runnable completes
   * normally, or emits {@code onError} if the runnable throws an exception.
   * The {@code onNext} signal emits the output of the {@code jdbcSupplier}.
   * @param jdbcSupplier Supplier to execute. Not null.
   * @return A publisher that emits the result of the {@code jdbcSupplier}.
   */
  <T> Publisher<T> get(JdbcSupplier<T> jdbcSupplier) {
    return Mono.create(monoSink ->
      lock(() -> {
        try {
          monoSink.success(jdbcSupplier.getOrThrow());
        }
        catch (Throwable throwable) {
          monoSink.error(throwable);
        }
        finally {
          unlock();
        }
      }));
  }

}
