package io.bytestreams.exchange.core;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Strategy for computing the delay between retry attempts.
 *
 * <p>Strategies are composable via decorator methods:
 *
 * <pre>{@code
 * BackoffStrategy strategy = BackoffStrategy.exponential(Duration.ofMillis(100))
 *     .withMax(Duration.ofSeconds(30))
 *     .withJitter(0.5);
 * }</pre>
 */
@FunctionalInterface
public interface BackoffStrategy {
  /**
   * Computes the delay before the given attempt.
   *
   * @param attempt the 1-based attempt number
   * @return the delay (must not be negative)
   */
  Duration delay(int attempt);

  /**
   * Returns a strategy that always returns the same delay.
   *
   * @param delay the fixed delay
   * @return a fixed-delay backoff strategy
   */
  static BackoffStrategy fixed(Duration delay) {
    Objects.requireNonNull(delay, "delay");
    return attempt -> delay;
  }

  /**
   * Returns a strategy that doubles the base delay on each attempt: {@code baseDelay * 2^(attempt -
   * 1)}.
   *
   * @param baseDelay the delay for the first attempt
   * @return an exponential backoff strategy
   */
  static BackoffStrategy exponential(Duration baseDelay) {
    Objects.requireNonNull(baseDelay, "baseDelay");
    long baseNanos = baseDelay.toNanos();
    return attempt -> {
      int shift = attempt - 1;
      long nanos;
      // baseNanos << shift overflows long well before shift reaches 63
      if (shift >= Long.SIZE - 1 || (shift > 0 && baseNanos > (Long.MAX_VALUE >> shift))) {
        nanos = Long.MAX_VALUE;
      } else {
        nanos = baseNanos << shift;
      }
      return Duration.ofNanos(nanos);
    };
  }

  /**
   * Wraps this strategy, capping the delay at the given maximum.
   *
   * @param max the maximum delay
   * @return a capped backoff strategy
   */
  default BackoffStrategy withMax(Duration max) {
    Objects.requireNonNull(max, "max");
    long maxNanos = max.toNanos();
    return attempt -> {
      Duration delay = this.delay(attempt);
      return delay.toNanos() > maxNanos ? max : delay;
    };
  }

  /**
   * Wraps this strategy, adding random jitter up to {@code factor * delay}.
   *
   * @param factor the jitter factor (0.0 = no jitter, 1.0 = up to 100% of the delay)
   * @return a jittered backoff strategy
   */
  default BackoffStrategy withJitter(double factor) {
    if (factor < 0.0 || factor > 1.0) {
      throw new IllegalArgumentException("jitter factor must be between 0.0 and 1.0");
    }
    return attempt -> {
      Duration delay = this.delay(attempt);
      long jitterNanos =
          (long) (delay.toNanos() * factor * ThreadLocalRandom.current().nextDouble());
      return delay.plusNanos(jitterNanos);
    };
  }
}
