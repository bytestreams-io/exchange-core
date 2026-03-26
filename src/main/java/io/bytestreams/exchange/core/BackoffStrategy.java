package io.bytestreams.exchange.core;

/**
 * Strategy for computing the delay between reconnect attempts.
 *
 * <p>Implementations receive a 1-based attempt number and return the delay in nanoseconds before
 * the next attempt. Return 0 for no delay.
 *
 * @see ExponentialBackoff
 */
@FunctionalInterface
public interface BackoffStrategy {
  /**
   * Computes the delay before the given reconnect attempt.
   *
   * @param attempt the 1-based attempt number
   * @return delay in nanoseconds (0 or positive)
   */
  long delayNanos(int attempt);
}
