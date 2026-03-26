package io.bytestreams.exchange.core;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with optional jitter.
 *
 * <p>Computes delay as {@code min(baseDelay * 2^(attempt-1), maxDelay) + jitter}. Jitter is a
 * random value between 0 and {@code jitterFactor * computedDelay}.
 *
 * <p>Use {@link #withDefaults()} for a pre-configured instance (100ms base, 30s max, 1.0 jitter)
 * or build a custom strategy via {@link #builder()}.
 */
public final class ExponentialBackoff implements BackoffStrategy {

  private static final Duration DEFAULT_BASE_DELAY = Duration.ofMillis(100);
  private static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(30);
  private static final double DEFAULT_JITTER_FACTOR = 1.0;

  private final long baseDelayNanos;
  private final long maxDelayNanos;
  private final double jitterFactor;

  private ExponentialBackoff(long baseDelayNanos, long maxDelayNanos, double jitterFactor) {
    this.baseDelayNanos = baseDelayNanos;
    this.maxDelayNanos = maxDelayNanos;
    this.jitterFactor = jitterFactor;
  }

  /** Returns an instance with default settings: 100ms base, 30s max, 1.0 jitter factor. */
  public static ExponentialBackoff withDefaults() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public long delayNanos(int attempt) {
    long delay;
    int shift = attempt - 1;
    // baseDelayNanos << shift overflows long well before shift reaches 63 (e.g. at ~34 for 100ms)
    if (shift >= Long.SIZE - 1 || (shift > 0 && baseDelayNanos > (Long.MAX_VALUE >> shift))) {
      delay = maxDelayNanos;
    } else {
      delay = Math.min(baseDelayNanos << shift, maxDelayNanos);
    }
    if (jitterFactor > 0.0) {
      long jitter = (long) (delay * jitterFactor * ThreadLocalRandom.current().nextDouble());
      delay += jitter;
    }
    return delay;
  }

  public static final class Builder {
    private Duration baseDelay = DEFAULT_BASE_DELAY;
    private Duration maxDelay = DEFAULT_MAX_DELAY;
    private double jitterFactor = DEFAULT_JITTER_FACTOR;

    private Builder() {}

    public Builder baseDelay(Duration baseDelay) {
      this.baseDelay = Objects.requireNonNull(baseDelay, "baseDelay");
      return this;
    }

    public Builder maxDelay(Duration maxDelay) {
      this.maxDelay = Objects.requireNonNull(maxDelay, "maxDelay");
      return this;
    }

    public Builder jitterFactor(double jitterFactor) {
      this.jitterFactor = jitterFactor;
      return this;
    }

    public ExponentialBackoff build() {
      if (jitterFactor < 0.0 || jitterFactor > 1.0) {
        throw new IllegalArgumentException("jitterFactor must be between 0.0 and 1.0");
      }
      return new ExponentialBackoff(baseDelay.toNanos(), maxDelay.toNanos(), jitterFactor);
    }
  }
}
