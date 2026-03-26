package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ExponentialBackoffTest {

  private static ExponentialBackoff noJitterBackoff() {
    return noJitterBackoff(Duration.ofSeconds(30));
  }

  private static ExponentialBackoff noJitterBackoff(Duration maxDelay) {
    return ExponentialBackoff.builder()
        .baseDelay(Duration.ofMillis(100))
        .maxDelay(maxDelay)
        .jitterFactor(0.0)
        .build();
  }

  @Nested
  class Delay {

    @Test
    void doubles_each_attempt() {
      ExponentialBackoff backoff = noJitterBackoff();
      assertThat(backoff.delayNanos(1)).isEqualTo(Duration.ofMillis(100).toNanos());
      assertThat(backoff.delayNanos(2)).isEqualTo(Duration.ofMillis(200).toNanos());
      assertThat(backoff.delayNanos(3)).isEqualTo(Duration.ofMillis(400).toNanos());
      assertThat(backoff.delayNanos(4)).isEqualTo(Duration.ofMillis(800).toNanos());
    }

    @Test
    void capped_at_max() {
      ExponentialBackoff backoff = noJitterBackoff(Duration.ofMillis(500));
      assertThat(backoff.delayNanos(4)).isEqualTo(Duration.ofMillis(500).toNanos());
      assertThat(backoff.delayNanos(100)).isEqualTo(Duration.ofMillis(500).toNanos());
    }

    @Test
    void does_not_overflow_at_high_attempts() {
      ExponentialBackoff backoff = noJitterBackoff();
      long maxNanos = Duration.ofSeconds(30).toNanos();
      assertThat(backoff.delayNanos(35)).isEqualTo(maxNanos);
      assertThat(backoff.delayNanos(40)).isEqualTo(maxNanos);
      assertThat(backoff.delayNanos(55)).isEqualTo(maxNanos);
      assertThat(backoff.delayNanos(63)).isEqualTo(maxNanos);
      assertThat(backoff.delayNanos(100)).isEqualTo(maxNanos);
    }

    @Test
    void zero_jitter_is_deterministic() {
      ExponentialBackoff backoff = noJitterBackoff();
      long first = backoff.delayNanos(3);
      for (int i = 0; i < 10; i++) {
        assertThat(backoff.delayNanos(3)).isEqualTo(first);
      }
    }
  }

  @Nested
  class Jitter {

    @Test
    void stays_within_bounds() {
      ExponentialBackoff backoff =
          ExponentialBackoff.builder()
              .baseDelay(Duration.ofMillis(100))
              .maxDelay(Duration.ofSeconds(30))
              .jitterFactor(1.0)
              .build();
      long baseNanos = Duration.ofMillis(100).toNanos();
      for (int i = 0; i < 100; i++) {
        long delay = backoff.delayNanos(1);
        assertThat(delay).isBetween(baseNanos, baseNanos * 2);
      }
    }

    @Test
    void withDefaults_returns_usable_instance() {
      ExponentialBackoff backoff = ExponentialBackoff.withDefaults();
      assertThat(backoff.delayNanos(1)).isPositive();
    }
  }

  @Nested
  class BuilderValidation {

    @Test
    void rejects_null_base_delay() {
      assertThatThrownBy(() -> ExponentialBackoff.builder().baseDelay(null))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejects_null_max_delay() {
      assertThatThrownBy(() -> ExponentialBackoff.builder().maxDelay(null))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejects_negative_jitter_factor() {
      assertThatThrownBy(() -> ExponentialBackoff.builder().jitterFactor(-0.1).build())
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejects_jitter_factor_above_one() {
      assertThatThrownBy(() -> ExponentialBackoff.builder().jitterFactor(1.1).build())
          .isInstanceOf(IllegalArgumentException.class);
    }
  }
}
