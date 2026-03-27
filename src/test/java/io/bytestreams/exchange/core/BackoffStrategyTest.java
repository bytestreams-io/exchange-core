package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class BackoffStrategyTest {

  @Test
  void custom_strategy_receives_attempt_number() {
    BackoffStrategy strategy = attempt -> Duration.ofMillis(attempt * 100L);
    assertThat(strategy.delay(1)).isEqualTo(Duration.ofMillis(100));
    assertThat(strategy.delay(5)).isEqualTo(Duration.ofMillis(500));
  }

  @Nested
  class Fixed {

    @Test
    void returns_same_delay_for_every_attempt() {
      BackoffStrategy strategy = BackoffStrategy.fixed(Duration.ofMillis(200));
      assertThat(strategy.delay(1)).isEqualTo(Duration.ofMillis(200));
      assertThat(strategy.delay(5)).isEqualTo(Duration.ofMillis(200));
      assertThat(strategy.delay(100)).isEqualTo(Duration.ofMillis(200));
    }

    @Test
    void zero_delay() {
      BackoffStrategy strategy = BackoffStrategy.fixed(Duration.ZERO);
      assertThat(strategy.delay(1)).isEqualTo(Duration.ZERO);
    }

    @Test
    void rejects_null() {
      assertThatThrownBy(() -> BackoffStrategy.fixed(null))
          .isInstanceOf(NullPointerException.class);
    }
  }

  @Nested
  class Exponential {

    @Test
    void doubles_each_attempt() {
      BackoffStrategy strategy = BackoffStrategy.exponential(Duration.ofMillis(100));
      assertThat(strategy.delay(1)).isEqualTo(Duration.ofMillis(100));
      assertThat(strategy.delay(2)).isEqualTo(Duration.ofMillis(200));
      assertThat(strategy.delay(3)).isEqualTo(Duration.ofMillis(400));
      assertThat(strategy.delay(4)).isEqualTo(Duration.ofMillis(800));
    }

    @Test
    void does_not_overflow_at_high_attempts() {
      BackoffStrategy strategy = BackoffStrategy.exponential(Duration.ofMillis(100));
      assertThat(strategy.delay(35).toNanos()).isPositive();
      assertThat(strategy.delay(63).toNanos()).isPositive();
      assertThat(strategy.delay(100).toNanos()).isPositive();
    }

    @Test
    void rejects_null() {
      assertThatThrownBy(() -> BackoffStrategy.exponential(null))
          .isInstanceOf(NullPointerException.class);
    }
  }

  @Nested
  class WithMax {

    @Test
    void caps_at_maximum() {
      BackoffStrategy strategy =
          BackoffStrategy.exponential(Duration.ofMillis(100)).withMax(Duration.ofMillis(500));
      assertThat(strategy.delay(1)).isEqualTo(Duration.ofMillis(100));
      assertThat(strategy.delay(3)).isEqualTo(Duration.ofMillis(400));
      assertThat(strategy.delay(4)).isEqualTo(Duration.ofMillis(500));
      assertThat(strategy.delay(100)).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void passes_through_when_under_max() {
      BackoffStrategy strategy =
          BackoffStrategy.fixed(Duration.ofMillis(100)).withMax(Duration.ofSeconds(1));
      assertThat(strategy.delay(1)).isEqualTo(Duration.ofMillis(100));
    }

    @Test
    void rejects_null() {
      var strategy = BackoffStrategy.fixed(Duration.ofMillis(100));
      assertThatThrownBy(() -> strategy.withMax(null)).isInstanceOf(NullPointerException.class);
    }
  }

  @Nested
  class WithJitter {

    @Test
    void stays_within_bounds() {
      BackoffStrategy strategy = BackoffStrategy.fixed(Duration.ofMillis(100)).withJitter(1.0);
      long baseNanos = Duration.ofMillis(100).toNanos();
      for (int i = 0; i < 100; i++) {
        long nanos = strategy.delay(1).toNanos();
        assertThat(nanos).isBetween(baseNanos, baseNanos * 2);
      }
    }

    @Test
    void zero_factor_adds_no_jitter() {
      BackoffStrategy strategy = BackoffStrategy.fixed(Duration.ofMillis(100)).withJitter(0.0);
      for (int i = 0; i < 10; i++) {
        assertThat(strategy.delay(1)).isEqualTo(Duration.ofMillis(100));
      }
    }

    @Test
    void rejects_negative_factor() {
      var strategy = BackoffStrategy.fixed(Duration.ofMillis(100));
      assertThatThrownBy(() -> strategy.withJitter(-0.1))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejects_factor_above_one() {
      var strategy = BackoffStrategy.fixed(Duration.ofMillis(100));
      assertThatThrownBy(() -> strategy.withJitter(1.1))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Nested
  class Composition {

    @Test
    void exponential_with_max_and_jitter() {
      BackoffStrategy strategy =
          BackoffStrategy.exponential(Duration.ofMillis(100))
              .withMax(Duration.ofSeconds(30))
              .withJitter(0.5);
      long maxNanos = Duration.ofSeconds(30).toNanos();
      // High attempt: capped at max + up to 50% jitter
      for (int i = 0; i < 10; i++) {
        long nanos = strategy.delay(100).toNanos();
        assertThat(nanos).isBetween(maxNanos, (long) (maxNanos * 1.5));
      }
    }
  }
}
