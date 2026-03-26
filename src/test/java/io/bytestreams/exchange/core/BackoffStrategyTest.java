package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BackoffStrategyTest {

  @Test
  void custom_strategy_receives_attempt_number() {
    BackoffStrategy strategy = attempt -> attempt * 1_000_000L; // attempt * 1ms
    assertThat(strategy.delayNanos(1)).isEqualTo(1_000_000L);
    assertThat(strategy.delayNanos(5)).isEqualTo(5_000_000L);
  }

  @Test
  void strategy_can_return_zero() {
    BackoffStrategy strategy = attempt -> 0L;
    assertThat(strategy.delayNanos(1)).isEqualTo(0L);
  }
}
