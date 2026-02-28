package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ChannelStatusTest {

  @Test
  void has_four_values() {
    assertThat(ChannelStatus.values())
        .containsExactly(
            ChannelStatus.INIT, ChannelStatus.READY, ChannelStatus.CLOSING, ChannelStatus.CLOSED);
  }

  @ParameterizedTest
  @EnumSource(ChannelStatus.class)
  void valueOf_returns_correct_constant(ChannelStatus status) {
    assertThat(ChannelStatus.valueOf(status.name())).isEqualTo(status);
  }
}
