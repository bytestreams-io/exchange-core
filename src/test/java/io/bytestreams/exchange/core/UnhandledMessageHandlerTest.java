package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class UnhandledMessageHandlerTest {

  @Test
  void can_assign_lambda() {
    UnhandledMessageHandler<String> handler = message -> {};
    assertThat(handler).isNotNull();
  }

  @Test
  void onMessage_invoked_with_message() {
    AtomicReference<String> received = new AtomicReference<>();
    UnhandledMessageHandler<String> handler = received::set;

    handler.onMessage("hello");

    assertThat(received.get()).isEqualTo("hello");
  }
}
