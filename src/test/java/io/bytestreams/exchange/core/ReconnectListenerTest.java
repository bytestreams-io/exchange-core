package io.bytestreams.exchange.core;

import org.junit.jupiter.api.Test;

class ReconnectListenerTest {

  @Test
  void default_methods_do_not_throw() {
    ReconnectListener listener = new ReconnectListener() {};
    listener.onDisconnect(new RuntimeException("test"));
    listener.onReconnecting(1);
    listener.onReconnected(1);
    listener.onGaveUp(3, new RuntimeException("test"));
  }
}
