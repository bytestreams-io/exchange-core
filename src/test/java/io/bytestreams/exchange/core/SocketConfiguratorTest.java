package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.io.IOException;
import java.net.Socket;
import org.junit.jupiter.api.Test;

class SocketConfiguratorTest {

  @Test
  void default_configurator_does_nothing() throws IOException {
    Socket socket = mock(Socket.class);
    SocketConfigurator.DEFAULT.configure(socket);
    verifyNoInteractions(socket);
  }

  @Test
  void custom_configurator_is_applied() throws IOException {
    Socket socket = mock(Socket.class);
    SocketConfigurator configurator =
        s -> {
          s.setKeepAlive(true);
          s.setTcpNoDelay(true);
        };
    configurator.configure(socket);
    verify(socket).setKeepAlive(true);
    verify(socket).setTcpNoDelay(true);
  }

  @Test
  void is_functional_interface() {
    assertThat(SocketConfigurator.class.isAnnotationPresent(FunctionalInterface.class)).isTrue();
  }
}
