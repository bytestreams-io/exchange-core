package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class TransportFactoryTest {

  @Test
  void create_returns_transport() throws IOException {
    Transport expected = mock(Transport.class);
    TransportFactory factory = () -> expected;
    assertThat(factory.create()).isSameAs(expected);
  }

  @Test
  void create_can_throw_io_exception() {
    TransportFactory factory =
        () -> {
          throw new IOException("connection refused");
        };
    assertThatThrownBy(factory::create)
        .isInstanceOf(IOException.class)
        .hasMessage("connection refused");
  }
}
