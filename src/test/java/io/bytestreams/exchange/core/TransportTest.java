package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.common.Attributes;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.junit.jupiter.api.Test;

class TransportTest {

  @Test
  void transport_is_closeable() {
    assertThat(Closeable.class).isAssignableFrom(Transport.class);
  }

  @Test
  void mock_transport_delegates_correctly() throws IOException {
    Transport transport = mock(Transport.class);
    InputStream in = mock(InputStream.class);
    OutputStream out = mock(OutputStream.class);
    Attributes attrs = Attributes.empty();

    when(transport.inputStream()).thenReturn(in);
    when(transport.outputStream()).thenReturn(out);
    when(transport.attributes()).thenReturn(attrs);

    assertThat(transport.inputStream()).isSameAs(in);
    assertThat(transport.outputStream()).isSameAs(out);
    assertThat(transport.attributes()).isSameAs(attrs);

    transport.close();
    verify(transport).close();
  }
}
