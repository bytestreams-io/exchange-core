package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.common.Attributes;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.junit.jupiter.api.Test;

class ReconnectingTransportTest {

  private Transport mockTransport(InputStream in, OutputStream out) throws IOException {
    Transport t = mock(Transport.class);
    when(t.inputStream()).thenReturn(in);
    when(t.outputStream()).thenReturn(out);
    when(t.attributes()).thenReturn(Attributes.empty());
    return t;
  }

  @Test
  void delegates_to_initial_transport_when_healthy() throws IOException {
    InputStream in = mock(InputStream.class);
    OutputStream out = mock(OutputStream.class);
    Transport initial = mockTransport(in, out);

    ReconnectingTransport transport =
        ReconnectingTransport.builder(() -> initial).maxAttempts(1).build();

    assertThat(transport.inputStream()).isNotNull();
    assertThat(transport.outputStream()).isNotNull();
    assertThat(transport.attributes()).isEqualTo(Attributes.empty());
  }

  @Test
  void reconnects_when_input_stream_throws() throws IOException {
    Transport dead = mock(Transport.class);
    when(dead.inputStream()).thenThrow(new IOException("broken pipe"));
    when(dead.attributes()).thenReturn(Attributes.empty());

    InputStream freshIn = mock(InputStream.class);
    OutputStream freshOut = mock(OutputStream.class);
    Transport fresh = mockTransport(freshIn, freshOut);

    TransportFactory factory = mock(TransportFactory.class);
    when(factory.create()).thenReturn(dead).thenReturn(fresh);

    ReconnectingTransport transport =
        ReconnectingTransport.builder(factory)
            .backoffStrategy(attempt -> 0L)
            .maxAttempts(3)
            .build();

    transport.markStale(new IOException("connection lost"));
    InputStream result = transport.inputStream();
    assertThat(result).isNotNull();
  }

  @Test
  void reconnects_when_stale_flag_is_set() throws IOException {
    InputStream in1 = mock(InputStream.class);
    OutputStream out1 = mock(OutputStream.class);
    Transport t1 = mockTransport(in1, out1);

    InputStream in2 = mock(InputStream.class);
    OutputStream out2 = mock(OutputStream.class);
    Transport t2 = mockTransport(in2, out2);

    TransportFactory factory = mock(TransportFactory.class);
    when(factory.create()).thenReturn(t1).thenReturn(t2);

    ReconnectingTransport transport =
        ReconnectingTransport.builder(factory)
            .backoffStrategy(attempt -> 0L)
            .maxAttempts(3)
            .build();

    transport.inputStream();
    transport.markStale(new IOException("connection lost"));
    transport.inputStream();
    verify(factory, times(2)).create();
    verify(t1).close();
  }

  @Test
  void max_attempts_exhausted_throws_io_exception() throws IOException {
    TransportFactory factory = mock(TransportFactory.class);
    when(factory.create()).thenThrow(new IOException("connection refused"));

    assertThatThrownBy(
            () ->
                ReconnectingTransport.builder(factory)
                    .backoffStrategy(attempt -> 0L)
                    .maxAttempts(3)
                    .build())
        .isInstanceOf(IOException.class);
  }

  @Test
  void close_closes_delegate() throws IOException {
    InputStream in = mock(InputStream.class);
    OutputStream out = mock(OutputStream.class);
    Transport delegate = mockTransport(in, out);

    ReconnectingTransport transport =
        ReconnectingTransport.builder(() -> delegate).maxAttempts(1).build();

    transport.close();
    verify(delegate).close();
  }

  @Test
  void no_reconnect_after_close() throws IOException {
    InputStream in = mock(InputStream.class);
    OutputStream out = mock(OutputStream.class);
    Transport delegate = mockTransport(in, out);

    ReconnectingTransport transport =
        ReconnectingTransport.builder(() -> delegate)
            .backoffStrategy(attempt -> 0L)
            .maxAttempts(3)
            .build();

    transport.close();
    transport.markStale(new IOException("connection lost"));

    assertThatThrownBy(transport::inputStream).isInstanceOf(IOException.class);
  }

  @Test
  void multiple_consecutive_reconnects() throws IOException {
    Transport t1 = mockTransport(mock(InputStream.class), mock(OutputStream.class));
    Transport t2 = mockTransport(mock(InputStream.class), mock(OutputStream.class));
    Transport t3 = mockTransport(mock(InputStream.class), mock(OutputStream.class));

    TransportFactory factory = mock(TransportFactory.class);
    when(factory.create()).thenReturn(t1).thenReturn(t2).thenReturn(t3);

    ReconnectingTransport transport =
        ReconnectingTransport.builder(factory)
            .backoffStrategy(attempt -> 0L)
            .maxAttempts(5)
            .build();

    transport.markStale(new IOException("connection lost"));
    transport.inputStream();
    verify(t1).close();

    transport.markStale(new IOException("connection lost"));
    transport.inputStream();
    verify(t2).close();

    verify(factory, times(3)).create();
  }

  @Test
  void builder_rejects_null_factory() {
    assertThatThrownBy(() -> ReconnectingTransport.builder(null))
        .isInstanceOf(NullPointerException.class);
  }
}
