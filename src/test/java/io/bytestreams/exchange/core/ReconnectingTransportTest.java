package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
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
    Transport initial = mockTransport(mock(InputStream.class), mock(OutputStream.class));

    TransportFactory factory = mock(TransportFactory.class);
    when(factory.create())
        .thenReturn(initial)
        .thenThrow(new IOException("fail1"))
        .thenThrow(new IOException("fail2"))
        .thenThrow(new IOException("fail3"));

    ReconnectingTransport transport =
        ReconnectingTransport.builder(factory)
            .backoffStrategy(attempt -> 0L)
            .maxAttempts(3)
            .build();

    transport.markStale(new IOException("connection lost"));

    assertThatThrownBy(transport::inputStream)
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Failed to reconnect after 3 attempts");
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

  @Test
  void listener_receives_callbacks_in_order() throws IOException {
    Transport t1 = mockTransport(mock(InputStream.class), mock(OutputStream.class));
    Transport t2 = mockTransport(mock(InputStream.class), mock(OutputStream.class));

    TransportFactory factory = mock(TransportFactory.class);
    when(factory.create()).thenReturn(t1).thenReturn(t2);

    ReconnectListener listener = mock(ReconnectListener.class);

    ReconnectingTransport transport =
        ReconnectingTransport.builder(factory)
            .backoffStrategy(attempt -> 0L)
            .maxAttempts(3)
            .listener(listener)
            .build();

    transport.markStale(new IOException("connection lost"));
    transport.inputStream();

    var inOrder = org.mockito.Mockito.inOrder(listener);
    inOrder.verify(listener).onDisconnect(any(IOException.class));
    inOrder.verify(listener).onReconnecting(1);
    inOrder.verify(listener).onReconnected(1);
  }

  @Test
  void listener_receives_gave_up_on_exhaustion() throws IOException {
    Transport initial = mockTransport(mock(InputStream.class), mock(OutputStream.class));

    TransportFactory factory = mock(TransportFactory.class);
    when(factory.create())
        .thenReturn(initial)
        .thenThrow(new IOException("fail1"))
        .thenThrow(new IOException("fail2"));

    ReconnectListener listener = mock(ReconnectListener.class);

    ReconnectingTransport transport =
        ReconnectingTransport.builder(factory)
            .backoffStrategy(attempt -> 0L)
            .maxAttempts(2)
            .listener(listener)
            .build();

    transport.markStale(new IOException("connection lost"));
    assertThatThrownBy(transport::inputStream).isInstanceOf(IOException.class);

    verify(listener, org.mockito.Mockito.atLeastOnce()).onGaveUp(eq(2), any(IOException.class));
  }

  @Test
  void otel_metrics_are_recorded() throws IOException {
    InMemoryMetricReader metricReader = InMemoryMetricReader.create();
    SdkMeterProvider meterProvider =
        SdkMeterProvider.builder().registerMetricReader(metricReader).build();
    Meter meter = meterProvider.get("test");

    Transport t1 = mockTransport(mock(InputStream.class), mock(OutputStream.class));
    Transport t2 = mockTransport(mock(InputStream.class), mock(OutputStream.class));

    TransportFactory factory = mock(TransportFactory.class);
    when(factory.create()).thenReturn(t1).thenReturn(t2);

    ReconnectingTransport transport =
        ReconnectingTransport.builder(factory)
            .backoffStrategy(attempt -> 0L)
            .maxAttempts(3)
            .meter(meter)
            .build();

    transport.markStale(new IOException("connection lost"));
    transport.inputStream();

    TestFixture.assertLongSum(metricReader, "transport.reconnect.total", 1);
    TestFixture.assertLongSum(metricReader, "transport.reconnect.success", 1);
  }

  @Test
  void concurrent_failures_trigger_single_reconnect() throws Exception {
    Transport t1 = mockTransport(mock(InputStream.class), mock(OutputStream.class));
    Transport t2 = mockTransport(mock(InputStream.class), mock(OutputStream.class));

    TransportFactory factory = mock(TransportFactory.class);
    when(factory.create()).thenReturn(t1).thenReturn(t2);

    ReconnectingTransport transport =
        ReconnectingTransport.builder(factory)
            .backoffStrategy(attempt -> 0L)
            .maxAttempts(3)
            .build();

    transport.markStale(new IOException("connection lost"));

    java.util.concurrent.CountDownLatch startLatch = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch doneLatch = new java.util.concurrent.CountDownLatch(2);

    Runnable task =
        () -> {
          try {
            startLatch.await();
            transport.inputStream();
          } catch (Exception e) {
            // ignore
          } finally {
            doneLatch.countDown();
          }
        };

    Thread.ofVirtual().start(task);
    Thread.ofVirtual().start(task);
    startLatch.countDown();
    doneLatch.await(5, java.util.concurrent.TimeUnit.SECONDS);

    verify(factory, times(2)).create();
  }
}
