package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
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
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ReconnectingTransportTest {

  private static Transport mockTransport(InputStream in, OutputStream out) {
    Transport t = mock(Transport.class);
    try {
      when(t.inputStream()).thenReturn(in);
      when(t.outputStream()).thenReturn(out);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
    when(t.attributes()).thenReturn(Attributes.empty());
    return t;
  }

  private static Transport mockTransport() {
    return mockTransport(mock(InputStream.class), mock(OutputStream.class));
  }

  @Nested
  class Reconnect {

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
    void inputStream_reconnects_when_delegate_throws() throws IOException {
      Transport dead = mock(Transport.class);
      try {
        when(dead.inputStream()).thenThrow(new IOException("broken pipe"));
      } catch (IOException e) {
        throw new AssertionError(e);
      }
      when(dead.attributes()).thenReturn(Attributes.empty());

      Transport fresh = mockTransport();

      TransportFactory factory = mock(TransportFactory.class);
      when(factory.create()).thenReturn(dead).thenReturn(fresh);

      ReconnectingTransport transport =
          ReconnectingTransport.builder(factory)
              .backoffStrategy(attempt -> 0L)
              .maxAttempts(3)
              .build();

      transport.markStale(new IOException("connection lost"));
      assertThat(transport.inputStream()).isNotNull();
    }

    @Test
    void outputStream_reconnects_when_delegate_throws() throws IOException {
      Transport dead = mock(Transport.class);
      try {
        when(dead.inputStream()).thenReturn(mock(InputStream.class));
        when(dead.outputStream()).thenThrow(new IOException("broken pipe"));
      } catch (IOException e) {
        throw new AssertionError(e);
      }
      when(dead.attributes()).thenReturn(Attributes.empty());

      Transport fresh = mockTransport();

      TransportFactory factory = mock(TransportFactory.class);
      when(factory.create()).thenReturn(dead).thenReturn(fresh);

      ReconnectingTransport transport =
          ReconnectingTransport.builder(factory)
              .backoffStrategy(attempt -> 0L)
              .maxAttempts(3)
              .build();

      assertThat(transport.outputStream()).isNotNull();
      verify(factory, times(2)).create();
    }

    @Test
    void reconnects_when_stale_flag_is_set() throws IOException {
      Transport t1 = mockTransport();
      Transport t2 = mockTransport();

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
    void multiple_consecutive_reconnects() throws IOException {
      Transport t1 = mockTransport();
      Transport t2 = mockTransport();
      Transport t3 = mockTransport();

      TransportFactory factory = mock(TransportFactory.class);
      when(factory.create()).thenReturn(t1).thenReturn(t2).thenReturn(t3);

      ReconnectingTransport transport =
          ReconnectingTransport.builder(factory)
              .backoffStrategy(attempt -> 0L)
              .maxAttempts(5)
              .build();

      transport.inputStream();

      transport.markStale(new IOException("connection lost"));
      transport.inputStream();
      verify(t1).close();

      transport.markStale(new IOException("connection lost"));
      transport.inputStream();
      verify(t2).close();

      verify(factory, times(3)).create();
    }

    @Test
    void max_attempts_exhausted_throws() throws IOException {
      Transport initial = mockTransport();

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

      transport.inputStream();
      transport.markStale(new IOException("connection lost"));

      assertThatThrownBy(transport::inputStream)
          .isInstanceOf(IOException.class)
          .hasMessageContaining("Failed to reconnect after 3 attempts");
    }

    @Test
    void backoff_delay_is_applied_between_attempts() throws IOException {
      Transport initial = mockTransport();

      TransportFactory factory = mock(TransportFactory.class);
      when(factory.create())
          .thenReturn(initial)
          .thenThrow(new IOException("fail"))
          .thenReturn(mockTransport());

      AtomicInteger backoffCalls = new AtomicInteger();
      BackoffStrategy strategy =
          attempt -> {
            backoffCalls.incrementAndGet();
            return 1_000L;
          };

      ReconnectingTransport transport =
          ReconnectingTransport.builder(factory).backoffStrategy(strategy).maxAttempts(3).build();

      transport.inputStream();
      transport.markStale(new IOException("connection lost"));
      transport.inputStream();

      assertThat(backoffCalls.get()).isEqualTo(1);
    }

    @Test
    void reconnect_aborts_when_thread_is_interrupted() throws Exception {
      Transport initial = mockTransport();

      TransportFactory factory = mock(TransportFactory.class);
      when(factory.create())
          .thenReturn(initial)
          .thenThrow(new IOException("fail1"))
          .thenThrow(new IOException("fail2"));

      ReconnectingTransport transport =
          ReconnectingTransport.builder(factory)
              .backoffStrategy(attempt -> 0L)
              .maxAttempts(5)
              .build();

      transport.inputStream();
      transport.markStale(new IOException("connection lost"));

      Thread.currentThread().interrupt();

      assertThatThrownBy(transport::inputStream)
          .isInstanceOf(IOException.class)
          .hasMessageContaining("interrupted");

      // Clear the interrupted flag
      Thread.interrupted();
    }
  }

  @Nested
  class StreamWrapper {

    @Test
    void read_failure_sets_stale_and_triggers_reconnect() throws IOException {
      InputStream failingIn = mock(InputStream.class);
      when(failingIn.read(any(byte[].class), anyInt(), anyInt()))
          .thenThrow(new IOException("broken"));
      Transport t1 = mockTransport(failingIn, mock(OutputStream.class));
      Transport t2 = mockTransport();

      TransportFactory factory = mock(TransportFactory.class);
      when(factory.create()).thenReturn(t1).thenReturn(t2);

      ReconnectingTransport transport =
          ReconnectingTransport.builder(factory)
              .backoffStrategy(attempt -> 0L)
              .maxAttempts(3)
              .build();

      InputStream wrapped = transport.inputStream();
      assertThatThrownBy(() -> wrapped.read(new byte[10], 0, 10)).isInstanceOf(IOException.class);

      transport.inputStream();
      verify(factory, times(2)).create();
    }

    @Test
    void write_failure_sets_stale_and_triggers_reconnect() throws IOException {
      OutputStream failingOut = mock(OutputStream.class);
      doThrow(new IOException("broken"))
          .when(failingOut)
          .write(any(byte[].class), anyInt(), anyInt());
      Transport t1 = mockTransport(mock(InputStream.class), failingOut);
      Transport t2 = mockTransport();

      TransportFactory factory = mock(TransportFactory.class);
      when(factory.create()).thenReturn(t1).thenReturn(t2);

      ReconnectingTransport transport =
          ReconnectingTransport.builder(factory)
              .backoffStrategy(attempt -> 0L)
              .maxAttempts(3)
              .build();

      OutputStream wrapped = transport.outputStream();
      assertThatThrownBy(() -> wrapped.write(new byte[10], 0, 10)).isInstanceOf(IOException.class);

      transport.outputStream();
      verify(factory, times(2)).create();
    }

    @Test
    void flush_failure_sets_stale_and_triggers_reconnect() throws IOException {
      OutputStream failingOut = mock(OutputStream.class);
      doThrow(new IOException("broken")).when(failingOut).flush();
      Transport t1 = mockTransport(mock(InputStream.class), failingOut);
      Transport t2 = mockTransport();

      TransportFactory factory = mock(TransportFactory.class);
      when(factory.create()).thenReturn(t1).thenReturn(t2);

      ReconnectingTransport transport =
          ReconnectingTransport.builder(factory)
              .backoffStrategy(attempt -> 0L)
              .maxAttempts(3)
              .build();

      OutputStream wrapped = transport.outputStream();
      assertThatThrownBy(wrapped::flush).isInstanceOf(IOException.class);

      transport.outputStream();
      verify(factory, times(2)).create();
    }

    @Test
    void single_byte_read_failure_sets_stale() throws IOException {
      InputStream failingIn = mock(InputStream.class);
      when(failingIn.read()).thenThrow(new IOException("broken"));
      Transport t1 = mockTransport(failingIn, mock(OutputStream.class));
      Transport t2 = mockTransport();

      TransportFactory factory = mock(TransportFactory.class);
      when(factory.create()).thenReturn(t1).thenReturn(t2);

      ReconnectingTransport transport =
          ReconnectingTransport.builder(factory)
              .backoffStrategy(attempt -> 0L)
              .maxAttempts(3)
              .build();

      InputStream wrapped = transport.inputStream();
      assertThatThrownBy(wrapped::read).isInstanceOf(IOException.class);

      transport.inputStream();
      verify(factory, times(2)).create();
    }

    @Test
    void single_byte_write_failure_sets_stale() throws IOException {
      OutputStream failingOut = mock(OutputStream.class);
      doThrow(new IOException("broken")).when(failingOut).write(42);
      Transport t1 = mockTransport(mock(InputStream.class), failingOut);
      Transport t2 = mockTransport();

      TransportFactory factory = mock(TransportFactory.class);
      when(factory.create()).thenReturn(t1).thenReturn(t2);

      ReconnectingTransport transport =
          ReconnectingTransport.builder(factory)
              .backoffStrategy(attempt -> 0L)
              .maxAttempts(3)
              .build();

      OutputStream wrapped = transport.outputStream();
      assertThatThrownBy(() -> wrapped.write(42)).isInstanceOf(IOException.class);

      transport.outputStream();
      verify(factory, times(2)).create();
    }

    @Test
    void single_byte_read_delegates_through_wrapper() throws IOException {
      InputStream in = mock(InputStream.class);
      when(in.read()).thenReturn(42);
      Transport t1 = mockTransport(in, mock(OutputStream.class));

      ReconnectingTransport transport =
          ReconnectingTransport.builder(() -> t1).maxAttempts(1).build();

      assertThat(transport.inputStream().read()).isEqualTo(42);
    }

    @Test
    void single_byte_write_delegates_through_wrapper() throws IOException {
      OutputStream out = mock(OutputStream.class);
      Transport t1 = mockTransport(mock(InputStream.class), out);

      ReconnectingTransport transport =
          ReconnectingTransport.builder(() -> t1).maxAttempts(1).build();

      transport.outputStream().write(42);
      verify(out).write(42);
    }
  }

  @Nested
  class Lifecycle {

    @Test
    void close_closes_delegate() throws IOException {
      Transport delegate = mockTransport();

      ReconnectingTransport transport =
          ReconnectingTransport.builder(() -> delegate).maxAttempts(1).build();

      transport.inputStream();
      transport.close();
      verify(delegate).close();
    }

    @Test
    void close_prevents_reconnect() throws IOException {
      Transport initial = mockTransport();

      TransportFactory factory = mock(TransportFactory.class);
      when(factory.create()).thenReturn(initial);

      ReconnectingTransport transport =
          ReconnectingTransport.builder(factory)
              .backoffStrategy(attempt -> 0L)
              .maxAttempts(3)
              .build();

      transport.close();
      transport.markStale(new IOException("connection lost"));

      assertThatThrownBy(transport::inputStream)
          .isInstanceOf(IOException.class)
          .hasMessageContaining("closed");

      verify(factory, times(0)).create();
    }
  }

  @Nested
  class Listener {

    @Test
    void receives_callbacks_in_order() throws IOException {
      Transport t1 = mockTransport();
      Transport t2 = mockTransport();

      TransportFactory factory = mock(TransportFactory.class);
      when(factory.create()).thenReturn(t1).thenReturn(t2);

      ReconnectListener listener = mock(ReconnectListener.class);

      ReconnectingTransport transport =
          ReconnectingTransport.builder(factory)
              .backoffStrategy(attempt -> 0L)
              .maxAttempts(3)
              .listener(listener)
              .build();

      transport.inputStream();
      transport.markStale(new IOException("connection lost"));
      transport.inputStream();

      var order = inOrder(listener);
      order.verify(listener).onDisconnect(any(IOException.class));
      order.verify(listener).onReconnecting(1);
      order.verify(listener).onReconnected(1);
    }

    @Test
    void receives_gave_up_on_exhaustion() throws IOException {
      Transport initial = mockTransport();

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

      transport.inputStream();
      transport.markStale(new IOException("connection lost"));
      assertThatThrownBy(transport::inputStream).isInstanceOf(IOException.class);

      verify(listener, atLeastOnce()).onGaveUp(eq(2), any(IOException.class));
    }
  }

  @Nested
  class Metrics {

    private InMemoryMetricReader metricReader;
    private SdkMeterProvider meterProvider;

    @BeforeEach
    void setUp() {
      metricReader = InMemoryMetricReader.create();
      meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build();
    }

    @AfterEach
    void tearDown() {
      meterProvider.close();
    }

    @Test
    void reconnect_metrics_are_recorded() throws IOException {
      Meter meter = meterProvider.get("test");

      Transport t1 = mockTransport();
      Transport t2 = mockTransport();

      TransportFactory factory = mock(TransportFactory.class);
      when(factory.create()).thenReturn(t1).thenReturn(t2);

      ReconnectingTransport transport =
          ReconnectingTransport.builder(factory)
              .backoffStrategy(attempt -> 0L)
              .maxAttempts(3)
              .meter(meter)
              .build();

      transport.inputStream();
      transport.markStale(new IOException("connection lost"));
      transport.inputStream();

      // Initial connect + reconnect both increment counters
      TestFixture.assertLongSum(metricReader, "transport.reconnect.total", 2);
      TestFixture.assertLongSum(metricReader, "transport.reconnect.success", 2);
    }
  }

  @Nested
  class BuilderValidation {

    @Test
    void rejects_null_factory() {
      assertThatThrownBy(() -> ReconnectingTransport.builder(null))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejects_zero_max_attempts() {
      assertThatThrownBy(
              () ->
                  ReconnectingTransport.builder(() -> mock(Transport.class)).maxAttempts(0).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("maxAttempts must be positive");
    }

    @Test
    void rejects_negative_max_attempts() {
      assertThatThrownBy(
              () ->
                  ReconnectingTransport.builder(() -> mock(Transport.class))
                      .maxAttempts(-1)
                      .build())
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Nested
  class Concurrency {

    @Test
    void concurrent_failures_trigger_single_reconnect() throws Exception {
      Transport t1 = mockTransport();
      Transport t2 = mockTransport();
      Transport t3 = mockTransport();

      TransportFactory factory = mock(TransportFactory.class);
      when(factory.create()).thenReturn(t1).thenReturn(t2).thenReturn(t3);

      ReconnectingTransport transport =
          ReconnectingTransport.builder(factory)
              .backoffStrategy(attempt -> 0L)
              .maxAttempts(3)
              .build();

      transport.inputStream();
      transport.markStale(new IOException("connection lost"));

      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(2);

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
      doneLatch.await(5, TimeUnit.SECONDS);

      verify(factory, times(2)).create();
    }
  }

  @Nested
  class IntegrationTest {

    @Test
    void channel_recovers_after_transport_failure() throws Exception {
      PipedInputStream clientIn1 = new PipedInputStream();
      PipedOutputStream serverOut1 = new PipedOutputStream(clientIn1);
      PipedInputStream serverIn1 = new PipedInputStream();
      PipedOutputStream clientOut1 = new PipedOutputStream(serverIn1);

      PipedInputStream clientIn2 = new PipedInputStream();
      PipedOutputStream serverOut2 = new PipedOutputStream(clientIn2);
      PipedInputStream serverIn2 = new PipedInputStream();
      PipedOutputStream clientOut2 = new PipedOutputStream(serverIn2);

      Transport t1 =
          new Transport() {
            public InputStream inputStream() {
              return clientIn1;
            }

            public OutputStream outputStream() {
              return clientOut1;
            }

            public Attributes attributes() {
              return Attributes.empty();
            }

            public void close() throws IOException {
              clientIn1.close();
              clientOut1.close();
            }
          };
      Transport t2 =
          new Transport() {
            public InputStream inputStream() {
              return clientIn2;
            }

            public OutputStream outputStream() {
              return clientOut2;
            }

            public Attributes attributes() {
              return Attributes.empty();
            }

            public void close() throws IOException {
              clientIn2.close();
              clientOut2.close();
            }
          };

      AtomicInteger callCount = new AtomicInteger(0);
      TransportFactory factory =
          () -> {
            int c = callCount.incrementAndGet();
            if (c == 1) return t1;
            if (c == 2) return t2;
            throw new IOException("no more transports");
          };

      CountDownLatch reconnectedLatch = new CountDownLatch(1);

      ReconnectingTransport reconnecting =
          ReconnectingTransport.builder(factory)
              .backoffStrategy(attempt -> 0L)
              .maxAttempts(3)
              .listener(
                  new ReconnectListener() {
                    @Override
                    public void onReconnected(int attempt) {
                      reconnectedLatch.countDown();
                    }
                  })
              .build();

      PipelinedChannel<String, String> channel =
          new PipelinedChannel<>(
              reconnecting,
              Integer.MAX_VALUE,
              TestFixture.FRAMED_WRITER,
              TestFixture.FRAMED_READER,
              1,
              new ErrorHandler<>() {
                @Override
                public boolean stopOnError(Channel ch, ErrorContext<String, String> context) {
                  return false;
                }
              },
              Duration.ofSeconds(5),
              AbstractChannel.DEFAULT_ERROR_BACKOFF_NANOS,
              OTel.meter(),
              OTel.tracer());
      channel.start();

      Thread.ofVirtual()
          .start(
              () -> {
                try {
                  String req = TestFixture.FRAMED_READER.read(serverIn1);
                  TestFixture.FRAMED_WRITER.write("reply1:" + req, serverOut1);
                } catch (IOException e) {
                  // expected when pipe closes
                }
              });

      String resp1 = channel.request("hello").get(3, TimeUnit.SECONDS);
      assertThat(resp1).isEqualTo("reply1:hello");

      serverOut1.close();
      serverIn1.close();

      assertThat(reconnectedLatch.await(5, TimeUnit.SECONDS)).isTrue();

      Thread.ofVirtual()
          .start(
              () -> {
                try {
                  String req = TestFixture.FRAMED_READER.read(serverIn2);
                  TestFixture.FRAMED_WRITER.write("reply2:" + req, serverOut2);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });

      String resp2 = channel.request("world").get(5, TimeUnit.SECONDS);
      assertThat(resp2).isEqualTo("reply2:world");

      channel.close().get(3, TimeUnit.SECONDS);
    }
  }
}
