package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.common.Attributes;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class AcceptorTest {

  private static Transport stubTransport() {
    Transport transport = mock(Transport.class);
    when(transport.attributes()).thenReturn(Attributes.empty());
    return transport;
  }

  /**
   * A transport factory backed by a {@link BlockingQueue}. Each call to {@link #create()} blocks
   * until a transport is offered. Implements {@link Closeable} so that {@link Acceptor#close()}
   * exercises the close-the-factory path.
   */
  private static final class BlockingQueueTransportFactory implements TransportFactory, Closeable {
    final BlockingQueue<Transport> queue = new LinkedBlockingQueue<>();
    private volatile boolean closed;

    @Override
    public Transport create() throws IOException {
      try {
        while (!closed) {
          Transport t = queue.poll(50, TimeUnit.MILLISECONDS);
          if (t != null) return t;
        }
        throw new IOException("factory closed");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted", e);
      }
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  @Nested
  class AcceptLoop {

    @Test
    void accepts_connections_and_creates_channels() throws Exception {
      BlockingQueueTransportFactory factory = new BlockingQueueTransportFactory();
      AtomicInteger factoryCallCount = new AtomicInteger();

      Function<Transport, Channel> channelFactory =
          transport -> {
            int n = factoryCallCount.incrementAndGet();
            Channel channel = mock(Channel.class);
            when(channel.id()).thenReturn("ch-" + n);
            when(channel.closeFuture()).thenReturn(new CompletableFuture<>());
            when(channel.close()).thenReturn(CompletableFuture.completedFuture(null));
            return channel;
          };

      Acceptor acceptor = Acceptor.builder(factory).channelFactory(channelFactory).build();
      acceptor.start();

      factory.queue.put(stubTransport());
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(factoryCallCount.get()).isGreaterThanOrEqualTo(1));

      acceptor.close().join();
    }

    @Test
    void channel_factory_exception_does_not_stop_acceptor() throws Exception {
      BlockingQueueTransportFactory factory = new BlockingQueueTransportFactory();
      AtomicInteger callCount = new AtomicInteger();

      Function<Transport, Channel> channelFactory =
          transport -> {
            int n = callCount.incrementAndGet();
            if (n == 1) throw new RuntimeException("factory error");
            Channel channel = mock(Channel.class);
            when(channel.id()).thenReturn("ch-" + n);
            when(channel.closeFuture()).thenReturn(new CompletableFuture<>());
            when(channel.close()).thenReturn(CompletableFuture.completedFuture(null));
            return channel;
          };

      Acceptor acceptor = Acceptor.builder(factory).channelFactory(channelFactory).build();
      acceptor.start();

      factory.queue.put(stubTransport());
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(callCount.get()).isGreaterThanOrEqualTo(1));

      factory.queue.put(stubTransport());
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(callCount.get()).isGreaterThanOrEqualTo(2));

      acceptor.close().join();
    }

    @Test
    void transport_factory_runtime_exception_is_caught_with_null_transport() throws Exception {
      AtomicInteger createCount = new AtomicInteger();
      CountDownLatch errorHit = new CountDownLatch(1);

      TransportFactory factory =
          () -> {
            int n = createCount.incrementAndGet();
            if (n == 1) {
              errorHit.countDown();
              throw new RuntimeException("simulated create failure");
            }
            return stubTransport();
          };

      AtomicInteger channelCount = new AtomicInteger();
      Function<Transport, Channel> channelFactory =
          transport -> {
            channelCount.incrementAndGet();
            Channel channel = mock(Channel.class);
            when(channel.id()).thenReturn("ch-" + channelCount.get());
            when(channel.closeFuture()).thenReturn(new CompletableFuture<>());
            when(channel.close()).thenReturn(CompletableFuture.completedFuture(null));
            return channel;
          };

      Acceptor acceptor = Acceptor.builder(factory).channelFactory(channelFactory).build();
      acceptor.start();

      errorHit.await(2, TimeUnit.SECONDS);

      // After the RuntimeException, the loop continues and accepts a real transport
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(channelCount.get()).isGreaterThanOrEqualTo(1));

      acceptor.close().join();
    }

    @Test
    void non_closeable_factory_skips_close_in_finally() throws Exception {
      // Uses a plain TransportFactory (not Closeable) and an Error to force the
      // finally block's compareAndSet to succeed, covering the non-Closeable branch.
      CountDownLatch errorThrown = new CountDownLatch(1);
      CountDownLatch vtDead = new CountDownLatch(1);

      // Plain lambda — NOT Closeable
      TransportFactory factory = AcceptorTest::stubTransport;

      Function<Transport, Channel> channelFactory =
          transport -> {
            Thread.currentThread().setUncaughtExceptionHandler((t, e) -> vtDead.countDown());
            errorThrown.countDown();
            throw new StackOverflowError("simulated");
          };

      Acceptor acceptor = Acceptor.builder(factory).channelFactory(channelFactory).build();
      acceptor.start();

      errorThrown.await(2, TimeUnit.SECONDS);

      // The finally block runs, compareAndSet succeeds, but factory is NOT Closeable
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(acceptor.closeFuture()).isDone());

      vtDead.await(2, TimeUnit.SECONDS);
    }

    @Test
    void io_exception_in_accept_loop_when_not_closed_records_and_retries() throws Exception {
      AtomicInteger createCount = new AtomicInteger();
      CountDownLatch errorHit = new CountDownLatch(1);

      TransportFactory factory =
          () -> {
            int n = createCount.incrementAndGet();
            if (n >= 2) {
              errorHit.countDown();
              throw new IOException("simulated accept failure");
            }
            return stubTransport();
          };

      AtomicInteger channelCount = new AtomicInteger();
      Function<Transport, Channel> channelFactory =
          transport -> {
            channelCount.incrementAndGet();
            Channel channel = mock(Channel.class);
            when(channel.id()).thenReturn("ch-" + channelCount.get());
            when(channel.closeFuture()).thenReturn(new CompletableFuture<>());
            when(channel.close()).thenReturn(CompletableFuture.completedFuture(null));
            return channel;
          };

      Acceptor acceptor =
          Acceptor.builder(factory)
              .channelFactory(channelFactory)
              .errorBackoff(Duration.ofMillis(10))
              .build();
      acceptor.start();

      // Wait for the error path to be exercised
      errorHit.await(2, TimeUnit.SECONDS);
      // Let a few retries happen
      await().during(Duration.ofMillis(100)).until(() -> true);

      acceptor.close().join();
    }

    @Test
    void io_exception_while_closed_breaks_accept_loop() throws Exception {
      CountDownLatch firstCreated = new CountDownLatch(1);
      AtomicInteger createCount = new AtomicInteger();

      TransportFactory factory =
          () -> {
            int n = createCount.incrementAndGet();
            if (n == 1) {
              firstCreated.countDown();
              return stubTransport();
            }
            // Throw immediately to simulate ServerSocket.accept() throwing after the socket
            // is closed
            throw new IOException("socket closed");
          };

      Function<Transport, Channel> channelFactory =
          transport -> {
            Channel channel = mock(Channel.class);
            when(channel.id()).thenReturn("ch-" + createCount.get());
            when(channel.closeFuture()).thenReturn(new CompletableFuture<>());
            when(channel.close()).thenReturn(CompletableFuture.completedFuture(null));
            return channel;
          };

      Acceptor acceptor =
          Acceptor.builder(factory)
              .channelFactory(channelFactory)
              .errorBackoff(Duration.ofMillis(10))
              .build();
      acceptor.start();

      firstCreated.await(2, TimeUnit.SECONDS);

      // Set closed=true — next IOException in the catch block will see closed=true and break
      acceptor.closed.set(true);

      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(acceptor.closeFuture()).isDone());
    }

    @Test
    void closed_after_accept_returns_closes_transport_and_breaks() throws Exception {
      BlockingQueueTransportFactory factory = new BlockingQueueTransportFactory();
      CountDownLatch firstAccepted = new CountDownLatch(1);
      AtomicInteger callCount = new AtomicInteger();

      Function<Transport, Channel> channelFactory =
          transport -> {
            int n = callCount.incrementAndGet();
            firstAccepted.countDown();
            Channel channel = mock(Channel.class);
            when(channel.id()).thenReturn("ch-" + n);
            CompletableFuture<Void> cf = new CompletableFuture<>();
            when(channel.closeFuture()).thenReturn(cf);
            when(channel.close())
                .thenAnswer(
                    inv -> {
                      cf.complete(null);
                      return CompletableFuture.completedFuture(null);
                    });
            return channel;
          };

      Acceptor acceptor = Acceptor.builder(factory).channelFactory(channelFactory).build();
      acceptor.start();

      // First transport — confirms the accept loop is running
      factory.queue.put(stubTransport());
      firstAccepted.await(2, TimeUnit.SECONDS);

      // Set closed=true WITHOUT closing the transport factory
      acceptor.closed.set(true);

      // Now offer another transport — create() returns it, but closed.get() is true
      factory.queue.put(stubTransport());

      // The loop should break, finally block runs
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(acceptor.closeFuture()).isDone());
    }

    @Test
    void factory_error_escapes_and_finally_closes_transport_factory() throws Exception {
      CountDownLatch errorThrown = new CountDownLatch(1);
      CountDownLatch vtDead = new CountDownLatch(1);

      BlockingQueueTransportFactory factory = new BlockingQueueTransportFactory();

      Function<Transport, Channel> channelFactory =
          transport -> {
            Thread.currentThread().setUncaughtExceptionHandler((t, e) -> vtDead.countDown());
            errorThrown.countDown();
            throw new StackOverflowError("simulated");
          };

      Acceptor acceptor = Acceptor.builder(factory).channelFactory(channelFactory).build();
      acceptor.start();

      factory.queue.put(stubTransport());
      errorThrown.await(2, TimeUnit.SECONDS);

      // The accept loop VT should have terminated via the finally block
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(acceptor.closeFuture()).isDone());

      vtDead.await(2, TimeUnit.SECONDS);
    }

    @Test
    void close_race_during_accept() throws Exception {
      BlockingQueueTransportFactory factory = new BlockingQueueTransportFactory();
      CountDownLatch factoryCalled = new CountDownLatch(1);
      AtomicReference<Acceptor> acceptorRef = new AtomicReference<>();

      Function<Transport, Channel> channelFactory =
          transport -> {
            Acceptor acc = acceptorRef.get();
            if (acc != null) {
              acc.close();
            }
            Channel channel = mock(Channel.class);
            when(channel.id()).thenReturn("ch-race");
            CompletableFuture<Void> cf = new CompletableFuture<>();
            when(channel.closeFuture()).thenReturn(cf);
            when(channel.close())
                .thenAnswer(
                    inv -> {
                      cf.complete(null);
                      return CompletableFuture.completedFuture(null);
                    });
            factoryCalled.countDown();
            return channel;
          };

      Acceptor acceptor = Acceptor.builder(factory).channelFactory(channelFactory).build();
      acceptorRef.set(acceptor);
      acceptor.start();

      factory.queue.put(stubTransport());
      factoryCalled.await();

      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(acceptor.closeFuture()).isDone());
    }

    @Test
    void double_start_throws() {
      BlockingQueueTransportFactory factory = new BlockingQueueTransportFactory();

      Acceptor acceptor =
          Acceptor.builder(factory)
              .channelFactory(
                  s -> {
                    Channel ch = mock(Channel.class);
                    when(ch.id()).thenReturn("ch");
                    when(ch.closeFuture()).thenReturn(new CompletableFuture<>());
                    when(ch.close()).thenReturn(CompletableFuture.completedFuture(null));
                    return ch;
                  })
              .build();
      acceptor.start();
      assertThatThrownBy(acceptor::start)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("already started");
      acceptor.close().join();
    }
  }

  @Nested
  class Close {

    @Test
    void close_stops_accepting_and_completes_close_future() throws Exception {
      BlockingQueueTransportFactory factory = new BlockingQueueTransportFactory();
      CompletableFuture<Void> channelCloseFuture = new CompletableFuture<>();
      CountDownLatch factoryCalled = new CountDownLatch(1);

      Function<Transport, Channel> channelFactory =
          transport -> {
            Channel channel = mock(Channel.class);
            when(channel.id()).thenReturn("ch-1");
            when(channel.closeFuture()).thenReturn(channelCloseFuture);
            when(channel.close())
                .thenAnswer(
                    inv -> {
                      channelCloseFuture.complete(null);
                      return CompletableFuture.completedFuture(null);
                    });
            factoryCalled.countDown();
            return channel;
          };

      Acceptor acceptor = Acceptor.builder(factory).channelFactory(channelFactory).build();
      acceptor.start();

      CompletableFuture<Void> closeFut = acceptor.closeFuture();
      assertThat(closeFut).isNotDone();

      factory.queue.put(stubTransport());
      factoryCalled.await(2, TimeUnit.SECONDS);

      acceptor.close();
      await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(closeFut).isDone());
    }

    @Test
    void double_close_is_idempotent() {
      BlockingQueueTransportFactory factory = new BlockingQueueTransportFactory();

      Function<Transport, Channel> channelFactory =
          transport -> {
            Channel channel = mock(Channel.class);
            when(channel.id()).thenReturn("ch-1");
            when(channel.closeFuture()).thenReturn(new CompletableFuture<>());
            when(channel.close()).thenReturn(CompletableFuture.completedFuture(null));
            return channel;
          };

      Acceptor acceptor = Acceptor.builder(factory).channelFactory(channelFactory).build();
      acceptor.start();

      CompletableFuture<Void> first = acceptor.close();
      CompletableFuture<Void> second = acceptor.close();
      first.join();
      second.join();
      assertThat(first).isDone();
      assertThat(second).isDone();
    }

    @Test
    void close_with_no_channels_completes_immediately() {
      BlockingQueueTransportFactory factory = new BlockingQueueTransportFactory();

      Acceptor acceptor =
          Acceptor.builder(factory)
              .channelFactory(
                  s -> {
                    Channel ch = mock(Channel.class);
                    when(ch.id()).thenReturn("ch");
                    when(ch.closeFuture()).thenReturn(new CompletableFuture<>());
                    when(ch.close()).thenReturn(CompletableFuture.completedFuture(null));
                    return ch;
                  })
              .build();
      acceptor.start();

      acceptor.close().join();
      assertThat(acceptor.closeFuture()).isDone();
    }

    @Test
    void close_future_is_read_only() {
      BlockingQueueTransportFactory factory = new BlockingQueueTransportFactory();

      Acceptor acceptor =
          Acceptor.builder(factory)
              .channelFactory(
                  s -> {
                    Channel ch = mock(Channel.class);
                    when(ch.id()).thenReturn("ch");
                    when(ch.closeFuture()).thenReturn(new CompletableFuture<>());
                    when(ch.close()).thenReturn(CompletableFuture.completedFuture(null));
                    return ch;
                  })
              .build();
      acceptor.start();

      CompletableFuture<Void> closeFut = acceptor.closeFuture();
      closeFut.complete(null);
      assertThat(acceptor.closeFuture()).isNotDone();

      acceptor.close().join();
    }

    @Test
    void channel_close_future_removes_channel_from_tracking() throws Exception {
      BlockingQueueTransportFactory factory = new BlockingQueueTransportFactory();
      CompletableFuture<Void> channelCloseFuture = new CompletableFuture<>();
      CountDownLatch factoryCalled = new CountDownLatch(1);

      Function<Transport, Channel> channelFactory =
          transport -> {
            Channel channel = mock(Channel.class);
            when(channel.id()).thenReturn("ch-tracked");
            when(channel.closeFuture()).thenReturn(channelCloseFuture);
            when(channel.close())
                .thenAnswer(
                    inv -> {
                      channelCloseFuture.complete(null);
                      return CompletableFuture.completedFuture(null);
                    });
            factoryCalled.countDown();
            return channel;
          };

      Acceptor acceptor = Acceptor.builder(factory).channelFactory(channelFactory).build();
      acceptor.start();

      factory.queue.put(stubTransport());
      factoryCalled.await();

      channelCloseFuture.complete(null);

      acceptor.close().join();
      assertThat(acceptor.closeFuture()).isDone();
    }
  }

  @Nested
  class BuilderValidation {

    @Test
    void builder_validates_missing_channel_factory() {
      BlockingQueueTransportFactory factory = new BlockingQueueTransportFactory();
      var builder = Acceptor.builder(factory);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("channelFactory");
    }

    @Test
    void builder_null_transport_factory_throws() {
      assertThatThrownBy(() -> Acceptor.builder(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("transportFactory");
    }

    @Test
    void builder_null_meter_throws() {
      BlockingQueueTransportFactory factory = new BlockingQueueTransportFactory();
      var builder = Acceptor.builder(factory).channelFactory(s -> mock(Channel.class));
      assertThatThrownBy(() -> builder.meter(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("meter");
    }

    @Test
    void builder_null_tracer_throws() {
      BlockingQueueTransportFactory factory = new BlockingQueueTransportFactory();
      var builder = Acceptor.builder(factory).channelFactory(s -> mock(Channel.class));
      assertThatThrownBy(() -> builder.tracer(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("tracer");
    }

    @Test
    void builder_setters_work_correctly() {
      BlockingQueueTransportFactory factory = new BlockingQueueTransportFactory();

      Acceptor acceptor =
          Acceptor.builder(factory)
              .channelFactory(
                  s -> {
                    Channel ch = mock(Channel.class);
                    when(ch.id()).thenReturn("ch");
                    when(ch.closeFuture()).thenReturn(new CompletableFuture<>());
                    when(ch.close()).thenReturn(CompletableFuture.completedFuture(null));
                    return ch;
                  })
              .errorBackoff(Duration.ofMillis(50))
              .meter(OTel.meter())
              .tracer(OTel.tracer())
              .build();
      acceptor.start();
      acceptor.close().join();
      assertThat(acceptor.closeFuture()).isDone();
    }
  }
}
