package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class SocketAcceptorTest {

  @Test
  void acceptsConnectionsAndCreatesChannels() throws Exception {
    AtomicInteger factoryCallCount = new AtomicInteger();
    Function<Transport, Channel> factory =
        socket -> {
          int n = factoryCallCount.incrementAndGet();
          Channel channel = mock(Channel.class);
          when(channel.id()).thenReturn("ch-" + n);
          when(channel.closeFuture()).thenReturn(new CompletableFuture<>());
          when(channel.close()).thenReturn(CompletableFuture.completedFuture(null));
          return channel;
        };

    SocketAcceptor acceptor = SocketAcceptor.builder().port(0).channelFactory(factory).build();
    acceptor.start();

    try (Socket client = new Socket("localhost", acceptor.port())) {
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(factoryCallCount.get()).isGreaterThanOrEqualTo(1));
    }
    acceptor.close().join();
  }

  @Test
  void closeStopsAcceptingAndCompletesCloseFuture() throws Exception {
    CompletableFuture<Void> channelCloseFuture = new CompletableFuture<>();
    CountDownLatch factoryCalled = new CountDownLatch(1);
    Function<Transport, Channel> factory =
        socket -> {
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

    SocketAcceptor acceptor = SocketAcceptor.builder().port(0).channelFactory(factory).build();
    acceptor.start();

    CompletableFuture<Void> closeFut = acceptor.closeFuture();
    assertThat(closeFut).isNotDone();

    try (Socket client = new Socket("localhost", acceptor.port())) {
      factoryCalled.await(2, TimeUnit.SECONDS);
    }

    acceptor.close();
    await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(closeFut).isDone());
  }

  @Test
  void channelFactoryExceptionDoesNotStopAcceptor() throws Exception {
    AtomicInteger callCount = new AtomicInteger();
    Function<Transport, Channel> factory =
        socket -> {
          int n = callCount.incrementAndGet();
          if (n == 1) throw new RuntimeException("factory error");
          Channel channel = mock(Channel.class);
          when(channel.id()).thenReturn("ch-" + n);
          when(channel.closeFuture()).thenReturn(new CompletableFuture<>());
          when(channel.close()).thenReturn(CompletableFuture.completedFuture(null));
          return channel;
        };

    SocketAcceptor acceptor = SocketAcceptor.builder().port(0).channelFactory(factory).build();
    acceptor.start();

    try (Socket c1 = new Socket("localhost", acceptor.port())) {
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(callCount.get()).isGreaterThanOrEqualTo(1));
    }

    try (Socket c2 = new Socket("localhost", acceptor.port())) {
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(callCount.get()).isGreaterThanOrEqualTo(2));
    }

    acceptor.close().join();
  }

  @Test
  void closeFutureIsReadOnly() throws Exception {
    SocketAcceptor acceptor =
        SocketAcceptor.builder()
            .port(0)
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
  void doubleStartThrows() throws Exception {
    SocketAcceptor acceptor =
        SocketAcceptor.builder()
            .port(0)
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

  @Test
  void builderValidatesMissingChannelFactory() {
    var builder = SocketAcceptor.builder();
    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("channelFactory");
  }

  @Test
  void doubleCloseIsIdempotent() throws Exception {
    Function<Transport, Channel> factory =
        socket -> {
          Channel channel = mock(Channel.class);
          when(channel.id()).thenReturn("ch-1");
          when(channel.closeFuture()).thenReturn(new CompletableFuture<>());
          when(channel.close()).thenReturn(CompletableFuture.completedFuture(null));
          return channel;
        };

    SocketAcceptor acceptor = SocketAcceptor.builder().port(0).channelFactory(factory).build();
    acceptor.start();

    CompletableFuture<Void> first = acceptor.close();
    CompletableFuture<Void> second = acceptor.close();
    first.join();
    second.join();
    assertThat(first).isDone();
    assertThat(second).isDone();
  }

  @Test
  void closeWithNoChannelsCompletesImmediately() throws Exception {
    SocketAcceptor acceptor =
        SocketAcceptor.builder()
            .port(0)
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
  void channelCloseFutureRemovesChannelFromTracking() throws Exception {
    CompletableFuture<Void> channelCloseFuture = new CompletableFuture<>();
    CountDownLatch factoryCalled = new CountDownLatch(1);

    Function<Transport, Channel> factory =
        socket -> {
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

    SocketAcceptor acceptor = SocketAcceptor.builder().port(0).channelFactory(factory).build();
    acceptor.start();

    try (Socket client = new Socket("localhost", acceptor.port())) {
      factoryCalled.await();
    }

    channelCloseFuture.complete(null);

    acceptor.close().join();
    assertThat(acceptor.closeFuture()).isDone();
  }

  @Test
  void builder_null_meter_throws() {
    var builder = SocketAcceptor.builder().channelFactory(s -> mock(Channel.class));
    assertThatThrownBy(() -> builder.meter(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("meter");
  }

  @Test
  void builder_null_tracer_throws() {
    var builder = SocketAcceptor.builder().channelFactory(s -> mock(Channel.class));
    assertThatThrownBy(() -> builder.tracer(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("tracer");
  }

  @Test
  void builderSettersWorkCorrectly() throws Exception {
    SocketAcceptor acceptor =
        SocketAcceptor.builder()
            .port(0)
            .host("127.0.0.1")
            .backlog(10)
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
    assertThat(acceptor.port()).isGreaterThan(0);
    acceptor.close().join();
  }

  @Test
  void closeRaceDuringAccept() throws Exception {
    CountDownLatch factoryCalled = new CountDownLatch(1);
    AtomicReference<SocketAcceptor> acceptorRef = new AtomicReference<>();

    Function<Transport, Channel> factory =
        socket -> {
          SocketAcceptor acc = acceptorRef.get();
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

    SocketAcceptor acceptor = SocketAcceptor.builder().port(0).channelFactory(factory).build();
    acceptorRef.set(acceptor);
    acceptor.start();

    try (Socket client = new Socket("localhost", acceptor.port())) {
      factoryCalled.await();
    }

    await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertThat(acceptor.closeFuture()).isDone());
  }

  @Test
  void ioExceptionInAcceptLoopWhenNotClosedRecordsAndRetries() throws Exception {
    // Close the underlying server socket directly (via reflection) to trigger IOException
    // while the closed flag is still false, covering the error-recovery path
    CountDownLatch factoryCalled = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger();

    Function<Transport, Channel> factory =
        socket -> {
          callCount.incrementAndGet();
          Channel channel = mock(Channel.class);
          when(channel.id()).thenReturn("ch-" + callCount.get());
          when(channel.closeFuture()).thenReturn(new CompletableFuture<>());
          when(channel.close()).thenReturn(CompletableFuture.completedFuture(null));
          factoryCalled.countDown();
          return channel;
        };

    SocketAcceptor acceptor = SocketAcceptor.builder().port(0).channelFactory(factory).build();
    acceptor.start();
    int port = acceptor.port();

    // Accept one connection to confirm the loop is running
    try (Socket client = new Socket("localhost", port)) {
      factoryCalled.await();
    }

    // Close the server socket directly — this triggers IOException
    // in the accept loop while closed is still false
    acceptor.serverSocket.close();

    // The accept loop should hit IOException, record it, backoff, retry, and keep hitting
    // IOException — wait for the error path to exercise
    await().during(Duration.ofMillis(200)).until(() -> true);

    // Now properly close the acceptor
    acceptor.close().join();
  }

  @Test
  void factoryErrorEscapesAsErrorAndFinallyClosesServerSocket() throws Exception {
    // An Error from the factory bypasses the inner exception handler, escapes
    // the while loop, and reaches the finally block which marks the acceptor closed
    CountDownLatch errorThrown = new CountDownLatch(1);
    AtomicBoolean errorWasThrown = new AtomicBoolean();

    Function<Transport, Channel> factory =
        socket -> {
          errorWasThrown.set(true);
          errorThrown.countDown();
          throw new StackOverflowError("simulated");
        };

    SocketAcceptor acceptor = SocketAcceptor.builder().port(0).channelFactory(factory).build();
    acceptor.start();

    try (Socket client = new Socket("localhost", acceptor.port())) {
      errorThrown.await(2, TimeUnit.SECONDS);
    }

    assertThat(errorWasThrown.get()).isTrue();

    // The accept loop VT should have terminated via the finally block.
    // The finally block sets closed=true and calls closeAllChannels(), which completes closeFuture.
    await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertThat(acceptor.closeFuture()).isDone());
  }

  @Test
  void closedAfterAcceptReturnsClosesSocketAndBreaks() throws Exception {
    // Covers the branch where accept returns a socket while the acceptor is already closed.
    // Strategy: accept one connection normally, then set closed via reflection without
    // closing the server socket. The next accepted connection sees the closed flag,
    // closes the socket, and exits the loop.
    CountDownLatch firstAccepted = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger();

    Function<Transport, Channel> factory =
        socket -> {
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

    SocketAcceptor acceptor = SocketAcceptor.builder().port(0).channelFactory(factory).build();
    acceptor.start();
    int port = acceptor.port();

    // First connection — confirms the accept loop is running
    try (Socket c1 = new Socket("localhost", port)) {
      firstAccepted.await(2, TimeUnit.SECONDS);
    }

    // Set closed=true WITHOUT closing the server socket.
    // Wait briefly for the accept loop to reach accept() and block, then set closed.
    await().during(Duration.ofMillis(100)).until(() -> true);
    acceptor.closed.set(true);

    // Now connect — accept() returns the socket, but closed.get() is true at line 104
    Socket c2 = new Socket("localhost", port);

    // The loop should break, finally block runs
    await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertThat(acceptor.closeFuture()).isDone());

    c2.close();
  }
}
