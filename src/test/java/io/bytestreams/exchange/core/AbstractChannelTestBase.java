package io.bytestreams.exchange.core;

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
abstract class AbstractChannelTestBase {

  protected ServerSocket serverSocket;
  protected Socket clientRawSocket, serverRawSocket;
  protected SocketTransport transport;
  protected OutputStream serverOut;
  protected InputStream serverIn;
  protected Channel channel;
  @Mock ErrorHandler<String, String> errorHandler;

  @BeforeEach
  void baseSetUp() throws Exception {
    serverSocket = new ServerSocket(0);
    serverSocket.setSoTimeout(30_000);
    clientRawSocket = new Socket("localhost", serverSocket.getLocalPort());
    serverRawSocket = serverSocket.accept();
    transport = new SocketTransport(clientRawSocket);
    serverOut = serverRawSocket.getOutputStream();
    serverIn = serverRawSocket.getInputStream();
  }

  @AfterEach
  void baseTearDown() {
    if (channel != null) {
      channel.close();
    }
    Closeables.closeQuietly(clientRawSocket);
    Closeables.closeQuietly(serverRawSocket);
    Closeables.closeQuietly(serverSocket);
  }

  abstract Channel createChannel();

  abstract Channel createUninitializedChannel();

  // -- Start lifecycle --

  @Test
  void initial_status_is_init() {
    channel = createUninitializedChannel();
    assertThat(channel.status()).isEqualTo(ChannelStatus.INIT);
  }

  @Test
  void status_after_start_is_ready() {
    channel = createUninitializedChannel();
    channel.start();
    assertThat(channel.status()).isEqualTo(ChannelStatus.READY);
  }

  // -- Read loop error handling (tests AbstractChannel.readLoop) --

  @Test
  void read_loop_io_exception_calls_error_handler() throws Exception {
    transport = spy(transport);
    when(transport.inputStream()).thenThrow(new IOException("read failed"));
    channel = createChannel();
    verify(errorHandler, timeout(1000)).stopOnError(any(), any());
  }

  @Test
  void read_loop_io_exception_closes_transport_when_handler_returns_true() throws Exception {
    transport = spy(transport);
    when(transport.inputStream()).thenThrow(new IOException("read failed"));
    when(errorHandler.stopOnError(any(), any())).thenReturn(true);
    channel = createChannel();
    verify(transport, timeout(1000)).close();
  }

  @Test
  void read_loop_continues_when_handler_returns_false() throws Exception {
    transport = spy(transport);
    when(transport.inputStream()).thenThrow(new IOException("read failed"));
    channel = createChannel();
    verify(errorHandler, timeout(1000).atLeastOnce()).stopOnError(any(), any());
  }

  // -- Read loop safety net --

  @Test
  void read_loop_outer_catch_throwable_closes_transport() throws Exception {
    transport = spy(transport);
    when(transport.inputStream()).thenThrow(new Error("unexpected error"));
    channel = createChannel();
    verify(transport, timeout(1000)).close();
  }

  @Test
  void read_loop_outer_catch_throwable_during_shutdown_completes_cleanly() throws Exception {
    CountDownLatch readLoopLatch = new CountDownLatch(1);
    CountDownLatch closingLatch = new CountDownLatch(1);
    transport = spy(transport);
    lenient()
        .when(transport.inputStream())
        .thenAnswer(
            inv -> {
              readLoopLatch.countDown();
              closingLatch.await();
              throw new Error("error during shutdown");
            });
    channel = createChannel();
    readLoopLatch.await();
    channel.close();
    closingLatch.countDown(); // release the Error now that we're shutting down
    await()
        .atMost(Duration.ofMillis(2000))
        .untilAsserted(() -> assertThat(channel.status()).isEqualTo(ChannelStatus.CLOSED));
  }

  // -- Start lifecycle --

  @Test
  void start_when_already_started_throws() {
    channel = createChannel();
    assertThatThrownBy(() -> channel.start())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("already started");
  }

  @Test
  void close_from_init_completes_immediately() {
    channel = createUninitializedChannel();
    assertThat(channel.status()).isEqualTo(ChannelStatus.INIT);
    CompletableFuture<Void> closeFuture = channel.close();
    assertThat(closeFuture).succeedsWithin(Duration.ofMillis(500));
    assertThat(channel.status()).isEqualTo(ChannelStatus.CLOSED);
  }

  // -- Close lifecycle (tests AbstractChannel.close/closeFuture) --

  @Test
  void close_is_idempotent() {
    channel = createChannel();
    CompletableFuture<Void> first = channel.close();
    assertThat(first).succeedsWithin(Duration.ofMillis(500));
    assertThat(channel.close()).succeedsWithin(Duration.ZERO);
  }

  @Test
  void closeFuture_is_read_only() {
    channel = createChannel();
    CompletableFuture<Void> closeFuture = channel.closeFuture();
    closeFuture.complete(null);
    assertThat(channel.status()).isNotEqualTo(ChannelStatus.CLOSED);
    channel.close();
  }
}
