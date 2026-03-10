package io.bytestreams.exchange.core;

import static io.bytestreams.exchange.core.TestFixture.CHANNEL_TYPE;
import static io.bytestreams.exchange.core.TestFixture.ERROR_TYPE;
import static io.bytestreams.exchange.core.TestFixture.MESSAGE_TYPE;
import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.equalTo;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ServerChannelTest extends AbstractChannelTestBase {

  @Override
  ServerChannel<String, String> createChannel() {
    return createChannel((req, future) -> {});
  }

  @Override
  ServerChannel<String, String> createUninitializedChannel() {
    return createUninitializedChannel((req, future) -> {});
  }

  ServerChannel<String, String> createChannel(RequestHandler<String, String> handler) {
    var ch = createUninitializedChannel(handler);
    ch.start();
    return ch;
  }

  private ServerChannel<String, String> createUninitializedChannel(
      RequestHandler<String, String> handler) {
    return new ServerChannel<>(
        transport,
        Integer.MAX_VALUE,
        TestFixture.FRAMED_READER,
        TestFixture.FRAMED_WRITER,
        handler,
        errorHandler,
        Duration.ofSeconds(30),
        ServerChannel.DEFAULT_ERROR_BACKOFF_NANOS,
        OTel.meter(),
        OTel.tracer());
  }

  // -- Builder --

  @Nested
  class BuilderTests {
    @Test
    void builder_missing_transport_throws() {
      assertThatThrownBy(
              () ->
                  ServerChannel.<String, String>builder()
                      .requestReader(TestFixture.FRAMED_READER)
                      .responseWriter(TestFixture.FRAMED_WRITER)
                      .requestHandler((req, future) -> {})
                      .build())
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("transport");
    }

    @Test
    void builder_missing_requestReader_throws() {
      assertThatThrownBy(
              () ->
                  ServerChannel.<String, String>builder()
                      .transport(transport)
                      .responseWriter(TestFixture.FRAMED_WRITER)
                      .requestHandler((req, future) -> {})
                      .build())
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("requestReader");
    }

    @Test
    void builder_missing_responseWriter_throws() {
      assertThatThrownBy(
              () ->
                  ServerChannel.<String, String>builder()
                      .transport(transport)
                      .requestReader(TestFixture.FRAMED_READER)
                      .requestHandler((req, future) -> {})
                      .build())
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("responseWriter");
    }

    @Test
    void builder_missing_requestHandler_throws() {
      assertThatThrownBy(
              () ->
                  ServerChannel.<String, String>builder()
                      .transport(transport)
                      .requestReader(TestFixture.FRAMED_READER)
                      .responseWriter(TestFixture.FRAMED_WRITER)
                      .build())
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("requestHandler");
    }

    @Test
    void builder_zero_writeBufferSize_throws() {
      assertThatThrownBy(
              () ->
                  ServerChannel.<String, String>builder()
                      .transport(transport)
                      .requestReader(TestFixture.FRAMED_READER)
                      .responseWriter(TestFixture.FRAMED_WRITER)
                      .requestHandler((req, future) -> {})
                      .writeBufferSize(0)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("writeBufferSize");
    }

    @Test
    void builder_null_defaultTimeout_throws() {
      assertThatThrownBy(
              () ->
                  ServerChannel.<String, String>builder()
                      .transport(transport)
                      .requestReader(TestFixture.FRAMED_READER)
                      .responseWriter(TestFixture.FRAMED_WRITER)
                      .requestHandler((req, future) -> {})
                      .defaultTimeout(null)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("defaultTimeout");
    }

    @Test
    void builder_zero_defaultTimeout_throws() {
      assertThatThrownBy(
              () ->
                  ServerChannel.<String, String>builder()
                      .transport(transport)
                      .requestReader(TestFixture.FRAMED_READER)
                      .responseWriter(TestFixture.FRAMED_WRITER)
                      .requestHandler((req, future) -> {})
                      .defaultTimeout(Duration.ZERO)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("defaultTimeout");
    }

    @Test
    void builder_null_meter_throws() {
      assertThatThrownBy(
              () ->
                  ServerChannel.<String, String>builder()
                      .transport(transport)
                      .requestReader(TestFixture.FRAMED_READER)
                      .responseWriter(TestFixture.FRAMED_WRITER)
                      .requestHandler((req, future) -> {})
                      .meter(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("meter");
    }

    @Test
    void builder_null_tracer_throws() {
      assertThatThrownBy(
              () ->
                  ServerChannel.<String, String>builder()
                      .transport(transport)
                      .requestReader(TestFixture.FRAMED_READER)
                      .responseWriter(TestFixture.FRAMED_WRITER)
                      .requestHandler((req, future) -> {})
                      .tracer(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("tracer");
    }

    @Test
    void builder_creates_channel_with_all_options() {
      var ch =
          ServerChannel.<String, String>builder()
              .transport(transport)
              .requestReader(TestFixture.FRAMED_READER)
              .responseWriter(TestFixture.FRAMED_WRITER)
              .requestHandler((req, future) -> {})
              .writeBufferSize(4096)
              .errorHandler(new ErrorHandler<>() {})
              .defaultTimeout(Duration.ofSeconds(5))
              .errorBackoff(Duration.ofMillis(50))
              .meter(OTel.meter())
              .tracer(OTel.tracer())
              .build();
      channel = ch;
      assertThat(ch.status()).isEqualTo(ChannelStatus.INIT);
    }

    @Test
    void builder_with_requestHandler_applies_it() throws Exception {
      CompletableFuture<String> received = new CompletableFuture<>();
      var ch =
          ServerChannel.<String, String>builder()
              .transport(transport)
              .requestReader(TestFixture.FRAMED_READER)
              .responseWriter(TestFixture.FRAMED_WRITER)
              .requestHandler((req, future) -> received.complete(req))
              .build();
      ch.start();
      channel = ch;
      TestFixture.writeFramed("ping", serverOut);
      assertThat(received).succeedsWithin(Duration.ofMillis(500)).isEqualTo("ping");
    }
  }

  // -- ReadLoop --

  @Nested
  class ReadLoop {
    @Test
    void dispatches_request_to_handler() throws Exception {
      CompletableFuture<String> received = new CompletableFuture<>();
      var ch = createChannel((req, future) -> received.complete(req));
      channel = ch;
      TestFixture.writeFramed("ping", serverOut);
      assertThat(received).succeedsWithin(Duration.ofMillis(500)).isEqualTo("ping");
    }
  }

  // -- WriteLoop --

  @Nested
  class WriteLoop {
    @Test
    void handler_completes_future_writes_response_to_transport() throws Exception {
      var ch = createChannel((req, future) -> future.complete("pong"));
      channel = ch;
      TestFixture.writeFramed("ping", serverOut);
      assertThat(TestFixture.readFramed(serverIn)).isEqualTo("pong");
    }

    @Test
    void write_loop_io_exception_calls_error_handler() throws Exception {
      IOException error = new IOException("write failed");
      transport = spy(transport);
      when(transport.outputStream()).thenThrow(error);
      var ch = createChannel((req, future) -> future.complete("pong"));
      channel = ch;
      TestFixture.writeFramed("ping", serverOut);
      verify(errorHandler, timeout(1000)).stopOnError(eq(ch), any());
    }

    @Test
    void write_loop_io_exception_closes_transport_when_handler_returns_true() throws Exception {
      IOException error = new IOException("write failed");
      transport = spy(transport);
      when(transport.outputStream()).thenThrow(error);
      when(errorHandler.stopOnError(any(), any())).thenReturn(true);
      var ch = createChannel((req, future) -> future.complete("pong"));
      channel = ch;
      TestFixture.writeFramed("ping", serverOut);
      verify(transport, timeout(1000).atLeastOnce()).close();
    }

    @Test
    void write_loop_continues_when_handler_returns_false() throws Exception {
      IOException error = new IOException("write failed");
      transport = spy(transport);
      when(transport.outputStream()).thenThrow(error);
      var ch = createChannel((req, future) -> future.complete("pong"));
      channel = ch;
      TestFixture.writeFramed("ping", serverOut);
      verify(errorHandler, timeout(1000).atLeastOnce()).stopOnError(eq(ch), any());
    }

    @Test
    void close_waits_for_pending_write_queue_to_drain() throws Exception {
      CountDownLatch writeLatch = new CountDownLatch(1);
      transport = spy(transport);
      when(transport.outputStream())
          .thenAnswer(
              inv -> {
                writeLatch.await();
                return clientRawSocket.getOutputStream();
              })
          .thenCallRealMethod();
      var ch = createChannel((req, future) -> future.complete("resp-" + req));
      channel = ch;
      TestFixture.writeFramed("req1", serverOut);
      TestFixture.writeFramed("req2", serverOut);
      // Wait for the write loop to call outputStream() (blocked by latch)
      verify(transport, timeout(500)).outputStream();
      CompletableFuture<Void> close = ch.close();
      await().during(Duration.ofMillis(200)).until(() -> !close.isDone());
      writeLatch.countDown();
      assertThat(TestFixture.readFramed(serverIn)).isEqualTo("resp-req1");
      assertThat(TestFixture.readFramed(serverIn)).isEqualTo("resp-req2");
    }
  }

  // -- Null response --

  @Nested
  class NullResponse {
    @Test
    void null_response_does_not_crash_channel() throws Exception {
      CompletableFuture<Void> done = new CompletableFuture<>();
      var ch =
          createChannel(
              (req, future) -> {
                future.complete(null);
                done.complete(null);
              });
      channel = ch;
      TestFixture.writeFramed("ping", serverOut);
      done.get();
      // Channel should still be running after null response
      await().during(Duration.ofMillis(200)).until(() -> ch.status() != ChannelStatus.CLOSED);
    }
  }

  // -- Handler timeout --

  @Nested
  class HandlerTimeout {
    @Test
    void handler_future_times_out_when_not_completed() throws Exception {
      // Use a short timeout so the test doesn't take 30s
      CompletableFuture<CompletableFuture<String>> futureHolder = new CompletableFuture<>();
      var ch =
          new ServerChannel<>(
              transport,
              Integer.MAX_VALUE,
              TestFixture.FRAMED_READER,
              TestFixture.FRAMED_WRITER,
              (req, future) -> futureHolder.complete(future),
              errorHandler,
              Duration.ofMillis(100),
              ServerChannel.DEFAULT_ERROR_BACKOFF_NANOS,
              OTel.meter(),
              OTel.tracer());
      ch.start();
      channel = ch;
      TestFixture.writeFramed("req", serverOut);
      CompletableFuture<String> handlerFuture = futureHolder.get(1, TimeUnit.SECONDS);
      // The future should time out since handler never completes it
      assertThat(handlerFuture)
          .failsWithin(Duration.ofSeconds(2))
          .withThrowableThat()
          .withCauseInstanceOf(java.util.concurrent.TimeoutException.class);
    }

    @Test
    void close_waits_for_active_handler_then_completes() throws Exception {
      CompletableFuture<CompletableFuture<String>> futureHolder = new CompletableFuture<>();
      var ch =
          new ServerChannel<>(
              transport,
              Integer.MAX_VALUE,
              TestFixture.FRAMED_READER,
              TestFixture.FRAMED_WRITER,
              (req, future) -> futureHolder.complete(future),
              errorHandler,
              Duration.ofMillis(200),
              ServerChannel.DEFAULT_ERROR_BACKOFF_NANOS,
              OTel.meter(),
              OTel.tracer());
      ch.start();
      channel = ch;
      TestFixture.writeFramed("req", serverOut);
      futureHolder.get(1, TimeUnit.SECONDS);
      // Close while handler is active — should wait for timeout, then complete
      CompletableFuture<Void> closeFuture = ch.close();
      assertThat(closeFuture).succeedsWithin(Duration.ofSeconds(2));
      assertThat(ch.status()).isEqualTo(ChannelStatus.CLOSED);
    }
  }

  // -- Close lifecycle --

  @Nested
  class CloseLifecycle {
    @Test
    void close_sets_closing_then_closed() throws Exception {
      var ch = createChannel();
      channel = ch;
      CompletableFuture<Void> closeFuture = ch.close();
      assertThat(closeFuture).succeedsWithin(Duration.ofMillis(500));
      assertThat(ch.status()).isEqualTo(ChannelStatus.CLOSED);
    }

    @Test
    void close_does_not_set_closing_when_already_closed() throws Exception {
      var ch = createChannel();
      channel = ch;
      ch.status.set(ChannelStatus.CLOSED);
      CompletableFuture<Void> future = ch.close();
      assertThat(ch.status()).isEqualTo(ChannelStatus.CLOSED);
      assertThat(future).succeedsWithin(Duration.ofMillis(500));
      assertThat(ch.status()).isEqualTo(ChannelStatus.CLOSED);
    }
  }

  // -- Hard-to-cover paths --

  @Nested
  class HardToCoverPaths {

    /**
     * Tests Path 1: InterruptedException in the whenComplete callback's writeQueue.put().
     *
     * <p>The whenComplete callback runs on the thread that calls future.complete(). If that thread's
     * interrupt flag is set, LinkedBlockingQueue.put() immediately throws InterruptedException.
     */
    @Test
    void interrupted_in_whenComplete_put_calls_error_handler() throws Exception {
      CompletableFuture<CompletableFuture<String>> futureHolder = new CompletableFuture<>();
      var ch = createChannel((req, future) -> futureHolder.complete(future));
      channel = ch;
      TestFixture.writeFramed("req", serverOut);
      CompletableFuture<String> handlerFuture = futureHolder.get(1, TimeUnit.SECONDS);

      // Complete the future from a pre-interrupted thread so put() throws InterruptedException
      // immediately (LinkedBlockingQueue.put() checks interrupt status before blocking)
      CompletableFuture<Void> completed = new CompletableFuture<>();
      VT.start(
          () -> {
            Thread.currentThread().interrupt();
            handlerFuture.complete("resp");
            completed.complete(null);
          },
          "pre-interrupted-completer");
      completed.get(1, TimeUnit.SECONDS);

      verify(errorHandler, timeout(1000)).stopOnError(eq(ch), any());
    }

    /**
     * Tests Path 1 (false branch): when errorHandler.onError returns false the readFuture is
     * completed exceptionally and the transport is closed.
     */
    @Test
    void interrupted_in_whenComplete_put_closes_transport_when_handler_returns_true()
        throws Exception {
      transport = spy(transport);
      when(errorHandler.stopOnError(any(), any())).thenReturn(true);

      CompletableFuture<CompletableFuture<String>> futureHolder = new CompletableFuture<>();
      var ch = createChannel((req, future) -> futureHolder.complete(future));
      channel = ch;
      TestFixture.writeFramed("req", serverOut);
      CompletableFuture<String> handlerFuture = futureHolder.get(1, TimeUnit.SECONDS);

      CompletableFuture<Void> completed = new CompletableFuture<>();
      VT.start(
          () -> {
            Thread.currentThread().interrupt();
            handlerFuture.complete("resp");
            completed.complete(null);
          },
          "pre-interrupted-completer-close");
      completed.get(1, TimeUnit.SECONDS);

      verify(transport, timeout(1000).atLeastOnce()).close();
    }

    /**
     * Tests Path 1 (false branch): when errorHandler.stopOnError returns false on
     * InterruptedException, the channel continues (transport is NOT closed by the whenComplete
     * handler).
     */
    @Test
    void interrupted_in_whenComplete_put_continues_when_handler_returns_false() throws Exception {
      CompletableFuture<CompletableFuture<String>> futureHolder = new CompletableFuture<>();
      var ch = createChannel((req, future) -> futureHolder.complete(future));
      channel = ch;
      TestFixture.writeFramed("req", serverOut);
      CompletableFuture<String> handlerFuture = futureHolder.get(1, TimeUnit.SECONDS);

      CompletableFuture<Void> completed = new CompletableFuture<>();
      VT.start(
          () -> {
            Thread.currentThread().interrupt();
            handlerFuture.complete("resp");
            completed.complete(null);
          },
          "pre-interrupted-completer-continue");
      completed.get(1, TimeUnit.SECONDS);

      verify(errorHandler, timeout(1000)).stopOnError(eq(ch), any());
    }

    /**
     * Tests Path 2: outer catch(Throwable) in writeLoop.
     *
     * <p>When transport.outputStream() throws a RuntimeException (not IOException/InterruptedException),
     * it bypasses the inner catch and is caught by the outer Throwable safety net, which completes
     * writeFuture exceptionally and closes the transport.
     */
    @Test
    void write_loop_outer_catch_throwable_closes_transport() throws Exception {
      RuntimeException boom = new RuntimeException("unexpected runtime error");
      transport = spy(transport);
      when(transport.outputStream()).thenThrow(boom);
      var ch = createChannel((req, future) -> future.complete("resp"));
      channel = ch;
      TestFixture.writeFramed("req", serverOut);
      // The outer catch(Throwable) closes transport and completes writeFuture exceptionally
      verify(transport, timeout(1000).atLeastOnce()).close();
    }
  }

  // -- OTel Metrics --

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

    private ServerChannel<String, String> createMetricChannel() {
      return createMetricChannel((req, future) -> {});
    }

    private ServerChannel<String, String> createMetricChannel(
        RequestHandler<String, String> handler) {
      var ch =
          new ServerChannel<>(
              transport,
              Integer.MAX_VALUE,
              TestFixture.FRAMED_READER,
              TestFixture.FRAMED_WRITER,
              handler,
              errorHandler,
              Duration.ofSeconds(30),
              ServerChannel.DEFAULT_ERROR_BACKOFF_NANOS,
              meterProvider.get(OTel.NAMESPACE),
              OTel.tracer());
      ch.start();
      return ch;
    }

    @Test
    void request_active_incremented_when_request_received() throws Exception {
      CountDownLatch handlerLatch = new CountDownLatch(1);
      var ch =
          createMetricChannel(
              (req, future) -> {
                // Hold the future open so request.active stays incremented
                handlerLatch.countDown();
              });
      channel = ch;
      TestFixture.writeFramed("hello", serverOut);
      handlerLatch.await();
      await()
          .atMost(Duration.ofMillis(500))
          .untilAsserted(
              () ->
                  TestFixture.assertLongSum(
                      metricReader,
                      "request.active",
                      1,
                      equalTo(CHANNEL_TYPE, "server"),
                      equalTo(MESSAGE_TYPE, "String")));
    }

    @Test
    void request_active_decremented_on_completion() throws Exception {
      CompletableFuture<String> done = new CompletableFuture<>();
      var ch =
          createMetricChannel(
              (req, future) -> {
                future.complete("resp");
                done.complete(null);
              });
      channel = ch;
      TestFixture.writeFramed("hello", serverOut);
      done.get();
      await()
          .atMost(Duration.ofMillis(500))
          .untilAsserted(
              () ->
                  TestFixture.assertLongSum(
                      metricReader,
                      "request.active",
                      0,
                      equalTo(CHANNEL_TYPE, "server"),
                      equalTo(MESSAGE_TYPE, "String")));
    }

    @Test
    void request_total_incremented_on_completion() throws Exception {
      CompletableFuture<String> done = new CompletableFuture<>();
      var ch =
          createMetricChannel(
              (req, future) -> {
                future.complete("resp");
                done.complete(null);
              });
      channel = ch;
      TestFixture.writeFramed("hello", serverOut);
      done.get();
      await()
          .atMost(Duration.ofMillis(500))
          .untilAsserted(
              () ->
                  TestFixture.assertLongSum(
                      metricReader,
                      "request.total",
                      1,
                      equalTo(CHANNEL_TYPE, "server"),
                      equalTo(MESSAGE_TYPE, "String")));
    }

    @Test
    void request_duration_recorded_on_completion() throws Exception {
      CompletableFuture<String> done = new CompletableFuture<>();
      var ch =
          createMetricChannel(
              (req, future) -> {
                future.complete("resp");
                done.complete(null);
              });
      channel = ch;
      TestFixture.writeFramed("hello", serverOut);
      done.get();
      await()
          .atMost(Duration.ofMillis(500))
          .untilAsserted(
              () ->
                  TestFixture.assertHistogram(
                      metricReader,
                      "request.duration",
                      1,
                      equalTo(CHANNEL_TYPE, "server"),
                      equalTo(MESSAGE_TYPE, "String")));
    }

    @Test
    void request_errors_incremented_on_failure() throws Exception {
      CompletableFuture<String> done = new CompletableFuture<>();
      var ch =
          createMetricChannel(
              (req, future) -> {
                future.completeExceptionally(new RuntimeException("handler error"));
                done.complete(null);
              });
      channel = ch;
      TestFixture.writeFramed("hello", serverOut);
      done.get();
      await()
          .atMost(Duration.ofMillis(500))
          .untilAsserted(
              () ->
                  TestFixture.assertLongSum(
                      metricReader,
                      "request.errors",
                      1,
                      equalTo(CHANNEL_TYPE, "server"),
                      equalTo(MESSAGE_TYPE, "String"),
                      equalTo(ERROR_TYPE, "RuntimeException")));
    }
  }

  // -- Tracing --

  @Nested
  class Tracing {
    private InMemorySpanExporter spanExporter;
    private SdkTracerProvider tracerProvider;

    @BeforeEach
    void setUp() {
      spanExporter = InMemorySpanExporter.create();
      tracerProvider =
          SdkTracerProvider.builder()
              .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
              .build();
    }

    @AfterEach
    void tearDown() {
      tracerProvider.close();
    }

    private ServerChannel<String, String> createTracedChannel() {
      return createTracedChannel((req, future) -> {});
    }

    private ServerChannel<String, String> createTracedChannel(
        RequestHandler<String, String> handler) {
      var ch =
          new ServerChannel<>(
              transport,
              Integer.MAX_VALUE,
              TestFixture.FRAMED_READER,
              TestFixture.FRAMED_WRITER,
              handler,
              errorHandler,
              Duration.ofSeconds(30),
              ServerChannel.DEFAULT_ERROR_BACKOFF_NANOS,
              OTel.meter(),
              tracerProvider.get("test"));
      ch.start();
      return ch;
    }

    @Test
    void channel_span_created_with_SERVER_kind() {
      var ch = createTracedChannel();
      channel = ch;
      ch.close();
      await()
          .atMost(Duration.ofSeconds(2))
          .until(
              () ->
                  spanExporter.getFinishedSpanItems().stream()
                      .anyMatch(s -> s.getName().equals("server")));
      SpanData span =
          spanExporter.getFinishedSpanItems().stream()
              .filter(s -> s.getName().equals("server"))
              .findFirst()
              .orElseThrow();
      assertThat(span).hasName("server").hasKind(SpanKind.SERVER);
      assertThat(span.getAttributes().get(OTel.CHANNEL_TYPE)).isEqualTo("server");
    }

    @Test
    void channel_span_ends_ok_on_clean_close() {
      var ch = createTracedChannel();
      channel = ch;
      ch.close();
      await()
          .atMost(Duration.ofSeconds(2))
          .until(
              () ->
                  spanExporter.getFinishedSpanItems().stream()
                      .anyMatch(s -> s.getName().equals("server")));
      SpanData span =
          spanExporter.getFinishedSpanItems().stream()
              .filter(s -> s.getName().equals("server"))
              .findFirst()
              .orElseThrow();
      assertThat(span).hasStatus(StatusData.unset());
    }

    @Test
    void handle_span_linked_to_channel_span() throws Exception {
      var ch = createTracedChannel((req, future) -> future.complete("pong"));
      channel = ch;
      TestFixture.writeFramed("ping", serverOut);
      await()
          .atMost(Duration.ofSeconds(2))
          .until(
              () ->
                  spanExporter.getFinishedSpanItems().stream()
                      .anyMatch(s -> s.getName().equals("handle")));
      SpanData handleSpan =
          spanExporter.getFinishedSpanItems().stream()
              .filter(s -> s.getName().equals("handle"))
              .findFirst()
              .orElseThrow();
      assertThat(handleSpan.getLinks()).hasSize(1);
      assertThat(handleSpan.getAttributes().get(OTel.MESSAGE_TYPE)).isEqualTo("String");
    }

    @Test
    void handle_span_linked_to_channel_span_by_span_id() throws Exception {
      CountDownLatch handlerInvokedLatch = new CountDownLatch(1);
      var ch =
          createTracedChannel(
              (req, future) -> {
                handlerInvokedLatch.countDown();
                future.complete("pong");
              });
      channel = ch;
      TestFixture.writeFramed("ping", serverOut);
      // Wait for handler to be invoked before closing, to avoid a race where close() sets CLOSING
      // before the read loop VT starts its first iteration, causing it to exit without processing.
      handlerInvokedLatch.await(2, TimeUnit.SECONDS);
      // Close the server side so the read loop gets an IOException on the next read.
      serverOut.close();
      ch.close();
      await()
          .atMost(Duration.ofSeconds(5))
          .until(
              () ->
                  spanExporter.getFinishedSpanItems().stream()
                          .anyMatch(s -> s.getName().equals("handle"))
                      && spanExporter.getFinishedSpanItems().stream()
                          .anyMatch(s -> s.getName().equals("server")));
      SpanData handleSpan =
          spanExporter.getFinishedSpanItems().stream()
              .filter(s -> s.getName().equals("handle"))
              .findFirst()
              .orElseThrow();
      SpanData channelSpan =
          spanExporter.getFinishedSpanItems().stream()
              .filter(s -> s.getName().equals("server"))
              .findFirst()
              .orElseThrow();
      assertThat(handleSpan.getLinks()).hasSize(1);
      assertThat(handleSpan.getLinks().getFirst().getSpanContext().getSpanId())
          .isEqualTo(channelSpan.getSpanContext().getSpanId());
    }

    @Test
    void handle_span_records_error_when_handler_fails() throws Exception {
      var ch =
          createTracedChannel(
              (req, future) -> future.completeExceptionally(new RuntimeException("handler error")));
      channel = ch;
      TestFixture.writeFramed("ping", serverOut);
      await()
          .atMost(Duration.ofSeconds(2))
          .until(
              () ->
                  spanExporter.getFinishedSpanItems().stream()
                      .anyMatch(s -> s.getName().equals("handle")));
      SpanData span =
          spanExporter.getFinishedSpanItems().stream()
              .filter(s -> s.getName().equals("handle"))
              .findFirst()
              .orElseThrow();
      assertThat(span).hasStatus(StatusData.error());
    }
  }
}
