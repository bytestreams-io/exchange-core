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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
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
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PipelinedChannelTest extends AbstractChannelTestBase {

  @Override
  PipelinedChannel<String, String> createChannel() {
    return createChannel(Integer.MAX_VALUE);
  }

  @Override
  PipelinedChannel<String, String> createUninitializedChannel() {
    return createUninitializedChannel(Integer.MAX_VALUE);
  }

  private PipelinedChannel<String, String> createChannel(int maxConcurrency) {
    var ch = createUninitializedChannel(maxConcurrency);
    ch.start();
    return ch;
  }

  private PipelinedChannel<String, String> createUninitializedChannel(int maxConcurrency) {
    return new PipelinedChannel<>(
        transport,
        Integer.MAX_VALUE,
        TestFixture.FRAMED_WRITER,
        TestFixture.FRAMED_READER,
        maxConcurrency,
        errorHandler,
        Duration.ofSeconds(30),
        AbstractChannel.DEFAULT_ERROR_BACKOFF_NANOS,
        OTel.meter(),
        OTel.tracer());
  }

  // -- Builder --

  @Nested
  class BuilderTests {
    @Test
    void builder_missing_transport_throws() {
      var builder =
          PipelinedChannel.<String, String>builder()
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("transport");
    }

    @Test
    void builder_missing_requestWriter_throws() {
      var builder =
          PipelinedChannel.<String, String>builder()
              .transport(transport)
              .responseReader(TestFixture.FRAMED_READER);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("requestWriter");
    }

    @Test
    void builder_missing_responseReader_throws() {
      var builder =
          PipelinedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("responseReader");
    }

    @Test
    void builder_zero_writeBufferSize_throws() {
      var builder =
          PipelinedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER)
              .writeBufferSize(0);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("writeBufferSize");
    }

    @Test
    void builder_zero_maxConcurrency_throws() {
      var builder =
          PipelinedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER)
              .maxConcurrency(0);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("maxConcurrency");
    }

    @Test
    void builder_null_defaultTimeout_throws() {
      var builder =
          PipelinedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER)
              .defaultTimeout(null);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("defaultTimeout");
    }

    @Test
    void builder_zero_defaultTimeout_throws() {
      var builder =
          PipelinedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER)
              .defaultTimeout(Duration.ZERO);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("defaultTimeout");
    }

    @Test
    void builder_null_meter_throws() {
      var builder =
          PipelinedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER);
      assertThatThrownBy(() -> builder.meter(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("meter");
    }

    @Test
    void builder_null_tracer_throws() {
      var builder =
          PipelinedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER);
      assertThatThrownBy(() -> builder.tracer(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("tracer");
    }

    @Test
    void builder_creates_channel_with_all_options() {
      channel =
          PipelinedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER)
              .maxConcurrency(10)
              .writeBufferSize(4096)
              .errorHandler(new ErrorHandler<>() {})
              .defaultTimeout(Duration.ofSeconds(5))
              .errorBackoff(Duration.ofMillis(50))
              .meter(OTel.meter())
              .tracer(OTel.tracer())
              .build();
      assertThat(channel.status()).isEqualTo(ChannelStatus.INIT);
    }
  }

  // -- Request --

  @Nested
  class Request {
    @Test
    void request_null_timeout_throws() {
      var ch = createChannel();
      channel = ch;
      assertThatThrownBy(() -> ch.request("hello", null))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("timeout must be positive");
    }

    @Test
    void request_zero_timeout_throws() {
      var ch = createChannel();
      channel = ch;
      assertThatThrownBy(() -> ch.request("hello", Duration.ZERO))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("timeout must be positive");
    }

    @Test
    void request_before_start_throws() {
      var ch = createUninitializedChannel(Integer.MAX_VALUE);
      channel = ch;
      assertThatThrownBy(() -> ch.request("hello"))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("not started");
    }

    @Test
    void request_returns_pending_future() {
      var ch = createChannel();
      channel = ch;
      assertThat(ch.request("msg")).isNotDone();
    }

    @Test
    void request_after_close_returns_cancellation() throws Exception {
      var ch = createChannel();
      channel = ch;
      ch.close().get(2, TimeUnit.SECONDS);
      CompletableFuture<String> future = ch.request("msg");
      assertThat(future)
          .isCompletedExceptionally()
          .failsWithin(Duration.ZERO)
          .withThrowableThat()
          .isInstanceOf(CancellationException.class);
    }

    @Test
    void request_and_correlate_completes_future() throws Exception {
      var ch = createChannel();
      channel = ch;
      CompletableFuture<String> future = ch.request("hello");
      TestFixture.writeFramed("hello", serverOut);
      assertThat(future).succeedsWithin(Duration.ofMillis(500)).isEqualTo("hello");
    }

    @Test
    void request_handles_interrupt() throws Exception {
      OutputStream mockOut = mock(OutputStream.class);
      CountDownLatch writeLatch = new CountDownLatch(1);
      lenient()
          .doAnswer(
              inv -> {
                writeLatch.await();
                return null;
              })
          .when(mockOut)
          .write(any(byte[].class));
      transport = spy(transport);
      lenient().when(transport.outputStream()).thenReturn(mockOut);
      var ch =
          new PipelinedChannel<>(
              transport,
              1,
              TestFixture.FRAMED_WRITER,
              TestFixture.FRAMED_READER,
              Integer.MAX_VALUE,
              errorHandler,
              Duration.ofSeconds(30),
              AbstractChannel.DEFAULT_ERROR_BACKOFF_NANOS,
              OTel.meter(),
              OTel.tracer());
      ch.start();
      channel = ch;
      ch.request("first");
      ch.request("second");
      CompletableFuture<CompletableFuture<String>> third = new CompletableFuture<>();
      Thread thread = VT.of(() -> third.complete(ch.request("third")), "3rd");
      thread.start();
      await().during(Duration.ofMillis(200)).until(() -> !third.isDone());
      thread.interrupt();
      await().atMost(Duration.ofMillis(200)).until(third::isDone);
      assertThat(third.join())
          .isCompletedExceptionally()
          .failsWithin(Duration.ZERO)
          .withThrowableThat()
          .isInstanceOf(ExecutionException.class)
          .withCauseInstanceOf(InterruptedException.class);
      writeLatch.countDown();
    }

    @Test
    void multiple_pipelined_requests_complete_in_order() throws Exception {
      var ch = createChannel();
      channel = ch;
      CompletableFuture<String> futureA = ch.request("A");
      CompletableFuture<String> futureB = ch.request("B");
      TestFixture.writeFramed("responseA", serverOut);
      assertThat(futureA).succeedsWithin(Duration.ofMillis(500)).isEqualTo("responseA");
      TestFixture.writeFramed("responseB", serverOut);
      assertThat(futureB).succeedsWithin(Duration.ofMillis(500)).isEqualTo("responseB");
    }
  }

  // -- Lockstep --

  @Nested
  class Lockstep {
    @Test
    void second_request_blocks_until_first_completes() throws Exception {
      var ch = createChannel(1);
      channel = ch;
      CompletableFuture<String> first = ch.request("A");
      CompletableFuture<CompletableFuture<String>> secondHolder = new CompletableFuture<>();
      VT.start(() -> secondHolder.complete(ch.request("B")), "lockstep-second");
      await().during(Duration.ofMillis(100)).until(() -> !secondHolder.isDone());
      TestFixture.writeFramed("responseA", serverOut);
      assertThat(first).succeedsWithin(Duration.ofMillis(500)).isEqualTo("responseA");
      await().atMost(Duration.ofMillis(500)).until(secondHolder::isDone);
      TestFixture.writeFramed("responseB", serverOut);
      assertThat(secondHolder.join()).succeedsWithin(Duration.ofMillis(500)).isEqualTo("responseB");
    }
  }

  // -- Timeout cascade --

  @Nested
  class TimeoutCascade {
    @Test
    void timeout_on_first_cascades_to_second() {
      var ch = createChannel();
      channel = ch;
      CompletableFuture<String> futureA = ch.request("A", Duration.ofMillis(50));
      CompletableFuture<String> futureB = ch.request("B", Duration.ofSeconds(30));
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(futureA).isCompletedExceptionally());
      assertThat(futureA)
          .failsWithin(Duration.ZERO)
          .withThrowableThat()
          .isInstanceOf(ExecutionException.class)
          .withCauseInstanceOf(TimeoutException.class);
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(futureB).isCompletedExceptionally());
    }
  }

  // -- WriteLoop --

  @Nested
  class WriteLoop {
    @Test
    void write_loop_io_exception_calls_error_handler() throws Exception {
      transport = spy(transport);
      IOException error = new IOException("write failed");
      when(transport.outputStream()).thenThrow(error);
      var ch = createChannel();
      channel = ch;
      ch.request("msg");
      verify(errorHandler, timeout(1000)).stopOnError(eq(channel), any());
    }

    @Test
    void write_loop_io_exception_closes_transport_when_handler_returns_true() throws Exception {
      transport = spy(transport);
      IOException error = new IOException("write failed");
      when(transport.outputStream()).thenThrow(error);
      when(errorHandler.stopOnError(any(), any())).thenReturn(true);
      var ch = createChannel();
      channel = ch;
      ch.request("msg");
      verify(transport, timeout(1000).atLeastOnce()).close();
    }

    @Test
    void write_loop_continues_when_handler_returns_false() throws Exception {
      transport = spy(transport);
      IOException error = new IOException("write failed");
      when(transport.outputStream()).thenThrow(error);
      var ch = createChannel();
      channel = ch;
      ch.request("msg");
      verify(errorHandler, timeout(1000).atLeastOnce()).stopOnError(eq(channel), any());
    }

    @Test
    void write_loop_interrupted_exception_calls_error_handler() throws Exception {
      transport = spy(transport);
      // Set the write thread's interrupt flag after the first write completes.
      // On the next poll(), the interrupt flag causes InterruptedException.
      lenient()
          .when(transport.outputStream())
          .thenAnswer(
              inv -> {
                Thread.currentThread().interrupt();
                return clientRawSocket.getOutputStream();
              });
      when(errorHandler.stopOnError(any(), any())).thenReturn(true);
      var ch = createChannel();
      channel = ch;
      ch.request("msg");
      verify(errorHandler, timeout(2000).atLeastOnce()).stopOnError(eq(channel), any());
    }

    @Test
    void close_waits_for_pending_write_queue_to_drain() throws Exception {
      CountDownLatch blockLatch = new CountDownLatch(1);
      transport = spy(transport);
      when(transport.outputStream())
          .thenAnswer(
              inv -> {
                blockLatch.await();
                return clientRawSocket.getOutputStream();
              })
          .thenCallRealMethod();
      var ch = createChannel();
      channel = ch;
      CompletableFuture<String> msg1 = ch.request("msg1");
      CompletableFuture<String> msg2 = ch.request("msg2");
      CompletableFuture<Void> close = ch.close();
      await().during(Duration.ofMillis(100)).until(() -> !close.isDone());
      blockLatch.countDown();
      String first = TestFixture.readFramed(serverIn);
      String second = TestFixture.readFramed(serverIn);
      assertThat(first).isEqualTo("msg1");
      assertThat(second).isEqualTo("msg2");
      TestFixture.writeFramed("resp1", serverOut);
      TestFixture.writeFramed("resp2", serverOut);
      assertThat(msg1).succeedsWithin(Duration.ofMillis(500)).isEqualTo("resp1");
      assertThat(msg2).succeedsWithin(Duration.ofMillis(500)).isEqualTo("resp2");
      assertThat(close).succeedsWithin(Duration.ofMillis(500));
    }
  }

  // -- Close lifecycle --

  @Nested
  class CloseLifecycle {
    @Test
    void close_sets_closing_then_closed_status() {
      channel = createChannel();
      CompletableFuture<Void> closeFuture = channel.close();
      assertThat(closeFuture).succeedsWithin(Duration.ofMillis(500));
      assertThat(channel.status()).isEqualTo(ChannelStatus.CLOSED);
    }

    @Test
    void io_exception_during_shutdown_with_pending_calls_error_handler() throws Exception {
      var ch = createChannel();
      channel = ch;
      ch.request("msg");
      // Close server socket to cause IOException (EOF) on the read loop
      serverRawSocket.close();
      verify(errorHandler, timeout(1000).atLeastOnce()).stopOnError(eq(channel), any());
    }

    @Test
    void close_does_not_set_closing_when_already_closed() {
      channel = createChannel();
      CompletableFuture<Void> future = channel.close();
      assertThat(future).succeedsWithin(Duration.ofMillis(500));
      assertThat(channel.status()).isEqualTo(ChannelStatus.CLOSED);
      channel.close();
      await()
          .during(Duration.ofMillis(500))
          .untilAsserted(() -> assertThat(channel.status()).isEqualTo(ChannelStatus.CLOSED));
      assertThat(channel.status()).isEqualTo(ChannelStatus.CLOSED);
    }
  }

  // -- Read loop safety net with pending requests --

  @Nested
  class ReadLoopOuterCatch {
    @Test
    void outer_catch_throwable_with_pending_closes_transport() throws Exception {
      CountDownLatch stateReady = new CountDownLatch(1);
      CountDownLatch errorThrown = new CountDownLatch(1);
      CountDownLatch blockForever = new CountDownLatch(1);
      transport = spy(transport);
      // Block the write loop so it can't drain writeQueue
      lenient()
          .when(transport.outputStream())
          .thenAnswer(
              inv -> {
                blockForever.await();
                return null;
              });
      lenient()
          .when(transport.inputStream())
          .thenAnswer(
              inv -> {
                stateReady.await();
                errorThrown.countDown();
                throw new Error("catastrophic");
              });
      var ch =
          new PipelinedChannel<>(
              transport,
              Integer.MAX_VALUE,
              TestFixture.FRAMED_WRITER,
              TestFixture.FRAMED_READER,
              Integer.MAX_VALUE,
              errorHandler,
              Duration.ofSeconds(30),
              AbstractChannel.DEFAULT_ERROR_BACKOFF_NANOS,
              OTel.meter(),
              OTel.tracer());
      ch.start();
      channel = ch;
      // Set up state: CLOSING with pending work in correlator
      ch.request("pending"); // registers in correlator -> hasPending()=true
      ch.status.set(ChannelStatus.CLOSING);
      stateReady.countDown();
      assertThat(errorThrown.await(2, TimeUnit.SECONDS)).isTrue();
      await()
          .atMost(Duration.ofMillis(500))
          .untilAsserted(() -> assertThat(ch.closeFuture()).failsWithin(Duration.ofMillis(100)));
    }
  }

  // -- Uncorrelated response --

  @Nested
  class UncorrelatedResponse {
    @Test
    void uncorrelated_response_continues_when_handler_returns_false() throws Exception {
      channel = createChannel();
      TestFixture.writeFramed("stale", serverOut);
      await().during(Duration.ofMillis(200)).until(() -> channel.status() != ChannelStatus.CLOSED);
    }

    @Test
    void uncorrelated_response_closes_when_handler_returns_true() throws Exception {
      transport = spy(transport);
      when(errorHandler.stopOnError(any(), any())).thenReturn(true);
      channel = createChannel();
      TestFixture.writeFramed("stale", serverOut);
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

    private PipelinedChannel<String, String> createMetricChannel() {
      var ch =
          new PipelinedChannel<>(
              transport,
              Integer.MAX_VALUE,
              TestFixture.FRAMED_WRITER,
              TestFixture.FRAMED_READER,
              Integer.MAX_VALUE,
              errorHandler,
              Duration.ofSeconds(30),
              AbstractChannel.DEFAULT_ERROR_BACKOFF_NANOS,
              meterProvider.get(OTel.NAMESPACE),
              OTel.tracer());
      ch.start();
      return ch;
    }

    @Test
    void request_active_incremented_on_send() {
      var ch = createMetricChannel();
      channel = ch;
      ch.request("hello");
      TestFixture.assertLongSum(
          metricReader,
          "request.active",
          1,
          equalTo(CHANNEL_TYPE, "pipelined"),
          equalTo(MESSAGE_TYPE, "String"));
    }

    @Test
    void request_active_decremented_on_completion() throws Exception {
      var ch = createMetricChannel();
      channel = ch;
      CompletableFuture<String> future = ch.request("hello");
      TestFixture.writeFramed("hello", serverOut);
      assertThat(future).succeedsWithin(Duration.ofMillis(500));
      await()
          .atMost(Duration.ofMillis(500))
          .untilAsserted(
              () ->
                  TestFixture.assertLongSum(
                      metricReader,
                      "request.active",
                      0,
                      equalTo(CHANNEL_TYPE, "pipelined"),
                      equalTo(MESSAGE_TYPE, "String")));
    }

    @Test
    void request_duration_recorded_on_completion() throws Exception {
      var ch = createMetricChannel();
      channel = ch;
      CompletableFuture<String> future = ch.request("hello");
      TestFixture.writeFramed("hello", serverOut);
      assertThat(future).succeedsWithin(Duration.ofMillis(500));
      await()
          .atMost(Duration.ofMillis(500))
          .untilAsserted(
              () ->
                  TestFixture.assertHistogram(
                      metricReader,
                      "request.duration",
                      1,
                      equalTo(CHANNEL_TYPE, "pipelined"),
                      equalTo(MESSAGE_TYPE, "String")));
    }

    @Test
    void request_total_incremented_on_completion() throws Exception {
      var ch = createMetricChannel();
      channel = ch;
      CompletableFuture<String> future = ch.request("hello");
      TestFixture.writeFramed("hello", serverOut);
      assertThat(future).succeedsWithin(Duration.ofMillis(500));
      await()
          .atMost(Duration.ofMillis(500))
          .untilAsserted(
              () ->
                  TestFixture.assertLongSum(
                      metricReader,
                      "request.total",
                      1,
                      equalTo(CHANNEL_TYPE, "pipelined"),
                      equalTo(MESSAGE_TYPE, "String")));
    }

    @Test
    void request_errors_incremented_on_failure() {
      var ch = createMetricChannel();
      channel = ch;
      CompletableFuture<String> future = ch.request("hello", Duration.ofMillis(50));
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(future).isCompletedExceptionally());
      await()
          .atMost(Duration.ofMillis(500))
          .untilAsserted(
              () ->
                  TestFixture.assertLongSum(
                      metricReader,
                      "request.errors",
                      1,
                      equalTo(CHANNEL_TYPE, "pipelined"),
                      equalTo(MESSAGE_TYPE, "String"),
                      equalTo(ERROR_TYPE, "TimeoutException")));
    }
  }

  // -- Tracing --

  @Nested
  class Tracing {
    private InMemorySpanExporter spanExporter;
    private LatchSpanProcessor latchProcessor;
    private SdkTracerProvider tracerProvider;

    @BeforeEach
    void setUp() {
      spanExporter = InMemorySpanExporter.create();
      latchProcessor = LatchSpanProcessor.create("pipelined", "request");
      tracerProvider =
          SdkTracerProvider.builder()
              .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
              .addSpanProcessor(latchProcessor)
              .build();
    }

    @AfterEach
    void tearDown() {
      tracerProvider.close();
    }

    private PipelinedChannel<String, String> createTracedChannel() {
      var ch =
          new PipelinedChannel<>(
              transport,
              Integer.MAX_VALUE,
              TestFixture.FRAMED_WRITER,
              TestFixture.FRAMED_READER,
              Integer.MAX_VALUE,
              errorHandler,
              Duration.ofSeconds(30),
              AbstractChannel.DEFAULT_ERROR_BACKOFF_NANOS,
              OTel.meter(),
              tracerProvider.get("test"));
      ch.start();
      return ch;
    }

    @Test
    void channel_span_created_with_attributes() throws Exception {
      var ch = createTracedChannel();
      channel = ch;
      ch.close();
      latchProcessor.await("pipelined", 5, TimeUnit.SECONDS);
      SpanData span =
          spanExporter.getFinishedSpanItems().stream()
              .filter(s -> s.getName().equals("pipelined"))
              .findFirst()
              .orElseThrow();
      assertThat(span).hasName("pipelined").hasKind(SpanKind.CLIENT);
      assertThat(span.getAttributes().get(OTel.CHANNEL_TYPE)).isEqualTo("pipelined");
    }

    @Test
    void channel_span_ends_ok_on_clean_close() throws Exception {
      var ch = createTracedChannel();
      channel = ch;
      ch.close();
      latchProcessor.await("pipelined", 5, TimeUnit.SECONDS);
      SpanData span =
          spanExporter.getFinishedSpanItems().stream()
              .filter(s -> s.getName().equals("pipelined"))
              .findFirst()
              .orElseThrow();
      assertThat(span).hasStatus(StatusData.unset());
    }

    @Test
    void request_span_linked_to_channel_span() throws Exception {
      var ch = createTracedChannel();
      channel = ch;
      CompletableFuture<String> future = ch.request("hello");
      TestFixture.writeFramed("world", serverOut);
      assertThat(future).succeedsWithin(Duration.ofMillis(500));
      ch.close();
      latchProcessor.awaitAll(5, TimeUnit.SECONDS);
      SpanData requestSpan =
          spanExporter.getFinishedSpanItems().stream()
              .filter(s -> s.getName().equals("request"))
              .findFirst()
              .orElseThrow();
      SpanData channelSpan =
          spanExporter.getFinishedSpanItems().stream()
              .filter(s -> s.getName().equals("pipelined"))
              .findFirst()
              .orElseThrow();
      assertThat(requestSpan.getLinks()).hasSize(1);
      assertThat(requestSpan.getLinks().getFirst().getSpanContext().getSpanId())
          .isEqualTo(channelSpan.getSpanContext().getSpanId());
    }
  }
}
