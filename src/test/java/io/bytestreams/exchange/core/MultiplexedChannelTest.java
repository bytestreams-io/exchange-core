package io.bytestreams.exchange.core;

import static io.bytestreams.exchange.core.TestFixture.CHANNEL_TYPE;
import static io.bytestreams.exchange.core.TestFixture.ERROR_TYPE;
import static io.bytestreams.exchange.core.TestFixture.MESSAGE_TYPE;
import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.equalTo;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class MultiplexedChannelTest extends AbstractChannelTestBase {

  // ID extractor: use the string itself as the ID
  private static final Function<String, String> ID_EXTRACTOR = s -> s;

  @Override
  MultiplexedChannel<String, String> createChannel() {
    return createChannel(Integer.MAX_VALUE);
  }

  @Override
  MultiplexedChannel<String, String> createUninitializedChannel() {
    return createUninitializedChannel(Integer.MAX_VALUE);
  }

  private MultiplexedChannel<String, String> createChannel(int maxConcurrency) {
    var ch = createUninitializedChannel(maxConcurrency);
    ch.start();
    return ch;
  }

  private MultiplexedChannel<String, String> createUninitializedChannel(int maxConcurrency) {
    return new MultiplexedChannel<>(
        transport,
        Integer.MAX_VALUE,
        TestFixture.FRAMED_WRITER,
        TestFixture.FRAMED_READER,
        ID_EXTRACTOR,
        ID_EXTRACTOR,
        maxConcurrency,
        msg -> {},
        errorHandler,
        Duration.ofSeconds(30),
        AbstractChannel.DEFAULT_ERROR_BACKOFF_NANOS,
        "multiplexed",
        SpanKind.CLIENT,
        OTel.meter(),
        OTel.tracer());
  }

  // -- Builder --

  @Nested
  class BuilderTests {
    @Test
    void builder_missing_transport_throws() {
      var builder =
          MultiplexedChannel.<String, String>builder()
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER)
              .requestIdExtractor(ID_EXTRACTOR)
              .responseIdExtractor(ID_EXTRACTOR);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("transport");
    }

    @Test
    void builder_missing_requestWriter_throws() {
      var builder =
          MultiplexedChannel.<String, String>builder()
              .transport(transport)
              .responseReader(TestFixture.FRAMED_READER)
              .requestIdExtractor(ID_EXTRACTOR)
              .responseIdExtractor(ID_EXTRACTOR);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("requestWriter");
    }

    @Test
    void builder_missing_responseReader_throws() {
      var builder =
          MultiplexedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .requestIdExtractor(ID_EXTRACTOR)
              .responseIdExtractor(ID_EXTRACTOR);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("responseReader");
    }

    @Test
    void builder_missing_requestIdExtractor_throws() {
      var builder =
          MultiplexedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER)
              .responseIdExtractor(ID_EXTRACTOR);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("requestIdExtractor");
    }

    @Test
    void builder_missing_responseIdExtractor_throws() {
      var builder =
          MultiplexedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER)
              .requestIdExtractor(ID_EXTRACTOR);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("responseIdExtractor");
    }

    @Test
    void builder_zero_writeBufferSize_throws() {
      var builder =
          MultiplexedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER)
              .requestIdExtractor(ID_EXTRACTOR)
              .responseIdExtractor(ID_EXTRACTOR)
              .writeBufferSize(0);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("writeBufferSize");
    }

    @Test
    void builder_zero_maxConcurrency_throws() {
      var builder =
          MultiplexedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER)
              .requestIdExtractor(ID_EXTRACTOR)
              .responseIdExtractor(ID_EXTRACTOR)
              .maxConcurrency(0);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("maxConcurrency");
    }

    @Test
    void builder_null_defaultTimeout_throws() {
      var builder =
          MultiplexedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER)
              .requestIdExtractor(ID_EXTRACTOR)
              .responseIdExtractor(ID_EXTRACTOR)
              .defaultTimeout(null);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("defaultTimeout");
    }

    @Test
    void builder_zero_defaultTimeout_throws() {
      var builder =
          MultiplexedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER)
              .requestIdExtractor(ID_EXTRACTOR)
              .responseIdExtractor(ID_EXTRACTOR)
              .defaultTimeout(Duration.ZERO);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("defaultTimeout");
    }

    @Test
    void builder_null_meter_throws() {
      var builder =
          MultiplexedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER)
              .requestIdExtractor(ID_EXTRACTOR)
              .responseIdExtractor(ID_EXTRACTOR);
      assertThatThrownBy(() -> builder.meter(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("meter");
    }

    @Test
    void builder_null_tracer_throws() {
      var builder =
          MultiplexedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER)
              .requestIdExtractor(ID_EXTRACTOR)
              .responseIdExtractor(ID_EXTRACTOR);
      assertThatThrownBy(() -> builder.tracer(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("tracer");
    }

    @Test
    void builder_creates_channel_with_all_options() {
      channel =
          MultiplexedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER)
              .requestIdExtractor(ID_EXTRACTOR)
              .responseIdExtractor(ID_EXTRACTOR)
              .maxConcurrency(10)
              .writeBufferSize(4096)
              .errorHandler(new ErrorHandler<>() {})
              .defaultTimeout(Duration.ofSeconds(5))
              .uncorrelatedHandler(msg -> {})
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
      assertThat(ch.request("reqA")).isNotDone();
    }

    @Test
    void request_after_close_returns_cancellation() throws Exception {
      var ch = createChannel();
      channel = ch;
      ch.close().get(2, TimeUnit.SECONDS);
      CompletableFuture<String> future = ch.request("reqA");
      assertThat(future)
          .isCompletedExceptionally()
          .failsWithin(Duration.ZERO)
          .withThrowableThat()
          .isInstanceOf(CancellationException.class);
    }

    @Test
    void response_matched_by_id_completes_correct_future() throws Exception {
      var ch = createChannel();
      channel = ch;
      CompletableFuture<String> future = ch.request("reqA");
      TestFixture.writeFramed("reqA", serverOut);
      assertThat(future).succeedsWithin(Duration.ofMillis(500)).isEqualTo("reqA");
    }

    @Test
    void out_of_order_responses_complete_correct_futures() throws Exception {
      var ch = createChannel();
      channel = ch;
      CompletableFuture<String> futureA = ch.request("reqA");
      CompletableFuture<String> futureB = ch.request("reqB");
      // Respond to B first
      TestFixture.writeFramed("reqB", serverOut);
      assertThat(futureB).succeedsWithin(Duration.ofMillis(500)).isEqualTo("reqB");
      assertThat(futureA).isNotDone();
      // Then respond to A
      TestFixture.writeFramed("reqA", serverOut);
      assertThat(futureA).succeedsWithin(Duration.ofMillis(500)).isEqualTo("reqA");
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
      // Build a channel with write buffer size of 1 so a third request blocks
      var ch =
          new MultiplexedChannel<>(
              transport,
              1,
              TestFixture.FRAMED_WRITER,
              TestFixture.FRAMED_READER,
              ID_EXTRACTOR,
              ID_EXTRACTOR,
              Integer.MAX_VALUE,
              msg -> {},
              errorHandler,
              Duration.ofSeconds(30),
              AbstractChannel.DEFAULT_ERROR_BACKOFF_NANOS,
              "multiplexed",
              SpanKind.CLIENT,
              OTel.meter(),
              OTel.tracer());
      ch.start();
      channel = ch;
      ch.request("id1");
      // "id2" fills the write queue (size=1, write loop is blocked writing "id1")
      ch.request("id2");
      // Third request will block trying to put into full queue
      CompletableFuture<CompletableFuture<String>> third = new CompletableFuture<>();
      Thread thread = VT.of(() -> third.complete(ch.request("id3")), "3rd");
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
    void duplicate_id_throws_duplicate_correlation_id_exception() {
      var ch = createChannel();
      channel = ch;
      ch.request("sameId");
      assertThatThrownBy(() -> ch.request("sameId"))
          .isInstanceOf(DuplicateCorrelationIdException.class);
    }
  }

  // -- Uncorrelated --

  @Nested
  class UncorrelatedMessages {
    @Test
    void uncorrelated_response_calls_handler() throws Exception {
      @SuppressWarnings("unchecked")
      UnhandledMessageHandler<String> handler = mock(UnhandledMessageHandler.class);
      var ch =
          new MultiplexedChannel<>(
              transport,
              Integer.MAX_VALUE,
              TestFixture.FRAMED_WRITER,
              TestFixture.FRAMED_READER,
              ID_EXTRACTOR,
              ID_EXTRACTOR,
              Integer.MAX_VALUE,
              handler,
              errorHandler,
              Duration.ofSeconds(30),
              AbstractChannel.DEFAULT_ERROR_BACKOFF_NANOS,
              "multiplexed",
              SpanKind.CLIENT,
              OTel.meter(),
              OTel.tracer());
      ch.start();
      channel = ch;
      TestFixture.writeFramed("unknownId", serverOut);
      verify(handler, timeout(1000)).onMessage("unknownId");
    }

    @Test
    void default_uncorrelated_handler_noop_no_crash() throws Exception {
      // Build with default no-op uncorrelated handler via builder
      channel =
          MultiplexedChannel.<String, String>builder()
              .transport(transport)
              .requestWriter(TestFixture.FRAMED_WRITER)
              .responseReader(TestFixture.FRAMED_READER)
              .requestIdExtractor(ID_EXTRACTOR)
              .responseIdExtractor(ID_EXTRACTOR)
              .build();
      channel.start();
      TestFixture.writeFramed("unknownId", serverOut);
      // Channel should still be running after uncorrelated message
      await().atMost(Duration.ofMillis(500)).until(() -> channel.status() == ChannelStatus.READY);
    }
  }

  // -- Timeout --

  @Nested
  class TimeoutTests {
    @Test
    void timeout_affects_only_timed_out_request() {
      var ch = createChannel();
      channel = ch;
      CompletableFuture<String> futureA = ch.request("reqA", Duration.ofMillis(50));
      CompletableFuture<String> futureB = ch.request("reqB", Duration.ofSeconds(30));
      await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(futureA).isCompletedExceptionally());
      assertThat(futureA)
          .failsWithin(Duration.ZERO)
          .withThrowableThat()
          .isInstanceOf(ExecutionException.class)
          .withCauseInstanceOf(TimeoutException.class);
      assertThat(futureB).isNotDone();
    }
  }

  // -- Backpressure --

  @Nested
  class BackpressureTests {
    @Test
    void backpressure_at_max_concurrency() {
      var ch = createChannel(1);
      channel = ch;
      // First request fills the semaphore
      ch.request("reqA");
      // Second request should block because maxConcurrency=1
      CompletableFuture<CompletableFuture<String>> secondHolder = new CompletableFuture<>();
      VT.start(() -> secondHolder.complete(ch.request("reqB")), "backpressure-second");
      await().during(Duration.ofMillis(100)).until(() -> !secondHolder.isDone());
      assertThat(secondHolder).isNotDone();
    }
  }

  // -- Close lifecycle --

  @Nested
  class CloseLifecycle {
    @Test
    void close_transitions_to_closed_status() {
      channel = createChannel();
      CompletableFuture<Void> closeFuture = channel.close();
      assertThat(closeFuture).succeedsWithin(Duration.ofMillis(500));
      assertThat(channel.status()).isEqualTo(ChannelStatus.CLOSED);
    }

    @Test
    void close_waits_for_pending_requests_to_drain() throws Exception {
      var ch = createChannel();
      channel = ch;
      CompletableFuture<String> futureA = ch.request("reqA");
      CompletableFuture<String> futureB = ch.request("reqB");
      // Close channel while both requests are pending — read loop should keep going
      ch.close();
      assertThat(ch.status()).isEqualTo(ChannelStatus.CLOSING);
      // Write responses from serverOut to complete pending requests
      TestFixture.writeFramed("reqA", serverOut);
      TestFixture.writeFramed("reqB", serverOut);
      assertThat(futureA).succeedsWithin(Duration.ofMillis(500)).isEqualTo("reqA");
      assertThat(futureB).succeedsWithin(Duration.ofMillis(500)).isEqualTo("reqB");
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

    private MultiplexedChannel<String, String> createMetricChannel() {
      var ch =
          new MultiplexedChannel<>(
              transport,
              Integer.MAX_VALUE,
              TestFixture.FRAMED_WRITER,
              TestFixture.FRAMED_READER,
              ID_EXTRACTOR,
              ID_EXTRACTOR,
              Integer.MAX_VALUE,
              msg -> {},
              errorHandler,
              Duration.ofSeconds(30),
              AbstractChannel.DEFAULT_ERROR_BACKOFF_NANOS,
              "multiplexed",
              SpanKind.CLIENT,
              meterProvider.get(OTel.NAMESPACE),
              OTel.tracer());
      ch.start();
      return ch;
    }

    @Test
    void request_active_incremented_on_send() {
      var ch = createMetricChannel();
      channel = ch;
      ch.request("reqA");
      TestFixture.assertLongSum(
          metricReader,
          "request.active",
          1,
          equalTo(CHANNEL_TYPE, "multiplexed"),
          equalTo(MESSAGE_TYPE, "String"));
    }

    @Test
    void request_total_incremented_on_completion() throws Exception {
      var ch = createMetricChannel();
      channel = ch;
      CompletableFuture<String> future = ch.request("reqA");
      TestFixture.writeFramed("reqA", serverOut);
      assertThat(future).succeedsWithin(Duration.ofMillis(500));
      await()
          .atMost(Duration.ofMillis(500))
          .untilAsserted(
              () ->
                  TestFixture.assertLongSum(
                      metricReader,
                      "request.total",
                      1,
                      equalTo(CHANNEL_TYPE, "multiplexed"),
                      equalTo(MESSAGE_TYPE, "String")));
    }

    @Test
    void request_duration_recorded_on_completion() throws Exception {
      var ch = createMetricChannel();
      channel = ch;
      CompletableFuture<String> future = ch.request("reqA");
      TestFixture.writeFramed("reqA", serverOut);
      assertThat(future).succeedsWithin(Duration.ofMillis(500));
      await()
          .atMost(Duration.ofMillis(500))
          .untilAsserted(
              () ->
                  TestFixture.assertHistogram(
                      metricReader,
                      "request.duration",
                      1,
                      equalTo(CHANNEL_TYPE, "multiplexed"),
                      equalTo(MESSAGE_TYPE, "String")));
    }

    @Test
    void request_errors_incremented_on_failure() {
      var ch = createMetricChannel();
      channel = ch;
      CompletableFuture<String> future = ch.request("reqA", Duration.ofMillis(50));
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
                      equalTo(CHANNEL_TYPE, "multiplexed"),
                      equalTo(MESSAGE_TYPE, "String"),
                      equalTo(ERROR_TYPE, "TimeoutException")));
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

    private MultiplexedChannel<String, String> createTracedChannel() {
      var ch =
          new MultiplexedChannel<>(
              transport,
              Integer.MAX_VALUE,
              TestFixture.FRAMED_WRITER,
              TestFixture.FRAMED_READER,
              ID_EXTRACTOR,
              ID_EXTRACTOR,
              Integer.MAX_VALUE,
              msg -> {},
              errorHandler,
              Duration.ofSeconds(30),
              AbstractChannel.DEFAULT_ERROR_BACKOFF_NANOS,
              "multiplexed",
              SpanKind.CLIENT,
              OTel.meter(),
              tracerProvider.get("test"));
      ch.start();
      return ch;
    }

    @Test
    void request_span_includes_message_id() throws Exception {
      var ch = createTracedChannel();
      channel = ch;
      CompletableFuture<String> future = ch.request("myReqId");
      TestFixture.writeFramed("myReqId", serverOut);
      assertThat(future).succeedsWithin(Duration.ofMillis(500));
      ch.close();
      await()
          .atMost(Duration.ofSeconds(2))
          .until(
              () ->
                  spanExporter.getFinishedSpanItems().stream()
                      .anyMatch(s -> s.getName().equals("request")));
      SpanData requestSpan =
          spanExporter.getFinishedSpanItems().stream()
              .filter(s -> s.getName().equals("request"))
              .findFirst()
              .orElseThrow();
      assertThat(requestSpan.getAttributes().get(OTel.MESSAGE_ID)).isEqualTo("myReqId");
    }

    @Test
    void request_span_linked_to_channel_span() throws Exception {
      var ch = createTracedChannel();
      channel = ch;
      CompletableFuture<String> future = ch.request("myReqId");
      TestFixture.writeFramed("myReqId", serverOut);
      assertThat(future).succeedsWithin(Duration.ofMillis(500));
      ch.close();
      await()
          .atMost(Duration.ofSeconds(2))
          .until(
              () ->
                  spanExporter.getFinishedSpanItems().stream()
                          .anyMatch(s -> s.getName().equals("request"))
                      && spanExporter.getFinishedSpanItems().stream()
                          .anyMatch(s -> s.getName().equals("multiplexed")));
      SpanData requestSpan =
          spanExporter.getFinishedSpanItems().stream()
              .filter(s -> s.getName().equals("request"))
              .findFirst()
              .orElseThrow();
      SpanData channelSpan =
          spanExporter.getFinishedSpanItems().stream()
              .filter(s -> s.getName().equals("multiplexed"))
              .findFirst()
              .orElseThrow();
      assertThat(requestSpan.getLinks()).hasSize(1);
      assertThat(requestSpan.getLinks().getFirst().getSpanContext().getSpanId())
          .isEqualTo(channelSpan.getSpanContext().getSpanId());
    }
  }
}
