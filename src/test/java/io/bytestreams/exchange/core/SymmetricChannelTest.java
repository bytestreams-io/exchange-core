package io.bytestreams.exchange.core;

import static io.bytestreams.exchange.core.TestFixture.CHANNEL_TYPE;
import static io.bytestreams.exchange.core.TestFixture.DIRECTION;
import static io.bytestreams.exchange.core.TestFixture.MESSAGE_TYPE;
import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.equalTo;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
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
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SymmetricChannelTest extends AbstractChannelTestBase {

  // ID extractor: use the string itself as the ID
  private static final Function<String, String> ID_EXTRACTOR = s -> s;

  @Override
  SymmetricChannel<String> createChannel() {
    return createChannel((req, future) -> {});
  }

  @Override
  SymmetricChannel<String> createUninitializedChannel() {
    return createUninitializedChannel((req, future) -> {});
  }

  SymmetricChannel<String> createChannel(RequestHandler<String, String> handler) {
    var ch = createUninitializedChannel(handler);
    ch.start();
    return ch;
  }

  private SymmetricChannel<String> createUninitializedChannel(
      RequestHandler<String, String> handler) {
    return new SymmetricChannel<>(
        transport,
        Integer.MAX_VALUE,
        TestFixture.FRAMED_WRITER,
        TestFixture.FRAMED_READER,
        ID_EXTRACTOR,
        Integer.MAX_VALUE,
        handler,
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
          SymmetricChannel.<String>symmetricBuilder()
              .writer(TestFixture.FRAMED_WRITER)
              .reader(TestFixture.FRAMED_READER)
              .idExtractor(ID_EXTRACTOR)
              .requestHandler((req, future) -> {});
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("transport");
    }

    @Test
    void builder_missing_writer_throws() {
      var builder =
          SymmetricChannel.<String>symmetricBuilder()
              .transport(transport)
              .reader(TestFixture.FRAMED_READER)
              .idExtractor(ID_EXTRACTOR)
              .requestHandler((req, future) -> {});
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("writer");
    }

    @Test
    void builder_missing_reader_throws() {
      var builder =
          SymmetricChannel.<String>symmetricBuilder()
              .transport(transport)
              .writer(TestFixture.FRAMED_WRITER)
              .idExtractor(ID_EXTRACTOR)
              .requestHandler((req, future) -> {});
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("reader");
    }

    @Test
    void builder_missing_idExtractor_throws() {
      var builder =
          SymmetricChannel.<String>symmetricBuilder()
              .transport(transport)
              .writer(TestFixture.FRAMED_WRITER)
              .reader(TestFixture.FRAMED_READER)
              .requestHandler((req, future) -> {});
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("idExtractor");
    }

    @Test
    void builder_missing_requestHandler_throws() {
      var builder =
          SymmetricChannel.<String>symmetricBuilder()
              .transport(transport)
              .writer(TestFixture.FRAMED_WRITER)
              .reader(TestFixture.FRAMED_READER)
              .idExtractor(ID_EXTRACTOR);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("requestHandler");
    }

    @Test
    void builder_zero_writeBufferSize_throws() {
      var builder =
          SymmetricChannel.<String>symmetricBuilder()
              .transport(transport)
              .writer(TestFixture.FRAMED_WRITER)
              .reader(TestFixture.FRAMED_READER)
              .idExtractor(ID_EXTRACTOR)
              .requestHandler((req, future) -> {})
              .writeBufferSize(0);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("writeBufferSize");
    }

    @Test
    void builder_zero_maxConcurrency_throws() {
      var builder =
          SymmetricChannel.<String>symmetricBuilder()
              .transport(transport)
              .writer(TestFixture.FRAMED_WRITER)
              .reader(TestFixture.FRAMED_READER)
              .idExtractor(ID_EXTRACTOR)
              .requestHandler((req, future) -> {})
              .maxConcurrency(0);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("maxConcurrency");
    }

    @Test
    void builder_creates_channel_with_all_options() {
      var ch =
          SymmetricChannel.<String>symmetricBuilder()
              .transport(transport)
              .writer(TestFixture.FRAMED_WRITER)
              .reader(TestFixture.FRAMED_READER)
              .idExtractor(ID_EXTRACTOR)
              .maxConcurrency(10)
              .writeBufferSize(4096)
              .defaultTimeout(Duration.ofSeconds(5))
              .requestHandler((req, future) -> {})
              .errorHandler(new ErrorHandler<>() {})
              .errorBackoff(Duration.ofMillis(50))
              .meter(OTel.meter())
              .tracer(OTel.tracer())
              .build();
      channel = ch;
      assertThat(ch.status()).isEqualTo(ChannelStatus.INIT);
    }

    @Test
    void builder_default_timeout_zero_throws() {
      var builder =
          SymmetricChannel.<String>symmetricBuilder()
              .transport(transport)
              .writer(TestFixture.FRAMED_WRITER)
              .reader(TestFixture.FRAMED_READER)
              .idExtractor(ID_EXTRACTOR)
              .requestHandler((req, future) -> {})
              .defaultTimeout(Duration.ZERO);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("defaultTimeout");
    }

    @Test
    void builder_default_timeout_negative_throws() {
      var builder =
          SymmetricChannel.<String>symmetricBuilder()
              .transport(transport)
              .writer(TestFixture.FRAMED_WRITER)
              .reader(TestFixture.FRAMED_READER)
              .idExtractor(ID_EXTRACTOR)
              .requestHandler((req, future) -> {})
              .defaultTimeout(Duration.ofSeconds(-1));
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("defaultTimeout");
    }

    @Test
    void builder_default_timeout_null_throws() {
      var builder =
          SymmetricChannel.<String>symmetricBuilder()
              .transport(transport)
              .writer(TestFixture.FRAMED_WRITER)
              .reader(TestFixture.FRAMED_READER)
              .idExtractor(ID_EXTRACTOR)
              .requestHandler((req, future) -> {})
              .defaultTimeout(null);
      assertThatThrownBy(builder::build)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("defaultTimeout");
    }

    @Test
    void builder_null_meter_throws() {
      var builder =
          SymmetricChannel.<String>symmetricBuilder()
              .transport(transport)
              .writer(TestFixture.FRAMED_WRITER)
              .reader(TestFixture.FRAMED_READER)
              .idExtractor(ID_EXTRACTOR)
              .requestHandler((req, future) -> {});
      assertThatThrownBy(() -> builder.meter(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("meter");
    }

    @Test
    void builder_null_tracer_throws() {
      var builder =
          SymmetricChannel.<String>symmetricBuilder()
              .transport(transport)
              .writer(TestFixture.FRAMED_WRITER)
              .reader(TestFixture.FRAMED_READER)
              .idExtractor(ID_EXTRACTOR)
              .requestHandler((req, future) -> {});
      assertThatThrownBy(() -> builder.tracer(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("tracer");
    }
  }

  // -- Outbound request --

  @Nested
  class OutboundRequest {
    @Test
    void outbound_request_sends_and_receives_response() throws Exception {
      var ch = createChannel();
      channel = ch;
      CompletableFuture<String> future = ch.request("reqA");
      TestFixture.writeFramed("reqA", serverOut);
      assertThat(future).succeedsWithin(Duration.ofMillis(500)).isEqualTo("reqA");
    }

    @Test
    void outbound_out_of_order_responses_complete_correct_futures() throws Exception {
      var ch = createChannel();
      channel = ch;
      CompletableFuture<String> futureA = ch.request("reqA");
      CompletableFuture<String> futureB = ch.request("reqB");
      // Send response for B first, then A (out of order)
      TestFixture.writeFramed("reqB", serverOut);
      assertThat(futureB).succeedsWithin(Duration.ofMillis(500)).isEqualTo("reqB");
      assertThat(futureA).isNotDone();
      TestFixture.writeFramed("reqA", serverOut);
      assertThat(futureA).succeedsWithin(Duration.ofMillis(500)).isEqualTo("reqA");
    }
  }

  // -- Inbound request handling --

  @Nested
  class InboundRequest {
    @Test
    void uncorrelated_message_routed_to_request_handler() throws Exception {
      CompletableFuture<String> received = new CompletableFuture<>();
      var ch = createChannel((req, future) -> received.complete(req));
      channel = ch;
      TestFixture.writeFramed("inboundMsg", serverOut);
      assertThat(received).succeedsWithin(Duration.ofMillis(500)).isEqualTo("inboundMsg");
    }

    @Test
    void inbound_handler_future_completion_writes_response() throws Exception {
      var ch = createChannel((req, future) -> future.complete("response-" + req));
      channel = ch;
      TestFixture.writeFramed("inboundMsg", serverOut);
      assertThat(TestFixture.readFramed(serverIn)).isEqualTo("response-inboundMsg");
    }

    @Test
    void inbound_handler_completing_exceptionally_does_not_close_channel() throws Exception {
      CompletableFuture<Void> done = new CompletableFuture<>();
      var ch =
          createChannel(
              (req, future) -> {
                future.completeExceptionally(new RuntimeException("handler error"));
                done.complete(null);
              });
      channel = ch;
      TestFixture.writeFramed("inboundMsg", serverOut);
      done.get();
      // Channel should still be running after inbound error
      await().during(Duration.ofMillis(100)).until(() -> ch.status() != ChannelStatus.CLOSED);
    }

    @Test
    void interrupted_in_whenComplete_put_calls_error_handler() throws Exception {
      CompletableFuture<CompletableFuture<String>> futureHolder = new CompletableFuture<>();
      var ch = createChannel((req, future) -> futureHolder.complete(future));
      channel = ch;
      TestFixture.writeFramed("inboundMsg", serverOut);
      CompletableFuture<String> handlerFuture =
          futureHolder.get(1, java.util.concurrent.TimeUnit.SECONDS);

      // Complete the future from a pre-interrupted thread so put() throws InterruptedException
      CompletableFuture<Void> completed = new CompletableFuture<>();
      VT.start(
          () -> {
            Thread.currentThread().interrupt();
            handlerFuture.complete("resp");
            completed.complete(null);
          },
          "pre-interrupted-completer");
      completed.get(1, java.util.concurrent.TimeUnit.SECONDS);

      verify(errorHandler, timeout(1000)).stopOnError(any(), any());
    }

    @Test
    void interrupted_in_whenComplete_put_closes_transport_when_handler_returns_true()
        throws Exception {
      transport = spy(transport);
      when(errorHandler.stopOnError(any(), any())).thenReturn(true);

      CompletableFuture<CompletableFuture<String>> futureHolder = new CompletableFuture<>();
      var ch = createChannel((req, future) -> futureHolder.complete(future));
      channel = ch;
      TestFixture.writeFramed("inboundMsg", serverOut);
      CompletableFuture<String> handlerFuture =
          futureHolder.get(1, java.util.concurrent.TimeUnit.SECONDS);

      CompletableFuture<Void> completed = new CompletableFuture<>();
      VT.start(
          () -> {
            Thread.currentThread().interrupt();
            handlerFuture.complete("resp");
            completed.complete(null);
          },
          "pre-interrupted-completer-close");
      completed.get(1, java.util.concurrent.TimeUnit.SECONDS);

      verify(transport, timeout(1000).atLeastOnce()).close();
    }

    @Test
    void interrupted_in_whenComplete_put_continues_when_handler_returns_false() throws Exception {
      CompletableFuture<CompletableFuture<String>> futureHolder = new CompletableFuture<>();
      var ch = createChannel((req, future) -> futureHolder.complete(future));
      channel = ch;
      TestFixture.writeFramed("inboundMsg", serverOut);
      CompletableFuture<String> handlerFuture =
          futureHolder.get(1, java.util.concurrent.TimeUnit.SECONDS);

      CompletableFuture<Void> completed = new CompletableFuture<>();
      VT.start(
          () -> {
            Thread.currentThread().interrupt();
            handlerFuture.complete("resp");
            completed.complete(null);
          },
          "pre-interrupted-completer-continue");
      completed.get(1, java.util.concurrent.TimeUnit.SECONDS);

      verify(errorHandler, timeout(1000)).stopOnError(any(), any());
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
      TestFixture.writeFramed("inboundMsg", serverOut);
      done.get();
      // Channel should still be running after null response
      await().during(Duration.ofMillis(200)).until(() -> ch.status() != ChannelStatus.CLOSED);
    }
  }

  // -- Bidirectional --

  @Nested
  class Bidirectional {
    @Test
    void interleaved_outbound_and_inbound_messages() throws Exception {
      CompletableFuture<String> inboundReceived = new CompletableFuture<>();
      var ch = createChannel((req, future) -> inboundReceived.complete(req));
      channel = ch;

      // Send outbound request
      CompletableFuture<String> outboundFuture = ch.request("outboundReq");
      // Feed response for outbound request
      TestFixture.writeFramed("outboundReq", serverOut);
      assertThat(outboundFuture).succeedsWithin(Duration.ofMillis(500)).isEqualTo("outboundReq");

      // Feed inbound message (no matching outbound request, goes to handler)
      TestFixture.writeFramed("inboundMsg", serverOut);
      assertThat(inboundReceived).succeedsWithin(Duration.ofMillis(500)).isEqualTo("inboundMsg");
    }
  }

  // -- Close lifecycle --

  @Nested
  class CloseLifecycle {
    @Test
    void close_transitions_to_closed_status() {
      var ch = createChannel();
      channel = ch;
      CompletableFuture<Void> closeFuture = ch.close();
      assertThat(closeFuture).succeedsWithin(Duration.ofMillis(500));
      assertThat(ch.status()).isEqualTo(ChannelStatus.CLOSED);
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

    private SymmetricChannel<String> createMetricChannel() {
      return createMetricChannel((req, future) -> {});
    }

    private SymmetricChannel<String> createMetricChannel(RequestHandler<String, String> handler) {
      var ch =
          new SymmetricChannel<>(
              transport,
              Integer.MAX_VALUE,
              TestFixture.FRAMED_WRITER,
              TestFixture.FRAMED_READER,
              ID_EXTRACTOR,
              Integer.MAX_VALUE,
              handler,
              errorHandler,
              Duration.ofSeconds(30),
              AbstractChannel.DEFAULT_ERROR_BACKOFF_NANOS,
              meterProvider.get(OTel.NAMESPACE),
              OTel.tracer());
      ch.start();
      return ch;
    }

    @Test
    void channel_type_is_symmetric_in_outbound_metrics() {
      var ch = createMetricChannel();
      channel = ch;
      ch.request("reqA");
      TestFixture.assertLongSum(
          metricReader,
          "request.active",
          1,
          equalTo(CHANNEL_TYPE, "symmetric"),
          equalTo(MESSAGE_TYPE, "String"));
    }

    @Test
    void outbound_request_has_direction_outbound() {
      var ch = createMetricChannel();
      channel = ch;
      ch.request("reqA");
      TestFixture.assertLongSum(
          metricReader,
          "request.active",
          1,
          equalTo(CHANNEL_TYPE, "symmetric"),
          equalTo(DIRECTION, "outbound"),
          equalTo(MESSAGE_TYPE, "String"));
    }

    @Test
    void inbound_request_has_direction_inbound() throws Exception {
      CountDownLatch handlerLatch = new CountDownLatch(1);
      var ch =
          createMetricChannel(
              (req, future) -> {
                // Hold future open so request.active stays at 1
                handlerLatch.countDown();
              });
      channel = ch;
      TestFixture.writeFramed("inboundMsg", serverOut);
      handlerLatch.await();
      await()
          .atMost(Duration.ofMillis(500))
          .untilAsserted(
              () ->
                  TestFixture.assertLongSum(
                      metricReader,
                      "request.active",
                      1,
                      equalTo(CHANNEL_TYPE, "symmetric"),
                      equalTo(DIRECTION, "inbound"),
                      equalTo(MESSAGE_TYPE, "String")));
    }

    @Test
    void inbound_request_total_incremented_on_completion() throws Exception {
      CompletableFuture<Void> done = new CompletableFuture<>();
      var ch =
          createMetricChannel(
              (req, future) -> {
                future.complete("resp-" + req);
                done.complete(null);
              });
      channel = ch;
      TestFixture.writeFramed("inboundMsg", serverOut);
      done.get();
      await()
          .atMost(Duration.ofMillis(500))
          .untilAsserted(
              () ->
                  TestFixture.assertLongSum(
                      metricReader,
                      "request.total",
                      1,
                      equalTo(CHANNEL_TYPE, "symmetric"),
                      equalTo(DIRECTION, "inbound"),
                      equalTo(MESSAGE_TYPE, "String")));
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

    private SymmetricChannel<String> createTracedChannel() {
      return createTracedChannel((req, future) -> {});
    }

    private SymmetricChannel<String> createTracedChannel(RequestHandler<String, String> handler) {
      var ch =
          new SymmetricChannel<>(
              transport,
              Integer.MAX_VALUE,
              TestFixture.FRAMED_WRITER,
              TestFixture.FRAMED_READER,
              ID_EXTRACTOR,
              Integer.MAX_VALUE,
              handler,
              errorHandler,
              Duration.ofSeconds(30),
              AbstractChannel.DEFAULT_ERROR_BACKOFF_NANOS,
              OTel.meter(),
              tracerProvider.get("test"));
      ch.start();
      return ch;
    }

    @Test
    void channel_span_has_INTERNAL_kind() {
      var ch = createTracedChannel();
      channel = ch;
      ch.close();
      await()
          .atMost(Duration.ofSeconds(2))
          .until(
              () ->
                  spanExporter.getFinishedSpanItems().stream()
                      .anyMatch(s -> s.getName().equals("symmetric")));
      SpanData span =
          spanExporter.getFinishedSpanItems().stream()
              .filter(s -> s.getName().equals("symmetric"))
              .findFirst()
              .orElseThrow();
      assertThat(span).hasName("symmetric").hasKind(SpanKind.INTERNAL);
    }

    @Test
    void handle_span_created_for_inbound() throws Exception {
      var ch = createTracedChannel((req, future) -> future.complete("resp"));
      channel = ch;
      TestFixture.writeFramed("inboundMsg", serverOut);
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
  }
}
