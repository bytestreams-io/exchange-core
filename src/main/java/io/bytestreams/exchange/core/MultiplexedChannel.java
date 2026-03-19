package io.bytestreams.exchange.core;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ClientChannel} backed by a {@link Transport} using ID-based (multiplexed) correlation.
 *
 * <p>Responses are matched to requests by message ID, allowing out-of-order completion. Supports
 * bounded concurrency (backpressure). Uncorrelated responses are dispatched to the {@link
 * UnhandledMessageHandler}, which subclasses may override via {@link
 * #onUncorrelatedMessage(Object)}.
 *
 * @param <REQ> the request message type
 * @param <RESP> the response message type
 */
public class MultiplexedChannel<REQ, RESP> extends AbstractClientChannel<REQ, RESP> {

  private static final Logger log = LoggerFactory.getLogger(MultiplexedChannel.class);
  private static final String DEFAULT_CHANNEL_TYPE = "multiplexed";

  private final MultiplexedCorrelator<REQ, RESP> correlator;
  private final UnhandledMessageHandler<RESP> uncorrelatedHandler;
  private final Function<REQ, String> requestIdExtractor;

  MultiplexedChannel(
      Transport transport,
      int writeBufferSize,
      MessageWriter<REQ> requestWriter,
      MessageReader<RESP> responseReader,
      Function<REQ, String> requestIdExtractor,
      Function<RESP, String> responseIdExtractor,
      int maxConcurrency,
      UnhandledMessageHandler<RESP> uncorrelatedHandler,
      ErrorHandler<REQ, RESP> errorHandler,
      Duration defaultTimeout,
      long errorBackoffNanos,
      String channelType,
      SpanKind spanKind,
      Meter meter,
      Tracer tracer) {
    super(
        transport,
        writeBufferSize,
        requestWriter,
        responseReader,
        errorHandler,
        defaultTimeout,
        channelType,
        spanKind,
        errorBackoffNanos,
        meter,
        tracer);
    this.uncorrelatedHandler = uncorrelatedHandler;
    this.requestIdExtractor = requestIdExtractor;
    this.correlator =
        new MultiplexedCorrelator<>(requestIdExtractor, responseIdExtractor, maxConcurrency);
    this.closeFuture.whenComplete((ignored, throwable) -> correlator.onClose(throwable));
  }

  public static <REQ, RESP> Builder<REQ, RESP> builder() {
    return new Builder<>();
  }

  @Override
  Span buildRequestSpan(REQ request) {
    return tracer
        .spanBuilder("request")
        .addLink(channelSpan.getSpanContext())
        .setAttribute(OTel.MESSAGE_TYPE, request.getClass().getSimpleName())
        .setAttribute(OTel.MESSAGE_ID, requestIdExtractor.apply(request))
        .startSpan();
  }

  @Override
  CompletableFuture<RESP> registerRequest(REQ request) {
    return correlator.register(request);
  }

  @Override
  protected void onInbound(RESP response) {
    MultiplexedCorrelator.CorrelationResult result = correlator.correlate(response);
    if (!result.success()) {
      log.debug(
          "Uncorrelated inbound message on channel {}: messageId={}", id(), result.messageId());
      onUncorrelatedMessage(response);
    }
  }

  /**
   * Called when the read loop receives a response that cannot be correlated to any pending request.
   *
   * <p>The default implementation delegates to the {@link UnhandledMessageHandler} provided at
   * construction time. Subclasses (e.g. {@code SymmetricChannel}) may override this to handle
   * inbound requests that arrive on the same connection.
   *
   * @param response the uncorrelated response message
   */
  protected void onUncorrelatedMessage(RESP response) {
    uncorrelatedHandler.onMessage(response);
  }

  @Override
  protected boolean hasPending() {
    return !writeQueue.isEmpty() || correlator.hasPending();
  }

  public static final class Builder<REQ, RESP> {
    private Transport transport;
    private int writeBufferSize = 8192;
    private MessageWriter<REQ> requestWriter;
    private MessageReader<RESP> responseReader;
    private Function<REQ, String> requestIdExtractor;
    private Function<RESP, String> responseIdExtractor;
    private int maxConcurrency = Integer.MAX_VALUE;
    private UnhandledMessageHandler<RESP> uncorrelatedHandler = UnhandledMessageHandler.noop();
    private ErrorHandler<REQ, RESP> errorHandler = new ErrorHandler<>() {};
    private Duration defaultTimeout = Duration.ofSeconds(30);
    private long errorBackoffNanos = AbstractChannel.DEFAULT_ERROR_BACKOFF_NANOS;
    private Meter meter;
    private Tracer tracer;

    private Builder() {
      this.meter = OTel.meter();
      this.tracer = OTel.tracer();
    }

    public Builder<REQ, RESP> transport(Transport transport) {
      this.transport = transport;
      return this;
    }

    public Builder<REQ, RESP> writeBufferSize(int writeBufferSize) {
      this.writeBufferSize = writeBufferSize;
      return this;
    }

    public Builder<REQ, RESP> requestWriter(MessageWriter<REQ> requestWriter) {
      this.requestWriter = requestWriter;
      return this;
    }

    public Builder<REQ, RESP> responseReader(MessageReader<RESP> responseReader) {
      this.responseReader = responseReader;
      return this;
    }

    public Builder<REQ, RESP> requestIdExtractor(Function<REQ, String> requestIdExtractor) {
      this.requestIdExtractor = requestIdExtractor;
      return this;
    }

    public Builder<REQ, RESP> responseIdExtractor(Function<RESP, String> responseIdExtractor) {
      this.responseIdExtractor = responseIdExtractor;
      return this;
    }

    public Builder<REQ, RESP> maxConcurrency(int maxConcurrency) {
      this.maxConcurrency = maxConcurrency;
      return this;
    }

    public Builder<REQ, RESP> uncorrelatedHandler(
        UnhandledMessageHandler<RESP> uncorrelatedHandler) {
      this.uncorrelatedHandler = uncorrelatedHandler;
      return this;
    }

    public Builder<REQ, RESP> errorHandler(ErrorHandler<REQ, RESP> errorHandler) {
      this.errorHandler = errorHandler;
      return this;
    }

    public Builder<REQ, RESP> defaultTimeout(Duration defaultTimeout) {
      this.defaultTimeout = defaultTimeout;
      return this;
    }

    public Builder<REQ, RESP> errorBackoff(Duration errorBackoff) {
      this.errorBackoffNanos = errorBackoff.toNanos();
      return this;
    }

    public Builder<REQ, RESP> meter(Meter meter) {
      this.meter = Objects.requireNonNull(meter, "meter");
      return this;
    }

    public Builder<REQ, RESP> tracer(Tracer tracer) {
      this.tracer = Objects.requireNonNull(tracer, "tracer");
      return this;
    }

    public MultiplexedChannel<REQ, RESP> build() {
      if (transport == null) throw new IllegalStateException("transport is required");
      if (requestWriter == null) throw new IllegalStateException("requestWriter is required");
      if (responseReader == null) throw new IllegalStateException("responseReader is required");
      if (requestIdExtractor == null)
        throw new IllegalStateException("requestIdExtractor is required");
      if (responseIdExtractor == null)
        throw new IllegalStateException("responseIdExtractor is required");
      if (writeBufferSize <= 0)
        throw new IllegalArgumentException("writeBufferSize must be positive");
      if (maxConcurrency <= 0)
        throw new IllegalArgumentException("maxConcurrency must be positive");
      if (defaultTimeout == null || !defaultTimeout.isPositive()) {
        throw new IllegalArgumentException("defaultTimeout must be positive");
      }
      return new MultiplexedChannel<>(
          transport,
          writeBufferSize,
          requestWriter,
          responseReader,
          requestIdExtractor,
          responseIdExtractor,
          maxConcurrency,
          uncorrelatedHandler,
          errorHandler,
          defaultTimeout,
          errorBackoffNanos,
          DEFAULT_CHANNEL_TYPE,
          SpanKind.CLIENT,
          meter,
          tracer);
    }
  }
}
