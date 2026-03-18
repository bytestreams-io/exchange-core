package io.bytestreams.exchange.core;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ClientChannel} backed by a {@link Transport} using positional (pipelined) correlation.
 *
 * <p>Responses are matched to requests in FIFO order. Supports bounded concurrency (backpressure).
 * Set {@code maxConcurrency=1} for lockstep (one in-flight request at a time).
 *
 * @param <REQ> the request message type
 * @param <RESP> the response message type
 */
public class PipelinedChannel<REQ, RESP> extends AbstractClientChannel<REQ, RESP> {

  private static final Logger log = LoggerFactory.getLogger(PipelinedChannel.class);
  private static final String CHANNEL_TYPE = "pipelined";

  private final PipelinedCorrelator<RESP> correlator;

  PipelinedChannel(
      Transport transport,
      int writeBufferSize,
      MessageWriter<REQ> requestWriter,
      MessageReader<RESP> responseReader,
      int maxConcurrency,
      ErrorHandler<REQ, RESP> errorHandler,
      Duration defaultTimeout,
      long errorBackoffNanos,
      Meter meter,
      Tracer tracer) {
    super(
        transport,
        writeBufferSize,
        requestWriter,
        responseReader,
        errorHandler,
        defaultTimeout,
        CHANNEL_TYPE,
        SpanKind.CLIENT,
        errorBackoffNanos,
        meter,
        tracer);
    this.correlator = new PipelinedCorrelator<>(maxConcurrency);
    this.closeFuture.whenComplete((ignored, throwable) -> correlator.onClose(throwable));
  }

  public static <REQ, RESP> Builder<REQ, RESP> builder() {
    return new Builder<>();
  }

  @Override
  CompletableFuture<RESP> registerRequest(REQ request) {
    return correlator.register();
  }

  @Override
  protected void onInbound(RESP response) {
    if (!correlator.correlate(response)) {
      IllegalStateException uncorrelated =
          new IllegalStateException(
              "Pipelined response could not be correlated"
                  + " (no pending request or future already completed)");
      log.warn("Uncorrelated pipelined response on channel {}", id());
      if (errorHandler.stopOnError(
          this, new ErrorContext<>(uncorrelated, Optional.empty(), Optional.of(response)))) {
        throw uncorrelated;
      }
    }
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
    private int maxConcurrency = Integer.MAX_VALUE;
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

    public Builder<REQ, RESP> maxConcurrency(int maxConcurrency) {
      this.maxConcurrency = maxConcurrency;
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

    public PipelinedChannel<REQ, RESP> build() {
      if (transport == null) throw new IllegalStateException("transport is required");
      if (requestWriter == null) throw new IllegalStateException("requestWriter is required");
      if (responseReader == null) throw new IllegalStateException("responseReader is required");
      if (writeBufferSize <= 0)
        throw new IllegalArgumentException("writeBufferSize must be positive");
      if (maxConcurrency <= 0)
        throw new IllegalArgumentException("maxConcurrency must be positive");
      if (defaultTimeout == null || !defaultTimeout.isPositive()) {
        throw new IllegalArgumentException("defaultTimeout must be positive");
      }
      return new PipelinedChannel<>(
          transport,
          writeBufferSize,
          requestWriter,
          responseReader,
          maxConcurrency,
          errorHandler,
          defaultTimeout,
          errorBackoffNanos,
          meter,
          tracer);
    }
  }
}
