package io.bytestreams.exchange.core;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * A {@link MultiplexedChannel} where request and response types are the same ({@code T=T}),
 * enabling bidirectional symmetric messaging on a single connection.
 *
 * <p>Outbound requests are multiplexed by message ID (via {@link #request}). Inbound messages that
 * cannot be correlated to a pending outbound request are treated as new inbound requests and
 * dispatched to the registered {@link RequestHandler}, which completes a future whose result is
 * enqueued for writing back to the remote peer.
 *
 * <p>The channel uses {@code SpanKind.INTERNAL} and reports {@code channel_type=symmetric} in OTel
 * metrics. Outbound requests carry {@code direction=outbound}; inbound requests carry {@code
 * direction=inbound}.
 *
 * @param <T> the message type (used for both requests and responses)
 */
public class SymmetricChannel<T> extends MultiplexedChannel<T, T> {

  private static final String CHANNEL_TYPE = "symmetric";

  private final RequestHandler<T, T> requestHandler;
  private final AtomicInteger activeRequests = new AtomicInteger();

  SymmetricChannel(
      Transport transport,
      int writeBufferSize,
      MessageWriter<T> writer,
      MessageReader<T> reader,
      Function<T, String> idExtractor,
      int maxConcurrency,
      RequestHandler<T, T> requestHandler,
      ErrorHandler<T, T> errorHandler,
      Duration defaultTimeout,
      long errorBackoffNanos,
      Meter meter,
      Tracer tracer) {
    super(
        transport,
        writeBufferSize,
        writer,
        reader,
        idExtractor,
        idExtractor,
        maxConcurrency,
        UnhandledMessageHandler.noop(),
        errorHandler,
        defaultTimeout,
        errorBackoffNanos,
        CHANNEL_TYPE,
        SpanKind.INTERNAL,
        meter,
        tracer);
    this.requestHandler = requestHandler;
  }

  public static <T> Builder<T> symmetricBuilder() {
    return new Builder<>();
  }

  /**
   * Returns request metric attributes that include {@code direction=outbound}, in addition to the
   * base channel_type and message_type attributes.
   */
  @Override
  Attributes requestAttributes(T request) {
    return Attributes.builder()
        .putAll(super.requestAttributes(request))
        .put(OTel.DIRECTION, OTel.Direction.OUTBOUND.value())
        .build();
  }

  /**
   * Called by the read loop for inbound messages that cannot be correlated to a pending outbound
   * request. Treats the message as a new inbound request: records metrics, creates a handle span,
   * wires response writing on completion, then dispatches to the registered {@link RequestHandler}.
   *
   * @param message the uncorrelated (inbound) message
   */
  @Override
  protected void onUncorrelatedMessage(T message) {
    handleInbound(message);
  }

  @Override
  protected boolean hasPending() {
    return super.hasPending() || activeRequests.get() > 0;
  }

  private void handleInbound(T message) {
    activeRequests.incrementAndGet();
    CompletableFuture<T> future = new CompletableFuture<>();
    Attributes attrs =
        Attributes.builder()
            .putAll(meterAttributes)
            .put(OTel.MESSAGE_TYPE, message.getClass().getSimpleName())
            .put(OTel.DIRECTION, OTel.Direction.INBOUND.value())
            .build();
    long startNanos = System.nanoTime();
    requestActive.add(1, attrs);
    Span handleSpan =
        tracer
            .spanBuilder("handle")
            .addLink(channelSpan.getSpanContext())
            .setAttribute(OTel.MESSAGE_TYPE, message.getClass().getSimpleName())
            .startSpan();
    future.whenComplete(
        (resp, err) -> {
          try {
            double durationMs = (System.nanoTime() - startNanos) / OTel.NANOS_PER_MS;
            requestTotal.add(1, OTel.withError(attrs, err));
            requestActive.add(-1, attrs);
            requestDuration.record(durationMs, OTel.withError(attrs, err));
            if (err != null) {
              requestErrors.add(1, OTel.withError(attrs, err));
              OTel.endSpan(handleSpan, err);
            } else if (resp == null) {
              NullPointerException npe = new NullPointerException("response must not be null");
              requestErrors.add(1, OTel.withError(attrs, npe));
              OTel.endSpan(handleSpan, npe);
            } else {
              try {
                writeQueue.put(resp);
                writeQueueSize.add(1, meterAttributes);
                OTel.endSpan(handleSpan, null);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                OTel.endSpan(handleSpan, e);
                if (errorHandler.stopOnError(
                    this, new ErrorContext<>(e, Optional.of(message), Optional.empty()))) {
                  readFuture.completeExceptionally(e);
                  Closeables.closeQuietly(transport);
                }
              }
            }
          } finally {
            activeRequests.decrementAndGet();
            interruptIfDrained();
          }
        });
    future.orTimeout(defaultTimeout.toNanos(), TimeUnit.NANOSECONDS);
    requestHandler.handle(message, future);
  }

  /** Builder for {@link SymmetricChannel}. */
  public static final class Builder<T> {
    private Transport transport;
    private int writeBufferSize = 8192;
    private MessageWriter<T> writer;
    private MessageReader<T> reader;
    private Function<T, String> idExtractor;
    private int maxConcurrency = Integer.MAX_VALUE;
    private RequestHandler<T, T> requestHandler;
    private ErrorHandler<T, T> errorHandler = new ErrorHandler<>() {};
    private Duration defaultTimeout = Duration.ofSeconds(30);
    private long errorBackoffNanos = AbstractChannel.DEFAULT_ERROR_BACKOFF_NANOS;
    private Meter meter;
    private Tracer tracer;

    private Builder() {
      this.meter = OTel.meter();
      this.tracer = OTel.tracer();
    }

    public Builder<T> transport(Transport transport) {
      this.transport = transport;
      return this;
    }

    public Builder<T> writeBufferSize(int writeBufferSize) {
      this.writeBufferSize = writeBufferSize;
      return this;
    }

    public Builder<T> writer(MessageWriter<T> writer) {
      this.writer = writer;
      return this;
    }

    public Builder<T> reader(MessageReader<T> reader) {
      this.reader = reader;
      return this;
    }

    public Builder<T> idExtractor(Function<T, String> idExtractor) {
      this.idExtractor = idExtractor;
      return this;
    }

    public Builder<T> maxConcurrency(int maxConcurrency) {
      this.maxConcurrency = maxConcurrency;
      return this;
    }

    public Builder<T> requestHandler(RequestHandler<T, T> requestHandler) {
      this.requestHandler = requestHandler;
      return this;
    }

    public Builder<T> errorHandler(ErrorHandler<T, T> errorHandler) {
      this.errorHandler = errorHandler;
      return this;
    }

    public Builder<T> defaultTimeout(Duration defaultTimeout) {
      this.defaultTimeout = defaultTimeout;
      return this;
    }

    public Builder<T> errorBackoff(Duration errorBackoff) {
      this.errorBackoffNanos = errorBackoff.toNanos();
      return this;
    }

    public Builder<T> meter(Meter meter) {
      this.meter = Objects.requireNonNull(meter, "meter");
      return this;
    }

    public Builder<T> tracer(Tracer tracer) {
      this.tracer = Objects.requireNonNull(tracer, "tracer");
      return this;
    }

    public SymmetricChannel<T> build() {
      if (transport == null) throw new IllegalStateException("transport is required");
      if (writer == null) throw new IllegalStateException("writer is required");
      if (reader == null) throw new IllegalStateException("reader is required");
      if (idExtractor == null) throw new IllegalStateException("idExtractor is required");
      if (requestHandler == null) throw new IllegalStateException("requestHandler is required");
      if (writeBufferSize <= 0)
        throw new IllegalArgumentException("writeBufferSize must be positive");
      if (maxConcurrency <= 0)
        throw new IllegalArgumentException("maxConcurrency must be positive");
      if (defaultTimeout == null || !defaultTimeout.isPositive()) {
        throw new IllegalArgumentException("defaultTimeout must be positive");
      }
      return new SymmetricChannel<>(
          transport,
          writeBufferSize,
          writer,
          reader,
          idExtractor,
          maxConcurrency,
          requestHandler,
          errorHandler,
          defaultTimeout,
          errorBackoffNanos,
          meter,
          tracer);
    }
  }
}
