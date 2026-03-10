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

/**
 * A {@link Channel} backed by a {@link Transport} that receives requests and dispatches them to a
 * registered handler.
 *
 * <p>Two virtual threads drive the channel: a <em>reader</em> that decodes requests from the
 * transport input stream and dispatches them to the registered handler, and a <em>writer</em> that
 * drains the response queue and encodes responses to the transport output stream. When the handler
 * completes the future provided by the reader, the response is enqueued for writing.
 *
 * <p>I/O and unexpected errors are routed to the {@link ErrorHandler}. If the handler returns
 * {@code true} (the default), the channel closes the transport and completes its close future
 * exceptionally. Return {@code false} to tolerate the error and continue processing. Unrecoverable
 * errors that bypass the inner I/O catch (e.g. {@link RuntimeException}) are caught by an outer
 * safety net that guarantees the close future always resolves.
 *
 * @param <REQ> the request message type
 * @param <RESP> the response message type
 */
public class ServerChannel<REQ, RESP> extends AbstractChannel<RESP, REQ> {

  private static final String CHANNEL_TYPE = "server";
  private final ErrorHandler<REQ, RESP> errorHandler;
  private final Tracer tracer;
  private final RequestHandler<REQ, RESP> requestHandler;
  private final AtomicInteger activeRequests = new AtomicInteger();

  ServerChannel(
      Transport transport,
      int writeBufferSize,
      MessageReader<REQ> requestReader,
      MessageWriter<RESP> responseWriter,
      RequestHandler<REQ, RESP> requestHandler,
      ErrorHandler<REQ, RESP> errorHandler,
      Duration defaultTimeout,
      long errorBackoffNanos,
      Meter meter,
      Tracer tracer) {
    super(
        transport,
        writeBufferSize,
        CHANNEL_TYPE,
        SpanKind.SERVER,
        errorBackoffNanos,
        meter,
        tracer,
        responseWriter,
        requestReader,
        defaultTimeout,
        Attributes.empty());
    this.requestHandler = requestHandler;
    this.errorHandler = errorHandler;
    this.tracer = tracer;
  }

  public static <REQ, RESP> Builder<REQ, RESP> builder() {
    return new Builder<>();
  }

  @Override
  protected void onInbound(REQ request) {
    activeRequests.incrementAndGet();
    CompletableFuture<RESP> future = new CompletableFuture<>();
    Attributes attrs = buildMessageAttributes(request);
    long startNanos = System.nanoTime();
    requestActive.add(1, attrs);
    Span handleSpan =
        tracer
            .spanBuilder("handle")
            .addLink(channelSpan.getSpanContext())
            .setAttribute(OTel.MESSAGE_TYPE, request.getClass().getSimpleName())
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
                if (stopOnOutboundError(e, resp)) {
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
    requestHandler.handle(request, future);
  }

  @Override
  protected boolean hasPending() {
    return !writeQueue.isEmpty() || activeRequests.get() > 0;
  }

  @Override
  boolean stopOnInboundError(Exception error, REQ message) {
    return errorHandler.stopOnError(
        this, new ErrorContext<>(error, Optional.ofNullable(message), Optional.empty()));
  }

  @Override
  boolean stopOnOutboundError(Exception error, RESP message) {
    return errorHandler.stopOnError(
        this, new ErrorContext<>(error, Optional.empty(), Optional.ofNullable(message)));
  }

  /** Builder for {@link ServerChannel}. */
  public static final class Builder<REQ, RESP> {
    private Transport transport;
    private int writeBufferSize = 8192;
    private MessageReader<REQ> requestReader;
    private MessageWriter<RESP> responseWriter;
    private RequestHandler<REQ, RESP> requestHandler;
    private ErrorHandler<REQ, RESP> errorHandler = new ErrorHandler<>() {};
    private Duration defaultTimeout = Duration.ofSeconds(30);
    private long errorBackoffNanos = DEFAULT_ERROR_BACKOFF_NANOS;
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

    public Builder<REQ, RESP> requestReader(MessageReader<REQ> requestReader) {
      this.requestReader = requestReader;
      return this;
    }

    public Builder<REQ, RESP> responseWriter(MessageWriter<RESP> responseWriter) {
      this.responseWriter = responseWriter;
      return this;
    }

    public Builder<REQ, RESP> requestHandler(RequestHandler<REQ, RESP> requestHandler) {
      this.requestHandler = requestHandler;
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

    public ServerChannel<REQ, RESP> build() {
      if (transport == null) throw new IllegalStateException("transport is required");
      if (requestReader == null) throw new IllegalStateException("requestReader is required");
      if (responseWriter == null) throw new IllegalStateException("responseWriter is required");
      if (requestHandler == null) throw new IllegalStateException("requestHandler is required");
      if (writeBufferSize <= 0)
        throw new IllegalArgumentException("writeBufferSize must be positive");
      if (defaultTimeout == null || !defaultTimeout.isPositive()) {
        throw new IllegalArgumentException("defaultTimeout must be positive");
      }
      return new ServerChannel<>(
          transport,
          writeBufferSize,
          requestReader,
          responseWriter,
          requestHandler,
          errorHandler,
          defaultTimeout,
          errorBackoffNanos,
          meter,
          tracer);
    }
  }
}
