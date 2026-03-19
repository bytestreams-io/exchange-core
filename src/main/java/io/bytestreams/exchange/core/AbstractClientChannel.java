package io.bytestreams.exchange.core;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Abstract base for {@link Transport}-backed client channels.
 *
 * <p>Provides shared infrastructure: transport lifecycle, write queue and write loop, OTel channel
 * span and metrics, and graceful close semantics. Two virtual threads drive the I/O: a
 * <em>writer</em> managed by this class and a <em>reader</em> provided by each subclass.
 *
 * @param <REQ> the request message type
 * @param <RESP> the response message type
 */
abstract class AbstractClientChannel<REQ, RESP> extends AbstractChannel<REQ, RESP>
    implements ClientChannel<REQ, RESP> {

  protected final Tracer tracer;
  protected final Object writeLock = new Object();
  protected final ErrorHandler<REQ, RESP> errorHandler;

  AbstractClientChannel(
      Transport transport,
      int writeBufferSize,
      MessageWriter<REQ> requestWriter,
      MessageReader<RESP> responseReader,
      ErrorHandler<REQ, RESP> errorHandler,
      Duration defaultTimeout,
      String channelType,
      SpanKind spanKind,
      long errorBackoffNanos,
      Meter meter,
      Tracer tracer) {
    super(
        transport,
        writeBufferSize,
        channelType,
        spanKind,
        errorBackoffNanos,
        meter,
        tracer,
        requestWriter,
        responseReader,
        defaultTimeout,
        // Client channels add peer address; server channels omit it to avoid high cardinality.
        Attributes.builder()
            .put(OTel.NETWORK_PEER_ADDRESS, transport.attributes().get(OTel.NETWORK_PEER_ADDRESS))
            .build());
    this.errorHandler = errorHandler;
    this.tracer = tracer;
  }

  @Override
  public CompletableFuture<RESP> request(REQ request) {
    return request(request, defaultTimeout);
  }

  @Override
  public CompletableFuture<RESP> request(REQ request, Duration timeout) {
    if (timeout == null || !timeout.isPositive()) {
      throw new IllegalArgumentException("timeout must be positive");
    }
    if (status() == ChannelStatus.INIT) {
      throw new IllegalStateException("Channel not started");
    }
    Span requestSpan = buildRequestSpan(request);
    if (SHUTTING_DOWN.contains(status())) {
      CancellationException channelClosed = new CancellationException("Channel closed");
      OTel.endSpan(requestSpan, channelClosed);
      return CompletableFuture.failedFuture(channelClosed);
    }
    try {
      synchronized (writeLock) {
        CompletableFuture<RESP> future;
        try {
          future = registerRequest(request);
        } catch (RuntimeException e) {
          OTel.endSpan(requestSpan, e);
          throw e;
        }
        Attributes attrs = requestAttributes(request);
        long startNanos = System.nanoTime();
        requestActive.add(1, attrs);
        future.whenComplete(
            (resp, e) -> {
              double durationMs = (System.nanoTime() - startNanos) / OTel.NANOS_PER_MS;
              requestTotal.add(1, OTel.withError(attrs, e));
              requestActive.add(-1, attrs);
              requestDuration.record(durationMs, OTel.withError(attrs, e));
              if (e != null) {
                requestErrors.add(1, OTel.withError(attrs, e));
              }
              OTel.endSpan(requestSpan, e);
              interruptIfDrained();
            });
        future.orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
        writeQueue.put(request);
        writeQueueSize.add(1, meterAttributes);
        return future;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      OTel.endSpan(requestSpan, e);
      return CompletableFuture.failedFuture(e);
    }
  }

  /** Creates the OTel span for a request. Subclasses may override to add extra attributes. */
  Span buildRequestSpan(REQ request) {
    return tracer
        .spanBuilder("request")
        .addLink(channelSpan.getSpanContext())
        .setAttribute(OTel.MESSAGE_TYPE, request.getClass().getSimpleName())
        .startSpan();
  }

  /** Registers a request with the channel's correlator and returns its completion future. */
  abstract CompletableFuture<RESP> registerRequest(REQ request);

  Attributes requestAttributes(REQ request) {
    return buildMessageAttributes(request);
  }

  @Override
  boolean stopOnInboundError(Exception error, RESP message) {
    return errorHandler.stopOnError(
        this, new ErrorContext<>(error, Optional.empty(), Optional.ofNullable(message)));
  }

  @Override
  boolean stopOnOutboundError(Exception error, REQ message) {
    return errorHandler.stopOnError(
        this, new ErrorContext<>(error, Optional.ofNullable(message), Optional.empty()));
  }
}
