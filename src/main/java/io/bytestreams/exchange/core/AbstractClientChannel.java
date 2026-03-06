package io.bytestreams.exchange.core;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

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
