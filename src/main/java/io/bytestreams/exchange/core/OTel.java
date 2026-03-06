package io.bytestreams.exchange.core;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;

/**
 * OpenTelemetry constants and helpers.
 *
 * <p>Public {@link AttributeKey} constants define the metric and span attribute names used by
 * channel implementations. These are the canonical keys for querying telemetry data.
 *
 * <p>The {@link #meter()} and {@link #tracer()} helpers delegate to {@link
 * GlobalOpenTelemetry#get()}, which returns a no-op provider unless the OpenTelemetry SDK is
 * configured by the application.
 */
public final class OTel {
  public static final AttributeKey<String> CHANNEL_ID = AttributeKey.stringKey("channel_id");
  public static final AttributeKey<String> CHANNEL_TYPE = AttributeKey.stringKey("channel_type");
  public static final AttributeKey<String> ERROR_TYPE = AttributeKey.stringKey("error_type");
  public static final AttributeKey<String> MESSAGE_ID = AttributeKey.stringKey("message_id");
  public static final AttributeKey<String> MESSAGE_TYPE = AttributeKey.stringKey("message_type");
  public static final AttributeKey<String> SERVER_ADDRESS =
      AttributeKey.stringKey("server.address");
  public static final AttributeKey<Long> SERVER_PORT = AttributeKey.longKey("server.port");

  public static final double NANOS_PER_MS = 1_000_000.0;
  public static final String NAMESPACE = "io.bytestreams";

  /** Returns the default {@link Meter}, resolved lazily from {@link GlobalOpenTelemetry}. */
  public static Meter meter() {
    return GlobalOpenTelemetry.get().getMeter(NAMESPACE);
  }

  /** Returns the default {@link Tracer}, resolved lazily from {@link GlobalOpenTelemetry}. */
  public static Tracer tracer() {
    return GlobalOpenTelemetry.get().getTracer(NAMESPACE);
  }

  public static final AttributeKey<String> NETWORK_TRANSPORT =
      AttributeKey.stringKey("network.transport");
  public static final AttributeKey<String> NETWORK_TYPE = AttributeKey.stringKey("network.type");
  public static final AttributeKey<String> NETWORK_PEER_ADDRESS =
      AttributeKey.stringKey("network.peer.address");
  public static final AttributeKey<Long> NETWORK_PEER_PORT =
      AttributeKey.longKey("network.peer.port");

  private OTel() {}

  public static void endSpan(Span span, Throwable error) {
    if (error != null) {
      span.setStatus(StatusCode.ERROR);
      span.recordException(error);
    }
    span.end();
  }

  /** Returns the given attributes with error_type added if the throwable is non-null. */
  public static Attributes withError(Attributes attrs, Throwable error) {
    if (error == null) {
      return attrs;
    }
    return Attributes.builder()
        .putAll(attrs)
        .put(ERROR_TYPE, error.getClass().getSimpleName())
        .build();
  }
}
