package io.bytestreams.exchange.core;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Accepts transports from a {@link TransportFactory} and creates channels for each.
 *
 * <p>A virtual thread runs the accept loop, calling {@link TransportFactory#create()} repeatedly.
 * Each returned transport is passed to the channel factory, which creates and starts a channel.
 * Accepted channels are tracked and automatically closed when the acceptor shuts down.
 *
 * <p>Error strategy: if the channel factory throws, the accepted transport is closed, the error is
 * logged (via span event), and the accept loop continues.
 *
 * <p>Example with TCP sockets:
 *
 * <pre>{@code
 * ServerSocketTransportFactory transportFactory =
 *     ServerSocketTransportFactory.builder(8080).build();
 *
 * Acceptor acceptor = Acceptor.builder(transportFactory)
 *     .channelFactory(transport -> ServerChannel.builder()
 *         .transport(transport)
 *         .requestReader(reader)
 *         .responseWriter(writer)
 *         .requestHandler(handler)
 *         .build())
 *     .build();
 * acceptor.start();
 * }</pre>
 */
public class Acceptor {
  private static final Logger log = LoggerFactory.getLogger(Acceptor.class);
  private final AtomicBoolean started = new AtomicBoolean();
  final AtomicBoolean closed = new AtomicBoolean();
  private final TransportFactory transportFactory;
  private final Function<Transport, Channel> channelFactory;
  private final long errorBackoffNanos;
  private final Meter meter;
  private final Tracer tracer;
  private final ConcurrentHashMap<String, Channel> channels = new ConcurrentHashMap<>();
  private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

  private Acceptor(Builder builder) {
    this.transportFactory = builder.transportFactory;
    this.channelFactory = builder.channelFactory;
    this.errorBackoffNanos = builder.errorBackoffNanos;
    this.meter = builder.meter;
    this.tracer = builder.tracer;
  }

  public static Builder builder(TransportFactory transportFactory) {
    return new Builder(Objects.requireNonNull(transportFactory, "transportFactory"));
  }

  /** Starts the accept loop on a virtual thread. */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("Acceptor already started");
    }
    VT.start(this::acceptLoop, "bytestreams-acceptor");
    log.info("Acceptor started");
  }

  /**
   * Closes the acceptor, the transport factory (if {@link Closeable}), and all tracked channels.
   *
   * @return a future that completes when all channels are closed
   */
  public CompletableFuture<Void> close() {
    if (closed.compareAndSet(false, true)) {
      log.info("Acceptor closing, active channels={}", channels.size());
      if (transportFactory instanceof Closeable c) {
        Closeables.closeQuietly(c);
      }
      closeAllChannels();
    }
    return closeFuture.copy();
  }

  public CompletableFuture<Void> closeFuture() {
    return closeFuture.copy();
  }

  private void acceptLoop() {
    Span acceptorSpan = tracer.spanBuilder("Acceptor").setSpanKind(SpanKind.SERVER).startSpan();
    LongUpDownCounter activeConnections =
        meter.upDownCounterBuilder("acceptor.connections.active").setUnit("{connection}").build();
    try {
      while (!closed.get()) {
        acceptOne(acceptorSpan, activeConnections);
      }
    } finally {
      if (closed.compareAndSet(false, true) && transportFactory instanceof Closeable c) {
        Closeables.closeQuietly(c);
      }
      closeAllChannels();
      OTel.endSpan(acceptorSpan, null);
      MDC.clear();
    }
  }

  private void acceptOne(Span acceptorSpan, LongUpDownCounter activeConnections) {
    Transport transport = null;
    try {
      transport = transportFactory.create();
      if (closed.get()) {
        Closeables.closeQuietly(transport);
        return;
      }
      Attributes attrs = transport.attributes();
      Channel channel = channelFactory.apply(transport);
      channel.start();
      channels.put(channel.id(), channel);
      activeConnections.add(1, attrs);
      log.info("Connection accepted: channelId={}", channel.id());
      channel
          .closeFuture()
          .whenComplete(
              (v, e) -> {
                channels.remove(channel.id());
                activeConnections.add(-1, attrs);
              });
      if (closed.get()) {
        channel.close();
      }
    } catch (IOException e) {
      if (!closed.get()) {
        log.warn("Accept loop error, retrying after backoff", e);
        acceptorSpan.recordException(e);
        LockSupport.parkNanos(errorBackoffNanos);
      }
    } catch (Exception factoryError) {
      log.warn("Channel factory error", factoryError);
      acceptorSpan.recordException(factoryError);
      if (transport != null) {
        Closeables.closeQuietly(transport);
      }
    }
  }

  private void closeAllChannels() {
    if (closeFuture.isDone()) {
      return;
    }
    CompletableFuture<?>[] futures =
        channels.values().stream().map(Channel::close).toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(futures).whenComplete((v, e) -> closeFuture.complete(null));
  }

  public static final class Builder {
    private final TransportFactory transportFactory;
    private Function<Transport, Channel> channelFactory;
    private long errorBackoffNanos = TimeUnit.MILLISECONDS.toNanos(100);
    private Meter meter;
    private Tracer tracer;

    private Builder(TransportFactory transportFactory) {
      this.transportFactory = transportFactory;
      this.meter = OTel.meter();
      this.tracer = OTel.tracer();
    }

    public Builder channelFactory(Function<Transport, Channel> channelFactory) {
      this.channelFactory = channelFactory;
      return this;
    }

    public Builder errorBackoff(Duration errorBackoff) {
      this.errorBackoffNanos = errorBackoff.toNanos();
      return this;
    }

    public Builder meter(Meter meter) {
      this.meter = Objects.requireNonNull(meter, "meter");
      return this;
    }

    public Builder tracer(Tracer tracer) {
      this.tracer = Objects.requireNonNull(tracer, "tracer");
      return this;
    }

    public Acceptor build() {
      if (channelFactory == null) {
        throw new IllegalStateException("channelFactory is required");
      }
      return new Acceptor(this);
    }
  }
}
