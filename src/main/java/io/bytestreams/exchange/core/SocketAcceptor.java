package io.bytestreams.exchange.core;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
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
 * Server-side TCP connection acceptor.
 *
 * <p>Listens on a {@link ServerSocket} and creates channels for each accepted connection using a
 * user-provided factory. A virtual thread runs the accept loop. Accepted channels are tracked and
 * automatically closed when the acceptor shuts down.
 *
 * <p>Error strategy: if the channel factory throws, the accepted socket is closed, the error is
 * logged (via span event), and the accept loop continues.
 */
public class SocketAcceptor {
  private static final Logger log = LoggerFactory.getLogger(SocketAcceptor.class);
  private final AtomicBoolean started = new AtomicBoolean();
  final AtomicBoolean closed = new AtomicBoolean();
  private final int port;
  private final String host;
  private final int backlog;
  private final Function<Transport, Channel> channelFactory;
  private final long errorBackoffNanos;
  private final Meter meter;
  private final Tracer tracer;
  private final ConcurrentHashMap<String, Channel> channels = new ConcurrentHashMap<>();
  private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
  volatile ServerSocket serverSocket;

  private SocketAcceptor(Builder builder) {
    this.port = builder.port;
    this.host = builder.host;
    this.backlog = builder.backlog;
    this.channelFactory = builder.channelFactory;
    this.errorBackoffNanos = builder.errorBackoffNanos;
    this.meter = builder.meter;
    this.tracer = builder.tracer;
  }

  /**
   * Creates a new builder.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Binds the server socket and starts the accept loop on a virtual thread. */
  public void start() throws IOException {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("Acceptor already started");
    }
    serverSocket = new ServerSocket();
    serverSocket.bind(new InetSocketAddress(host, port), backlog);
    VT.start(this::acceptLoop, "bytestreams-acceptor-" + serverSocket.getLocalPort());
    log.info("Acceptor started on {}:{}", host, serverSocket.getLocalPort());
  }

  /** Returns the local port this acceptor is bound to. Only valid after {@link #start()}. */
  public int port() {
    return serverSocket.getLocalPort();
  }

  /**
   * Closes the acceptor and all tracked channels.
   *
   * @return a future that completes when all channels are closed
   */
  public CompletableFuture<Void> close() {
    if (closed.compareAndSet(false, true)) {
      log.info("Acceptor closing, active channels={}", channels.size());
      Closeables.closeQuietly(serverSocket);
      closeAllChannels();
    }
    return closeFuture.copy();
  }

  /**
   * Returns a read-only view of the close future.
   *
   * @return a copy of the close future
   */
  public CompletableFuture<Void> closeFuture() {
    return closeFuture.copy();
  }

  private void acceptLoop() {
    Span acceptorSpan =
        tracer
            .spanBuilder("SocketAcceptor")
            .setSpanKind(SpanKind.SERVER)
            .setAttribute(OTel.SERVER_ADDRESS, host)
            .setAttribute(OTel.SERVER_PORT, (long) serverSocket.getLocalPort())
            .startSpan();
    LongUpDownCounter activeConnections =
        meter.upDownCounterBuilder("acceptor.connections.active").setUnit("{connection}").build();
    Attributes attrs =
        Attributes.builder()
            .put(OTel.SERVER_ADDRESS, host)
            .put(OTel.SERVER_PORT, (long) serverSocket.getLocalPort())
            .build();
    MDC.put("acceptor.port", String.valueOf(serverSocket.getLocalPort()));
    try {
      while (!closed.get()) {
        try {
          Socket socket = serverSocket.accept();
          if (closed.get()) {
            Closeables.closeQuietly(socket);
            break;
          }
          Transport transport = new SocketTransport(socket);
          try {
            Channel channel = channelFactory.apply(transport);
            channel.start();
            channels.put(channel.id(), channel);
            activeConnections.add(1, attrs);
            log.info(
                "Connection accepted: peer={}:{}, channelId={}",
                socket.getInetAddress().getHostAddress(),
                socket.getPort(),
                channel.id());
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
          } catch (Exception factoryError) {
            log.warn(
                "Channel factory error for peer {}:{}",
                socket.getInetAddress().getHostAddress(),
                socket.getPort(),
                factoryError);
            acceptorSpan.recordException(factoryError);
            Closeables.closeQuietly(transport);
          }
        } catch (IOException e) {
          if (closed.get()) {
            break;
          }
          log.warn("Accept loop error, retrying after backoff", e);
          acceptorSpan.recordException(e);
          LockSupport.parkNanos(errorBackoffNanos);
        }
      }
    } finally {
      if (closed.compareAndSet(false, true)) {
        Closeables.closeQuietly(serverSocket);
      }
      closeAllChannels();
      OTel.endSpan(acceptorSpan, null);
      MDC.clear();
    }
  }

  private void closeAllChannels() {
    CompletableFuture<?>[] futures =
        channels.values().stream().map(Channel::close).toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(futures).whenComplete((v, e) -> closeFuture.complete(null));
  }

  /** Builder for {@link SocketAcceptor}. */
  public static final class Builder {
    private int port;
    private String host = "0.0.0.0";
    private int backlog = 50;
    private Function<Transport, Channel> channelFactory;
    private long errorBackoffNanos = TimeUnit.MILLISECONDS.toNanos(100);
    private Meter meter;
    private Tracer tracer;

    private Builder() {
      this.meter = OTel.meter();
      this.tracer = OTel.tracer();
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder backlog(int backlog) {
      this.backlog = backlog;
      return this;
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

    /**
     * Builds the acceptor.
     *
     * @return a new {@link SocketAcceptor}
     * @throws IllegalStateException if channelFactory is not set
     */
    public SocketAcceptor build() {
      if (channelFactory == null) {
        throw new IllegalStateException("channelFactory is required");
      }
      return new SocketAcceptor(this);
    }
  }
}
