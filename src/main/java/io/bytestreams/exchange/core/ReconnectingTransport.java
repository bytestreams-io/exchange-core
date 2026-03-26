package io.bytestreams.exchange.core;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Transport} decorator that automatically reconnects using a {@link TransportFactory} when
 * the underlying transport fails.
 *
 * <p>Reconnection happens at message boundaries — when {@link #inputStream()} or {@link
 * #outputStream()} is called after a failure has been detected. The returned streams are wrapped to
 * detect I/O failures and set an internal stale flag, which triggers reconnection on the next
 * stream access.
 *
 * <p><b>Stream identity:</b> Unlike the general {@link Transport} contract, this implementation may
 * return different stream instances after a reconnect. This is safe with {@link AbstractChannel},
 * which re-fetches streams on each loop iteration.
 *
 * <p>Use {@link #builder(TransportFactory)} to configure backoff strategy, max attempts, lifecycle
 * listener, and OTel metrics.
 */
public final class ReconnectingTransport implements Transport {

  private static final Logger log = LoggerFactory.getLogger(ReconnectingTransport.class);

  private final TransportFactory factory;
  private final BackoffStrategy backoffStrategy;
  private final int maxAttempts;
  private final ReconnectListener listener;
  private final LongCounter reconnectTotal;
  private final LongCounter reconnectSuccess;
  private final LongCounter reconnectGaveUp;
  private final ReentrantLock reconnectLock = new ReentrantLock();
  private final AtomicBoolean stale = new AtomicBoolean(true);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private volatile Transport delegate;
  private volatile Throwable staleCause;

  private ReconnectingTransport(
      TransportFactory factory,
      BackoffStrategy backoffStrategy,
      int maxAttempts,
      ReconnectListener listener,
      Meter meter) {
    this.factory = factory;
    this.backoffStrategy = backoffStrategy;
    this.maxAttempts = maxAttempts;
    this.listener = listener;
    this.reconnectTotal =
        meter.counterBuilder("transport.reconnect.total").setUnit("{attempt}").build();
    this.reconnectSuccess =
        meter.counterBuilder("transport.reconnect.success").setUnit("{attempt}").build();
    this.reconnectGaveUp =
        meter.counterBuilder("transport.reconnect.gave_up").setUnit("{attempt}").build();
  }

  public static Builder builder(TransportFactory factory) {
    return new Builder(Objects.requireNonNull(factory, "factory"));
  }

  /**
   * Returns a wrapped input stream from the current (or reconnected) delegate. If the delegate's
   * {@code inputStream()} throws, marks the transport stale and retries once via reconnect.
   */
  @Override
  public InputStream inputStream() throws IOException {
    try {
      return new ReconnectingInputStream(getOrReconnect().inputStream());
    } catch (IOException e) {
      markStale(e);
      return new ReconnectingInputStream(getOrReconnect().inputStream());
    }
  }

  /**
   * Returns a wrapped output stream from the current (or reconnected) delegate. If the delegate's
   * {@code outputStream()} throws, marks the transport stale and retries once via reconnect.
   */
  @Override
  public OutputStream outputStream() throws IOException {
    try {
      return new ReconnectingOutputStream(getOrReconnect().outputStream());
    } catch (IOException e) {
      markStale(e);
      return new ReconnectingOutputStream(getOrReconnect().outputStream());
    }
  }

  @Override
  public Attributes attributes() {
    Transport d = delegate;
    return d != null ? d.attributes() : Attributes.empty();
  }

  @Override
  public void close() throws IOException {
    reconnectLock.lock();
    try {
      closed.set(true);
      Transport d = delegate;
      if (d != null) {
        d.close();
      }
    } finally {
      reconnectLock.unlock();
    }
  }

  /**
   * Marks the current transport as stale, triggering reconnection on the next {@link
   * #inputStream()} or {@link #outputStream()} call.
   *
   * @param cause the exception that caused the staleness, passed to {@link
   *     ReconnectListener#onDisconnect(Throwable)}
   */
  void markStale(Throwable cause) {
    // Order matters: cause must be visible before the stale flag is read by reconnect()
    staleCause = cause;
    stale.set(true);
  }

  private void checkAborted() throws IOException {
    if (Thread.currentThread().isInterrupted()) {
      throw new IOException("Reconnect aborted: thread interrupted");
    }
  }

  private Transport getOrReconnect() throws IOException {
    if (closed.get()) {
      throw new IOException("Transport is closed");
    }
    if (!stale.get()) {
      return delegate;
    }
    return reconnect();
  }

  private Transport reconnect() throws IOException {
    reconnectLock.lock();
    try {
      // Double-check: another thread may have already reconnected
      if (!stale.get()) {
        return delegate;
      }

      Throwable lastCause = staleCause;
      staleCause = null;
      boolean initialConnect = delegate == null;
      if (!initialConnect) {
        listener.onDisconnect(lastCause);
        log.warn("Transport connection lost, attempting reconnect");
        Closeables.closeQuietly(delegate);
      }

      for (int attempt = 1; attempt <= maxAttempts; attempt++) {
        checkAborted();

        listener.onReconnecting(attempt);
        reconnectTotal.add(1, Attributes.empty());
        log.info("Reconnect attempt {}/{}", attempt, maxAttempts);

        try {
          delegate = factory.create();
          stale.set(false);
          listener.onReconnected(attempt);
          reconnectSuccess.add(1, Attributes.empty());
          log.info("Reconnected successfully on attempt {}", attempt);
          return delegate;
        } catch (IOException e) {
          lastCause = e;
          log.debug("Reconnect attempt {} failed: {}", attempt, e.getMessage());
          if (attempt < maxAttempts) {
            long delayNanos = backoffStrategy.delayNanos(attempt);
            if (delayNanos > 0) {
              LockSupport.parkNanos(delayNanos);
            }
            checkAborted();
          }
        }
      }

      listener.onGaveUp(maxAttempts, lastCause);
      reconnectGaveUp.add(1, Attributes.empty());
      log.error("Reconnect gave up after {} attempts", maxAttempts);
      throw new IOException("Failed to reconnect after " + maxAttempts + " attempts", lastCause);
    } finally {
      reconnectLock.unlock();
    }
  }

  public static final class Builder {
    private final TransportFactory factory;
    private BackoffStrategy backoffStrategy;
    private int maxAttempts = Integer.MAX_VALUE;
    private ReconnectListener listener = new ReconnectListener() {};
    private Meter meter;

    private Builder(TransportFactory factory) {
      this.factory = factory;
      this.backoffStrategy = ExponentialBackoff.withDefaults();
      this.meter = OTel.meter();
    }

    public Builder backoffStrategy(BackoffStrategy backoffStrategy) {
      this.backoffStrategy = Objects.requireNonNull(backoffStrategy, "backoffStrategy");
      return this;
    }

    public Builder maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public Builder listener(ReconnectListener listener) {
      this.listener = Objects.requireNonNull(listener, "listener");
      return this;
    }

    public Builder meter(Meter meter) {
      this.meter = Objects.requireNonNull(meter, "meter");
      return this;
    }

    public ReconnectingTransport build() {
      if (maxAttempts <= 0) {
        throw new IllegalArgumentException("maxAttempts must be positive");
      }
      return new ReconnectingTransport(factory, backoffStrategy, maxAttempts, listener, meter);
    }
  }

  private final class ReconnectingInputStream extends FilterInputStream {
    ReconnectingInputStream(InputStream in) {
      super(in);
    }

    @Override
    public int read() throws IOException {
      try {
        return super.read();
      } catch (IOException e) {
        markStale(e);
        throw e;
      }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      try {
        return super.read(b, off, len);
      } catch (IOException e) {
        markStale(e);
        throw e;
      }
    }
  }

  private final class ReconnectingOutputStream extends FilterOutputStream {
    ReconnectingOutputStream(OutputStream out) {
      super(out);
    }

    @Override
    public void write(int b) throws IOException {
      try {
        // Bypass FilterOutputStream.write(int) which allocates a new byte[1] per call
        out.write(b);
      } catch (IOException e) {
        markStale(e);
        throw e;
      }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      try {
        // Bypass FilterOutputStream.write(byte[],int,int) which loops over write(int)
        out.write(b, off, len);
      } catch (IOException e) {
        markStale(e);
        throw e;
      }
    }

    @Override
    public void flush() throws IOException {
      try {
        super.flush();
      } catch (IOException e) {
        markStale(e);
        throw e;
      }
    }
  }
}
