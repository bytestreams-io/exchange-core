package io.bytestreams.exchange.core;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
 * <p>Use {@link #builder(TransportFactory)} to configure backoff strategy and max attempts.
 */
public final class ResilientTransport implements Transport {

  private static final Logger log = LoggerFactory.getLogger(ResilientTransport.class);

  private final TransportFactory factory;
  private final BackoffStrategy backoffStrategy;
  private final int maxAttempts;
  private final LongCounter reconnectTotal;
  private final LongCounter reconnectSuccess;
  private final LongCounter reconnectGaveUp;
  private final ReentrantLock reconnectLock = new ReentrantLock();
  private final AtomicBoolean stale = new AtomicBoolean(true);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicReference<Transport> delegate = new AtomicReference<>();
  private final AtomicReference<Throwable> staleCause = new AtomicReference<>();
  private final AtomicReference<InputStream> cachedIn = new AtomicReference<>();
  private final AtomicReference<OutputStream> cachedOut = new AtomicReference<>();

  private ResilientTransport(
      TransportFactory factory, BackoffStrategy backoffStrategy, int maxAttempts, Meter meter) {
    this.factory = factory;
    this.backoffStrategy = backoffStrategy;
    this.maxAttempts = maxAttempts;
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

  @Override
  public InputStream inputStream() throws IOException {
    InputStream in = cachedIn.get();
    if (in != null && !stale.get()) {
      return in;
    }
    try {
      in = new ReconnectingInputStream(getOrReconnect().inputStream());
    } catch (IOException e) {
      markStale(e);
      in = new ReconnectingInputStream(getOrReconnect().inputStream());
    }
    cachedIn.set(in);
    return in;
  }

  @Override
  public OutputStream outputStream() throws IOException {
    OutputStream out = cachedOut.get();
    if (out != null && !stale.get()) {
      return out;
    }
    try {
      out = new ReconnectingOutputStream(getOrReconnect().outputStream());
    } catch (IOException e) {
      markStale(e);
      out = new ReconnectingOutputStream(getOrReconnect().outputStream());
    }
    cachedOut.set(out);
    return out;
  }

  @Override
  public Attributes attributes() {
    Transport d = delegate.get();
    return d != null ? d.attributes() : Attributes.empty();
  }

  @Override
  public void close() throws IOException {
    reconnectLock.lock();
    try {
      closed.set(true);
      Transport d = delegate.get();
      if (d != null) {
        d.close();
      }
    } finally {
      reconnectLock.unlock();
    }
  }

  void markStale(Throwable cause) {
    staleCause.set(cause);
    stale.set(true);
  }

  private Transport getOrReconnect() throws IOException {
    if (closed.get()) {
      throw new IOException("Transport is closed");
    }
    if (!stale.get()) {
      return delegate.get();
    }
    return reconnect();
  }

  private void checkAborted() throws IOException {
    if (Thread.currentThread().isInterrupted()) {
      throw new IOException("Reconnect aborted: thread interrupted");
    }
  }

  private Transport reconnect() throws IOException {
    reconnectLock.lock();
    try {
      if (!stale.get()) {
        return delegate.get();
      }

      Throwable lastCause = staleCause.getAndSet(null);
      boolean initialConnect = delegate.get() == null;
      if (!initialConnect) {
        log.warn("Transport connection lost, attempting reconnect");
        Closeables.closeQuietly(delegate.get());
      }

      for (int attempt = 1; attempt <= maxAttempts; attempt++) {
        checkAborted();

        reconnectTotal.add(1, Attributes.empty());
        log.info("Reconnect attempt {}/{}", attempt, maxAttempts);

        try {
          delegate.set(factory.create());
          cachedIn.set(null);
          cachedOut.set(null);
          stale.set(false);
          reconnectSuccess.add(1, Attributes.empty());
          log.info("Reconnected successfully on attempt {}", attempt);
          return delegate.get();
        } catch (IOException e) {
          lastCause = e;
          log.debug("Reconnect attempt {} failed: {}", attempt, e.getMessage());
          if (attempt < maxAttempts) {
            LockSupport.parkNanos(backoffStrategy.delay(attempt).toNanos());
            checkAborted();
          }
        }
      }

      reconnectGaveUp.add(1, Attributes.empty());
      log.error("Reconnect gave up after {} attempts", maxAttempts);
      throw new IOException("Failed to reconnect after " + maxAttempts + " attempts", lastCause);
    } finally {
      reconnectLock.unlock();
    }
  }

  public static final class Builder {
    private final TransportFactory factory;
    private BackoffStrategy backoffStrategy =
        BackoffStrategy.exponential(Duration.ofMillis(100))
            .withMax(Duration.ofSeconds(30))
            .withJitter(0.5);
    private int maxAttempts = Integer.MAX_VALUE;
    private Meter meter;

    private Builder(TransportFactory factory) {
      this.factory = factory;
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

    public Builder meter(Meter meter) {
      this.meter = Objects.requireNonNull(meter, "meter");
      return this;
    }

    public ResilientTransport build() {
      if (maxAttempts <= 0) {
        throw new IllegalArgumentException("maxAttempts must be positive");
      }
      return new ResilientTransport(factory, backoffStrategy, maxAttempts, meter);
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
        out.write(b); // avoid FilterOutputStream's per-byte allocation
      } catch (IOException e) {
        markStale(e);
        throw e;
      }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      try {
        out.write(b, off, len); // avoid FilterOutputStream's per-byte loop
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
