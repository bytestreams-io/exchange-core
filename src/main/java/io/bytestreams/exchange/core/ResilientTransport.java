package io.bytestreams.exchange.core;

import io.opentelemetry.api.common.Attributes;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
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
 * <p>Use {@link #builder(TransportFactory)} to configure backoff strategy and max attempts.
 */
public final class ResilientTransport implements Transport {

  private static final Logger log = LoggerFactory.getLogger(ResilientTransport.class);

  private final TransportFactory factory;
  private final BackoffStrategy backoffStrategy;
  private final int maxAttempts;
  private final ReentrantLock reconnectLock = new ReentrantLock();
  private final AtomicBoolean stale = new AtomicBoolean(true);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private volatile Transport delegate;
  private volatile Throwable staleCause;
  private volatile InputStream cachedIn;
  private volatile OutputStream cachedOut;

  private ResilientTransport(
      TransportFactory factory, BackoffStrategy backoffStrategy, int maxAttempts) {
    this.factory = factory;
    this.backoffStrategy = backoffStrategy;
    this.maxAttempts = maxAttempts;
  }

  public static Builder builder(TransportFactory factory) {
    return new Builder(Objects.requireNonNull(factory, "factory"));
  }

  @Override
  public InputStream inputStream() throws IOException {
    InputStream in = cachedIn;
    if (in != null && !stale.get()) {
      return in;
    }
    try {
      in = new ReconnectingInputStream(getOrReconnect().inputStream());
    } catch (IOException e) {
      markStale(e);
      in = new ReconnectingInputStream(getOrReconnect().inputStream());
    }
    cachedIn = in;
    return in;
  }

  @Override
  public OutputStream outputStream() throws IOException {
    OutputStream out = cachedOut;
    if (out != null && !stale.get()) {
      return out;
    }
    try {
      out = new ReconnectingOutputStream(getOrReconnect().outputStream());
    } catch (IOException e) {
      markStale(e);
      out = new ReconnectingOutputStream(getOrReconnect().outputStream());
    }
    cachedOut = out;
    return out;
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

  void markStale(Throwable cause) {
    staleCause = cause;
    stale.set(true);
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

  private void checkAborted() throws IOException {
    if (Thread.currentThread().isInterrupted()) {
      throw new IOException("Reconnect aborted: thread interrupted");
    }
  }

  private Transport reconnect() throws IOException {
    reconnectLock.lock();
    try {
      if (!stale.get()) {
        return delegate;
      }

      Throwable lastCause = staleCause;
      staleCause = null;
      boolean initialConnect = delegate == null;
      if (!initialConnect) {
        log.warn("Transport connection lost, attempting reconnect");
        Closeables.closeQuietly(delegate);
      }

      for (int attempt = 1; attempt <= maxAttempts; attempt++) {
        checkAborted();

        log.info("Reconnect attempt {}/{}", attempt, maxAttempts);

        try {
          delegate = factory.create();
          cachedIn = null;
          cachedOut = null;
          stale.set(false);
          log.info("Reconnected successfully on attempt {}", attempt);
          return delegate;
        } catch (IOException e) {
          lastCause = e;
          log.debug("Reconnect attempt {} failed: {}", attempt, e.getMessage());
          if (attempt < maxAttempts) {
            LockSupport.parkNanos(backoffStrategy.delay(attempt).toNanos());
            checkAborted();
          }
        }
      }

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

    private Builder(TransportFactory factory) {
      this.factory = factory;
    }

    public Builder backoffStrategy(BackoffStrategy backoffStrategy) {
      this.backoffStrategy = Objects.requireNonNull(backoffStrategy, "backoffStrategy");
      return this;
    }

    public Builder maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public ResilientTransport build() {
      if (maxAttempts <= 0) {
        throw new IllegalArgumentException("maxAttempts must be positive");
      }
      return new ResilientTransport(factory, backoffStrategy, maxAttempts);
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
