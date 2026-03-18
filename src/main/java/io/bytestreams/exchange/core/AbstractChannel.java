package io.bytestreams.exchange.core;

import static io.bytestreams.exchange.core.ChannelStatus.CLOSED;
import static io.bytestreams.exchange.core.ChannelStatus.CLOSING;
import static io.bytestreams.exchange.core.ChannelStatus.INIT;
import static io.bytestreams.exchange.core.ChannelStatus.READY;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Base class for {@link Transport}-backed channels with managed I/O loops.
 *
 * <p>Provides write queue, read/write virtual threads, OTel channel span and metrics, graceful
 * close semantics, and error handling. Subclasses implement {@link #onInbound}, {@link
 * #hasPending}, {@link #stopOnInboundError}, and {@link #stopOnOutboundError}.
 *
 * <p>Lifecycle: construct → {@link #start()} → {@link #close()}. The channel is not usable until
 * {@link #start()} is called, which starts the reader and writer virtual threads.
 *
 * @param <OUT> the outbound (written) message type
 * @param <IN> the inbound (read) message type
 */
public abstract class AbstractChannel<OUT, IN> implements Channel {
  public static final long DEFAULT_ERROR_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
  protected static final Set<ChannelStatus> SHUTTING_DOWN =
      Collections.unmodifiableSet(EnumSet.of(CLOSING, CLOSED));
  private static final Logger log = LoggerFactory.getLogger(AbstractChannel.class);
  protected final LinkedBlockingQueue<OUT> writeQueue;
  protected final LongUpDownCounter writeQueueSize;
  protected final Transport transport;
  protected final CompletableFuture<Void> readFuture = new CompletableFuture<>();
  protected final LongUpDownCounter requestActive;
  protected final LongCounter requestTotal;
  protected final LongCounter requestErrors;
  protected final DoubleHistogram requestDuration;
  protected final Span channelSpan;
  protected final AtomicReference<ChannelStatus> status = new AtomicReference<>(INIT);
  protected final CompletableFuture<Void> closeFuture;
  protected final Duration defaultTimeout;
  private final CompletableFuture<Void> writeFuture = new CompletableFuture<>();
  private final long errorBackoffNanos;
  private final MessageWriter<OUT> writer;
  private final MessageReader<IN> reader;
  private final String id;
  private final String channelType;
  private final Thread readerThread;
  private final Thread writerThread;
  protected final Attributes meterAttributes;

  AbstractChannel(
      Transport transport,
      int writeBufferSize,
      String channelType,
      SpanKind spanKind,
      long errorBackoffNanos,
      Meter meter,
      Tracer tracer,
      MessageWriter<OUT> writer,
      MessageReader<IN> reader,
      Duration defaultTimeout,
      Attributes extraMeterAttributes) {
    this(
        UUID.randomUUID().toString(),
        transport,
        writeBufferSize,
        channelType,
        spanKind,
        errorBackoffNanos,
        meter,
        tracer,
        writer,
        reader,
        defaultTimeout,
        extraMeterAttributes);
  }

  AbstractChannel(
      String id,
      Transport transport,
      int writeBufferSize,
      String channelType,
      SpanKind spanKind,
      long errorBackoffNanos,
      Meter meter,
      Tracer tracer,
      MessageWriter<OUT> writer,
      MessageReader<IN> reader,
      Duration defaultTimeout,
      Attributes extraMeterAttributes) {
    this.id = id;
    this.channelType = channelType;
    this.transport = transport;
    this.writeQueue = new LinkedBlockingQueue<>(writeBufferSize);
    this.defaultTimeout = defaultTimeout;
    this.writeQueueSize =
        meter.upDownCounterBuilder("write_queue.size").setUnit("{message}").build();
    this.errorBackoffNanos = errorBackoffNanos;
    Attributes transportAttrs = transport.attributes();
    this.meterAttributes =
        Attributes.builder()
            .put(OTel.CHANNEL_TYPE, channelType)
            .put(OTel.NETWORK_TRANSPORT, transportAttrs.get(OTel.NETWORK_TRANSPORT))
            .put(OTel.NETWORK_TYPE, transportAttrs.get(OTel.NETWORK_TYPE))
            .putAll(extraMeterAttributes)
            .build();
    this.channelSpan =
        tracer
            .spanBuilder(channelType)
            .setSpanKind(spanKind)
            .setAllAttributes(transportAttrs)
            .setAttribute(OTel.CHANNEL_TYPE, channelType)
            .setAttribute(OTel.CHANNEL_ID, id())
            .startSpan();
    this.closeFuture =
        CompletableFuture.allOf(writeFuture, readFuture).whenComplete((v, e) -> cleanUp(e));
    this.requestActive =
        meter.upDownCounterBuilder("request.active").setUnit(OTel.UNIT_REQUEST).build();
    this.requestTotal = meter.counterBuilder("request.total").setUnit(OTel.UNIT_REQUEST).build();
    this.requestErrors = meter.counterBuilder("request.errors").setUnit(OTel.UNIT_REQUEST).build();
    this.requestDuration = meter.histogramBuilder("request.duration").setUnit("ms").build();
    this.writer = writer;
    this.reader = reader;
    this.readerThread = VT.of(this::readLoop, channelType + "-reader-" + id());
    this.writerThread = VT.of(this::writeLoop, channelType + "-writer-" + id());
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public ChannelStatus status() {
    return status.get();
  }

  @Override
  public void start() {
    if (!status.compareAndSet(INIT, READY)) {
      throw new IllegalStateException("Channel already started");
    }
    readerThread.start();
    writerThread.start();
    log.info("Channel opened: type={}, id={}", channelType, id);
  }

  /**
   * Returns a defensive copy of the close future. Each call returns a new instance; all copies
   * complete when the channel finishes shutting down.
   */
  @Override
  public CompletableFuture<Void> closeFuture() {
    return closeFuture.copy();
  }

  @Override
  public CompletableFuture<Void> close() {
    if (status.compareAndSet(READY, CLOSING)) {
      log.debug("Channel closing: type={}, id={}, hasPending={}", channelType, id, hasPending());
    } else if (status.compareAndSet(INIT, CLOSED)) {
      log.debug("Channel closed without starting: type={}, id={}", channelType, id);
      readFuture.complete(null);
      writeFuture.complete(null);
      return closeFuture();
    }
    interruptIfDrained();
    return closeFuture();
  }

  /** Interrupts the reader thread if the channel is shutting down and no work remains. */
  protected void interruptIfDrained() {
    if (SHUTTING_DOWN.contains(status()) && !hasPending()) {
      readerThread.interrupt();
    }
  }

  /** Brief pause after a handled error to prevent busy-spinning on persistent failures. */
  void errorBackoff() {
    LockSupport.parkNanos(errorBackoffNanos);
  }

  private void cleanUp(Throwable error) {
    status.set(CLOSED);
    writeQueueSize.add(-writeQueue.size(), meterAttributes);
    writeQueue.clear();
    OTel.endSpan(channelSpan, error);
    log.info("Channel closed: type={}, id={}", channelType, id);
  }

  private void readLoop() {
    MDC.put("channel.id", id);
    MDC.put("channel.type", channelType);
    try {
      while (!SHUTTING_DOWN.contains(status()) || hasPending()) {
        IN inbound = null;
        try {
          inbound = reader.read(transport.inputStream());
          onInbound(inbound);
        } catch (Exception e) {
          if (SHUTTING_DOWN.contains(status()) && !hasPending()) {
            break;
          }
          if (stopOnInboundError(e, inbound)) {
            log.warn("Read loop stopped by error handler", e);
            Closeables.closeQuietly(transport);
            readFuture.completeExceptionally(e);
            return;
          } else {
            log.debug("Read loop error, continuing after backoff", e);
            errorBackoff();
          }
        }
      }
      readFuture.complete(null);
    } catch (Throwable t) {
      log.error("Read loop terminated unexpectedly", t);
      readFuture.completeExceptionally(t);
      Closeables.closeQuietly(transport);
    } finally {
      MDC.clear();
    }
  }

  /**
   * Returns metric attributes for a message, including {@code message_type} set to the simple class
   * name of the given message. Subclasses use this to build request-scoped metric attributes.
   */
  protected Attributes buildMessageAttributes(Object message) {
    return Attributes.builder()
        .putAll(meterAttributes)
        .put(OTel.MESSAGE_TYPE, message.getClass().getSimpleName())
        .build();
  }

  protected abstract boolean hasPending();

  protected abstract void onInbound(IN inbound);

  private void writeLoop() {
    MDC.put("channel.id", id);
    MDC.put("channel.type", channelType);
    try {
      while (!SHUTTING_DOWN.contains(status()) || !writeQueue.isEmpty()) {
        OUT outbound = null;
        try {
          outbound = writeQueue.poll(100, TimeUnit.MILLISECONDS);
          if (outbound == null) {
            continue;
          }
          writeQueueSize.add(-1, meterAttributes);
          OutputStream outputStream = transport.outputStream();
          writer.write(outbound, outputStream);
          outputStream.flush();
          interruptIfDrained();
        } catch (IOException | InterruptedException e) {
          if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
          if (stopOnOutboundError(e, outbound)) {
            log.warn("Write loop stopped by error handler", e);
            Closeables.closeQuietly(transport);
            writeFuture.completeExceptionally(e);
            return;
          } else {
            log.debug("Write loop error, continuing after backoff", e);
            errorBackoff();
          }
        }
      }
      writeFuture.complete(null);
    } catch (Throwable t) {
      log.error("Write loop terminated unexpectedly", t);
      writeFuture.completeExceptionally(t);
      Closeables.closeQuietly(transport);
    } finally {
      MDC.clear();
    }
  }

  abstract boolean stopOnOutboundError(Exception error, OUT message);

  abstract boolean stopOnInboundError(Exception error, IN message);
}
