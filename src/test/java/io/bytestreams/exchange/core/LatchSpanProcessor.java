package io.bytestreams.exchange.core;

import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A {@link SpanProcessor} that signals a {@link CountDownLatch} when spans with matching names end.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * var processor = LatchSpanProcessor.create("server", "handle");
 * // ... configure tracer with processor ...
 * processor.await("server", 5, TimeUnit.SECONDS);
 * }</pre>
 */
class LatchSpanProcessor implements SpanProcessor {

  private final java.util.concurrent.ConcurrentHashMap<String, CountDownLatch> latches;

  private LatchSpanProcessor(Set<String> spanNames) {
    this.latches = new java.util.concurrent.ConcurrentHashMap<>();
    for (String name : spanNames) {
      latches.put(name, new CountDownLatch(1));
    }
  }

  static LatchSpanProcessor create(String... spanNames) {
    return new LatchSpanProcessor(Set.of(spanNames));
  }

  boolean await(String spanName, long timeout, TimeUnit unit) throws InterruptedException {
    CountDownLatch latch = latches.get(spanName);
    if (latch == null) {
      throw new IllegalArgumentException("Unknown span name: " + spanName);
    }
    return latch.await(timeout, unit);
  }

  boolean awaitAll(long timeout, TimeUnit unit) throws InterruptedException {
    for (CountDownLatch latch : latches.values()) {
      if (!latch.await(timeout, unit)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void onStart(Context parentContext, ReadWriteSpan span) {
    // Only onEnd is needed; isStartRequired() returns false
  }

  @Override
  public boolean isStartRequired() {
    return false;
  }

  @Override
  public void onEnd(ReadableSpan span) {
    CountDownLatch latch = latches.get(span.getName());
    if (latch != null) {
      latch.countDown();
    }
  }

  @Override
  public boolean isEndRequired() {
    return true;
  }
}
