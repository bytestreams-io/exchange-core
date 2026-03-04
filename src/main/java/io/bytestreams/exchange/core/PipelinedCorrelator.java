package io.bytestreams.exchange.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

final class PipelinedCorrelator<RESP> {

  private final LinkedBlockingQueue<CompletableFuture<RESP>> queue;
  private final AtomicBoolean closing = new AtomicBoolean();

  PipelinedCorrelator(int maxConcurrency) {
    this.queue = new LinkedBlockingQueue<>(maxConcurrency);
  }

  CompletableFuture<RESP> register() {
    if (closing.get()) {
      return CompletableFuture.failedFuture(new CancellationException());
    }
    CompletableFuture<RESP> future = new CompletableFuture<>();
    attachCascade(future);
    try {
      queue.put(future);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      future.completeExceptionally(e);
      return future;
    }
    return future;
  }

  private void attachCascade(CompletableFuture<RESP> future) {
    future.whenComplete(
        (result, error) -> {
          if (error != null) {
            List<CompletableFuture<RESP>> drained = new ArrayList<>();
            queue.drainTo(drained);
            for (CompletableFuture<RESP> f : drained) {
              f.completeExceptionally(error);
            }
            queue.clear();
          }
        });
  }

  boolean correlate(RESP response) {
    CompletableFuture<RESP> future = queue.poll();
    if (future == null) {
      return false;
    } else {
      boolean firstComplete = future.complete(response);
      if (!firstComplete) {
        onClose(new CompletedOutOfOrder());
      }
      return firstComplete;
    }
  }

  boolean hasPending() {
    return !queue.isEmpty();
  }

  void onClose(Throwable cause) {
    if (closing.compareAndSet(false, true)) {
      Throwable throwable =
          Optional.ofNullable(cause).orElse(new CancellationException("Channel closed"));
      List<CompletableFuture<RESP>> drained = new ArrayList<>();
      queue.drainTo(drained);
      for (CompletableFuture<RESP> future : drained) {
        future.completeExceptionally(throwable);
      }
      queue.clear();
    }
  }

  static class CompletedOutOfOrder extends RuntimeException {
    CompletedOutOfOrder() {
      super("Correlation completed out of order");
    }
  }
}
