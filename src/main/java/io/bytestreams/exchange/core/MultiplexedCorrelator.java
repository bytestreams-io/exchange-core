package io.bytestreams.exchange.core;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

final class MultiplexedCorrelator<REQ, RESP> {

  private final Function<REQ, String> requestIdExtractor;
  private final Function<RESP, String> responseIdExtractor;
  private final ConcurrentHashMap<String, CompletableFuture<RESP>> pending;
  private final Semaphore semaphore;
  private final AtomicBoolean closing = new AtomicBoolean();

  MultiplexedCorrelator(
      Function<REQ, String> requestIdExtractor,
      Function<RESP, String> responseIdExtractor,
      int maxConcurrency) {
    this.requestIdExtractor = requestIdExtractor;
    this.responseIdExtractor = responseIdExtractor;
    this.pending = new ConcurrentHashMap<>();
    this.semaphore = new Semaphore(maxConcurrency);
  }

  RegistrationResult<RESP> register(REQ request) {
    String messageId = requestIdExtractor.apply(request);
    if (closing.get()) {
      CompletableFuture<RESP> future = CompletableFuture.failedFuture(new CancellationException());
      return new RegistrationResult<>(future, messageId);
    }
    try {
      semaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      CompletableFuture<RESP> future = CompletableFuture.failedFuture(e);
      return new RegistrationResult<>(future, messageId);
    }
    CompletableFuture<RESP> future = new CompletableFuture<>();
    if (pending.putIfAbsent(messageId, future) != null) {
      semaphore.release();
      throw new DuplicateCorrelationIdException(messageId);
    }
    future.whenComplete(
        (result, error) -> {
          pending.remove(messageId, future);
          semaphore.release();
        });
    return new RegistrationResult<>(future, messageId);
  }

  CorrelationResult correlate(RESP response) {
    String messageId = responseIdExtractor.apply(response);
    CompletableFuture<RESP> future = pending.remove(messageId);
    if (future == null) {
      return new CorrelationResult(false, messageId);
    } else {
      return new CorrelationResult(future.complete(response), messageId);
    }
  }

  boolean hasPending() {
    return !pending.isEmpty();
  }

  void onClose(Throwable cause) {
    if (closing.compareAndSet(false, true)) {
      Throwable throwable =
          Optional.ofNullable(cause).orElse(new CancellationException("Channel closed"));
      pending.forEach((key, future) -> future.completeExceptionally(throwable));
      pending.clear();
    }
  }

  record RegistrationResult<RESP>(CompletableFuture<RESP> future, String messageId) {}

  record CorrelationResult(boolean success, String messageId) {}
}
