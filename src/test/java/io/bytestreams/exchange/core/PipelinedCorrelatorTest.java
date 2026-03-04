package io.bytestreams.exchange.core;

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link PipelinedCorrelator}: positional (FIFO) matching, strict ordering. */
class PipelinedCorrelatorTest {

  private PipelinedCorrelator<String> correlator;

  @BeforeEach
  void setUp() {
    correlator = new PipelinedCorrelator<>(Integer.MAX_VALUE);
  }

  // -- register --

  @Test
  void register_returns_pending_future() {
    CompletableFuture<String> future = correlator.register();
    assertThat(future).isNotDone();
  }

  @Test
  void register_after_close_returns_failed_future() {
    correlator.onClose(null);
    CompletableFuture<String> future = correlator.register();
    assertThat(future).isCompletedExceptionally();
  }

  // -- correlate --

  @Test
  void correlate_completes_head_of_queue() {
    CompletableFuture<String> future = correlator.register();
    assertThat(correlator.correlate("response")).isTrue();
    assertThat(future).isCompletedWithValue("response");
  }

  @Test
  void correlate_multiple_in_order() {
    CompletableFuture<String> a = correlator.register();
    CompletableFuture<String> b = correlator.register();
    correlator.correlate("X");
    correlator.correlate("Y");

    assertThat(a).isCompletedWithValue("X");
    assertThat(b).isCompletedWithValue("Y");
  }

  @Test
  void correlate_returns_false_when_empty() {
    assertThat(correlator.correlate("response")).isFalse();
  }

  @Test
  void correlate_out_of_order_detection() {
    CompletableFuture<String> a = correlator.register();
    CompletableFuture<String> b = correlator.register();
    // Externally complete 'a' before correlate drains it
    a.complete("a");
    // correlate will try to complete 'a' but it's already done -> triggers onClose cascading
    assertThat(correlator.correlate("X")).isFalse();
    // 'b' should be failed due to cascade
    assertThat(b).isCompletedExceptionally();
  }

  // -- attachCascade / external error --

  @Test
  void external_error_cascades_to_all_pending() {
    CompletableFuture<String> a = correlator.register();
    CompletableFuture<String> b = correlator.register();
    CompletableFuture<String> c = correlator.register();
    a.completeExceptionally(new TimeoutException());
    assertThat(b)
        .isCompletedExceptionally()
        .failsWithin(java.time.Duration.ZERO)
        .withThrowableThat()
        .withCauseInstanceOf(TimeoutException.class);
    assertThat(c)
        .isCompletedExceptionally()
        .failsWithin(java.time.Duration.ZERO)
        .withThrowableThat()
        .withCauseInstanceOf(TimeoutException.class);
  }

  // -- onClose --

  @Test
  void onClose_fails_with_cancellation() {
    CompletableFuture<String> a = correlator.register();
    CompletableFuture<String> b = correlator.register();

    correlator.onClose(null);

    assertThat(a)
        .isCompletedExceptionally()
        .failsWithin(java.time.Duration.ZERO)
        .withThrowableThat()
        .isInstanceOf(CancellationException.class)
        .withMessage("Channel closed");
    assertThat(b)
        .isCompletedExceptionally()
        .failsWithin(java.time.Duration.ZERO)
        .withThrowableThat()
        .isInstanceOf(CancellationException.class)
        .withMessage("Channel closed");
  }

  @Test
  void onClose_fails_with_cause() {
    CompletableFuture<String> a = correlator.register();
    CompletableFuture<String> b = correlator.register();

    IOException cause = new IOException();
    correlator.onClose(cause);

    assertThat(a)
        .isCompletedExceptionally()
        .failsWithin(java.time.Duration.ZERO)
        .withThrowableThat()
        .havingCause()
        .isSameAs(cause);
    assertThat(b)
        .isCompletedExceptionally()
        .failsWithin(java.time.Duration.ZERO)
        .withThrowableThat()
        .havingCause()
        .isSameAs(cause);
  }

  @Test
  void onClose_with_no_pending_does_not_throw() {
    correlator.onClose(null);
  }

  @Test
  void onClose_twice_does_not_throw() {
    correlator.register();
    correlator.onClose(null);
    correlator.onClose(null);
  }

  // -- Backpressure --

  @Test
  void register_blocks_when_at_capacity() {
    PipelinedCorrelator<String> bounded = new PipelinedCorrelator<>(1);
    bounded.register();
    CompletableFuture<CompletableFuture<String>> registered = new CompletableFuture<>();
    Thread thread = new Thread(() -> registered.complete(bounded.register()));
    thread.start();
    await().during(Duration.ofMillis(200)).untilAsserted(() -> assertThat(registered).isNotDone());
    bounded.correlate("unblock first");
    await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(registered).isDone());
  }

  @Test
  void register_handles_interrupt() throws Exception {
    PipelinedCorrelator<String> bounded = new PipelinedCorrelator<>(1);
    bounded.register();
    CompletableFuture<CompletableFuture<String>> registered = new CompletableFuture<>();
    Thread thread = new Thread(() -> registered.complete(bounded.register()));
    thread.start();
    await().during(Duration.ofMillis(200)).untilAsserted(() -> assertThat(registered).isNotDone());
    thread.interrupt();
    thread.join();
    assertThat(registered.join())
        .failsWithin(java.time.Duration.ZERO)
        .withThrowableThat()
        .withCauseInstanceOf(InterruptedException.class);
  }

  // -- hasPending --

  @Test
  void hasPending_false_when_empty() {
    assertThat(correlator.hasPending()).isFalse();
  }

  @Test
  void hasPending_true_after_register() {
    correlator.register();
    assertThat(correlator.hasPending()).isTrue();
  }

  @Test
  void hasPending_false_after_correlate() {
    correlator.register();
    correlator.correlate("response");
    assertThat(correlator.hasPending()).isFalse();
  }
}
