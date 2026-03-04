package io.bytestreams.exchange.core;

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link MultiplexedCorrelator}: ID-based matching, out-of-order responses. */
class MultiplexedCorrelatorTest {

  private MultiplexedCorrelator<String, String> correlator;

  @BeforeEach
  void setUp() {
    correlator = new MultiplexedCorrelator<>(msg -> msg, msg -> msg, Integer.MAX_VALUE);
  }

  // -- register --

  @Test
  void register_returns_pending_future() {
    CompletableFuture<String> future = correlator.register("req-1").future();
    assertThat(future).isNotDone();
  }

  @Test
  void register_returns_message_id() {
    MultiplexedCorrelator.RegistrationResult<String> result = correlator.register("req-1");
    assertThat(result.messageId()).isEqualTo("req-1");
  }

  @Test
  void register_duplicate_throws() {
    correlator.register("dup");
    assertThatThrownBy(() -> correlator.register("dup"))
        .isInstanceOf(DuplicateCorrelationIdException.class);
  }

  @Test
  void register_after_correlate_with_same_key_succeeds() {
    correlator.register("key");
    correlator.correlate("key");

    CompletableFuture<String> second = correlator.register("key").future();
    assertThat(second).isNotDone();
  }

  @Test
  void register_after_timeout_with_same_key_succeeds() {
    CompletableFuture<String> first = correlator.register("key").future();
    first.completeExceptionally(new TimeoutException());

    CompletableFuture<String> second = correlator.register("key").future();
    assertThat(second).isNotDone();
  }

  @Test
  void register_after_close_returns_failed_future() {
    correlator.onClose(null);
    CompletableFuture<String> future = correlator.register("req-1").future();
    assertThat(future).isCompletedExceptionally();
  }

  // -- correlate --

  @Test
  void correlate_completes_matching_future() {
    CompletableFuture<String> future = correlator.register("req-1").future();
    assertThat(correlator.correlate("req-1").success()).isTrue();
    assertThat(future).isCompletedWithValue("req-1");
  }

  @Test
  void correlate_returns_false_for_unknown_key() {
    assertThat(correlator.correlate("unknown").success()).isFalse();
  }

  @Test
  void correlate_returns_message_id() {
    correlator.register("req-1");
    MultiplexedCorrelator.CorrelationResult result = correlator.correlate("req-1");
    assertThat(result.messageId()).isEqualTo("req-1");
  }

  @Test
  void correlate_out_of_order() {
    CompletableFuture<String> a = correlator.register("A").future();
    CompletableFuture<String> b = correlator.register("B").future();

    assertThat(correlator.correlate("B").success()).isTrue();
    assertThat(b).isCompletedWithValue("B");
    assertThat(a).isNotDone();

    assertThat(correlator.correlate("A").success()).isTrue();
    assertThat(a).isCompletedWithValue("A");
  }

  @Test
  void external_error_fails_only_that_request() {
    CompletableFuture<String> a = correlator.register("A").future();
    CompletableFuture<String> b = correlator.register("B").future();
    a.completeExceptionally(new TimeoutException());
    assertThat(b).isNotDone();
  }

  // -- onClose --

  @Test
  void onClose_fails_with_cancellation() {
    CompletableFuture<String> a = correlator.register("A").future();
    CompletableFuture<String> b = correlator.register("B").future();

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
    CompletableFuture<String> a = correlator.register("A").future();
    CompletableFuture<String> b = correlator.register("B").future();

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
    correlator.register("A");
    correlator.onClose(null);
    correlator.onClose(null);
  }

  // -- Backpressure --

  @Test
  void register_blocks_when_at_capacity() {
    MultiplexedCorrelator<String, String> bounded =
        new MultiplexedCorrelator<>(msg -> msg, msg -> msg, 1);
    bounded.register("fill");
    CompletableFuture<CompletableFuture<String>> registered = new CompletableFuture<>();
    Thread thread = new Thread(() -> registered.complete(bounded.register("blocked").future()));
    thread.start();
    await().during(Duration.ofMillis(200)).untilAsserted(() -> assertThat(registered).isNotDone());
    bounded.correlate("fill");
    await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(registered).isDone());
  }

  @Test
  void register_handles_interrupt() throws InterruptedException {
    MultiplexedCorrelator<String, String> bounded =
        new MultiplexedCorrelator<>(msg -> msg, msg -> msg, 1);
    bounded.register("first");
    CompletableFuture<CompletableFuture<String>> registered = new CompletableFuture<>();
    Thread thread = new Thread(() -> registered.complete(bounded.register("blocked").future()));
    thread.start();
    await().during(Duration.ofMillis(200)).untilAsserted(() -> assertThat(registered).isNotDone());
    thread.interrupt();
    thread.join();
    assertThat(registered.join())
        .failsWithin(java.time.Duration.ZERO)
        .withThrowableThat()
        .withCauseInstanceOf(InterruptedException.class);
  }

  @Test
  void self_cleaning_releases_permit() {
    MultiplexedCorrelator<String, String> bounded =
        new MultiplexedCorrelator<>(msg -> msg, msg -> msg, 1);
    CompletableFuture<String> first = bounded.register("first").future();

    first.complete("external");

    CompletableFuture<String> second = bounded.register("second").future();
    assertThat(second).isNotDone();
  }

  // -- hasPending --

  @Test
  void hasPending_false_when_empty() {
    assertThat(correlator.hasPending()).isFalse();
  }

  @Test
  void hasPending_true_after_register() {
    correlator.register("req-1");
    assertThat(correlator.hasPending()).isTrue();
  }

  @Test
  void hasPending_false_after_correlate() {
    correlator.register("req-1");
    correlator.correlate("req-1");
    assertThat(correlator.hasPending()).isFalse();
  }
}
