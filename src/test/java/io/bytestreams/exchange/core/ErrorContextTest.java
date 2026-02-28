package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class ErrorContextTest {

  @Test
  void accessors_return_correct_values() {
    Throwable cause = new RuntimeException("error");
    Optional<String> request = Optional.of("req");
    Optional<String> response = Optional.of("resp");

    ErrorContext<String, String> ctx = new ErrorContext<>(cause, request, response);

    assertThat(ctx.cause()).isSameAs(cause);
    assertThat(ctx.request()).isSameAs(request);
    assertThat(ctx.response()).isSameAs(response);
  }

  @Test
  void null_cause_throws_npe() {
    assertThatNullPointerException()
        .isThrownBy(() -> new ErrorContext<>(null, Optional.empty(), Optional.empty()))
        .withMessage("cause");
  }

  @Test
  void null_request_optional_throws_npe() {
    assertThatNullPointerException()
        .isThrownBy(() -> new ErrorContext<>(new RuntimeException(), null, Optional.empty()))
        .withMessage("request");
  }

  @Test
  void null_response_optional_throws_npe() {
    assertThatNullPointerException()
        .isThrownBy(() -> new ErrorContext<>(new RuntimeException(), Optional.empty(), null))
        .withMessage("response");
  }

  @Test
  void populated_request_and_response() {
    Throwable cause = new IllegalStateException("bad");
    ErrorContext<Integer, String> ctx =
        new ErrorContext<>(cause, Optional.of(42), Optional.of("response"));

    assertThat(ctx.request()).contains(42);
    assertThat(ctx.response()).contains("response");
  }
}
