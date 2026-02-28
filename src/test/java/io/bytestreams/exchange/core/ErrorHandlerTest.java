package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class ErrorHandlerTest {

  private final ErrorHandler<String, String> errorHandler = new ErrorHandler<>() {};

  @Test
  void stopOnError_returns_true_by_default() {
    ErrorContext<String, String> ctx =
        new ErrorContext<>(new RuntimeException(), Optional.empty(), Optional.empty());
    assertThat(errorHandler.stopOnError(null, ctx)).isTrue();
  }

  @Test
  void custom_implementation_can_return_false() {
    ErrorHandler<String, String> custom =
        new ErrorHandler<>() {
          @Override
          public boolean stopOnError(Channel channel, ErrorContext<String, String> context) {
            return false;
          }
        };
    ErrorContext<String, String> ctx =
        new ErrorContext<>(new RuntimeException(), Optional.empty(), Optional.empty());
    assertThat(custom.stopOnError(null, ctx)).isFalse();
  }
}
