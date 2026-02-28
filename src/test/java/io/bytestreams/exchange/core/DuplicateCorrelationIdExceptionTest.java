package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DuplicateCorrelationIdExceptionTest {

  @Test
  void message_contains_id() {
    var exception = new DuplicateCorrelationIdException("my-id");
    assertThat(exception).hasMessage("Duplicate correlation id: my-id");
  }

  @Test
  void getMessageId_returns_id() {
    var exception = new DuplicateCorrelationIdException("my-id");
    assertThat(exception.getMessageId()).isEqualTo("my-id");
  }
}
