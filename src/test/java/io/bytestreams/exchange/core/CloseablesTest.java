package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class CloseablesTest {

  @Test
  void closeQuietly_closes_closeable() {
    AtomicBoolean closed = new AtomicBoolean();
    assertThatCode(() -> Closeables.closeQuietly(() -> closed.set(true)))
        .doesNotThrowAnyException();
    assertThat(closed).isTrue();
  }

  @Test
  void closeQuietly_swallows_IOException() {
    assertThatCode(
            () ->
                Closeables.closeQuietly(
                    () -> {
                      throw new IOException("boom");
                    }))
        .doesNotThrowAnyException();
  }

  @Test
  void closeQuietly_handles_null() {
    assertThatCode(() -> Closeables.closeQuietly(null)).doesNotThrowAnyException();
  }
}
