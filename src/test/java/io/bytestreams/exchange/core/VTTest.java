package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class VTTest {

  @Test
  void of_creates_unstarted_virtual_thread_with_given_name() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    Thread thread = VT.of(latch::countDown, "test-vt");

    assertThat(thread.isVirtual()).isTrue();
    assertThat(thread.getName()).isEqualTo("test-vt");
    assertThat(thread.getState()).isEqualTo(Thread.State.NEW);
    thread.start();
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  void start_creates_and_starts_virtual_thread_with_given_name() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    Thread thread = VT.start(latch::countDown, "test-vt-start");

    assertThat(thread.isVirtual()).isTrue();
    assertThat(thread.getName()).isEqualTo("test-vt-start");
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
  }
}
