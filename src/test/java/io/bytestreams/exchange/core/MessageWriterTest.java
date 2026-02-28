package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class MessageWriterTest {
  @Test
  void writesToOutputStream() throws IOException {
    MessageWriter<String> writer = (msg, out) -> out.write(msg.getBytes());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.write("hello", out);
    assertThat(out.toByteArray()).isEqualTo("hello".getBytes());
  }
}
