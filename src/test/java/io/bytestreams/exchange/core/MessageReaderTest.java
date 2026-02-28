package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class MessageReaderTest {
  @Test
  void readsFromInputStream() throws IOException {
    MessageReader<String> reader = in -> new String(in.readAllBytes());
    ByteArrayInputStream in = new ByteArrayInputStream("hello".getBytes());
    assertThat(reader.read(in)).isEqualTo("hello");
  }
}
