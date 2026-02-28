package io.bytestreams.exchange.core;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Writes a message to an output stream.
 *
 * @param <T> the message type
 */
@FunctionalInterface
public interface MessageWriter<T> {
  void write(T message, OutputStream output) throws IOException;
}
