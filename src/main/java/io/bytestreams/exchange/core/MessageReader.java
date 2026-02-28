package io.bytestreams.exchange.core;

import java.io.IOException;
import java.io.InputStream;

/**
 * Reads a message from an input stream.
 *
 * @param <T> the message type
 */
@FunctionalInterface
public interface MessageReader<T> {
  T read(InputStream input) throws IOException;
}
