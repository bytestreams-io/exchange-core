package io.bytestreams.exchange.core;

import io.opentelemetry.api.common.Attributes;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Pluggable I/O transport that provides input/output streams and connection metadata.
 *
 * <p>Implementations must be thread-safe: the input stream is read by the channel's reader thread
 * while the output stream is written by the writer thread concurrently. Both methods may be called
 * multiple times and should return the same underlying stream.
 */
public interface Transport extends Closeable {
  /** Returns the input stream for reading inbound messages. */
  InputStream inputStream() throws IOException;

  /** Returns the output stream for writing outbound messages. */
  OutputStream outputStream() throws IOException;

  /** Returns transport-level OTel attributes (e.g. network type, peer address, peer port). */
  Attributes attributes();

  @Override
  void close() throws IOException;
}
