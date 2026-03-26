package io.bytestreams.exchange.core;

import java.io.IOException;

/**
 * Factory for creating new {@link Transport} instances.
 *
 * <p>Used by {@link ReconnectingTransport} to establish fresh connections on each reconnect
 * attempt. Implementations should create a new, ready-to-use transport each time {@link #create()}
 * is called.
 */
@FunctionalInterface
public interface TransportFactory {
  /**
   * Creates a new transport.
   *
   * @return the new transport
   * @throws IOException if the transport cannot be created
   */
  Transport create() throws IOException;
}
