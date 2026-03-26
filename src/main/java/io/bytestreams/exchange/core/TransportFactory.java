package io.bytestreams.exchange.core;

import java.io.IOException;

/**
 * Factory for creating new {@link Transport} instances.
 *
 * <p>Each call to {@link #create()} should return a new, ready-to-use transport. For example, a
 * server-side factory might accept incoming connections via {@code serverSocket.accept()}, while a
 * client-side factory might open a new socket to a remote address.
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
