package io.bytestreams.exchange.core;

import java.io.IOException;
import java.net.Socket;

/**
 * Configures a {@link Socket} before it is connected.
 *
 * <p>Use this to set socket options such as timeouts, keep-alive, TCP no-delay, and buffer sizes.
 * Options that must be set before connecting (e.g. {@code SO_RCVBUF}) are safely applied here.
 */
@FunctionalInterface
public interface SocketConfigurator {

  /** A no-op configurator that leaves the socket with default settings. */
  SocketConfigurator DEFAULT = socket -> {};

  /**
   * Configures the given socket.
   *
   * @param socket the socket to configure (not yet connected)
   * @throws IOException if configuration fails
   */
  void configure(Socket socket) throws IOException;
}
