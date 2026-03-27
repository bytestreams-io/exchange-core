package io.bytestreams.exchange.core;

import java.io.IOException;
import java.net.Socket;

/**
 * Configures a {@link Socket}.
 *
 * <p>Use this to set socket options such as timeouts, keep-alive, TCP no-delay, and buffer sizes.
 * For client sockets, the configurator is called before connecting so that options requiring
 * pre-connect setup (e.g. {@code SO_RCVBUF}) are safely applied. For accepted server sockets, it is
 * called after accept.
 */
@FunctionalInterface
public interface SocketConfigurator {

  /** A no-op configurator that leaves the socket with default settings. */
  SocketConfigurator DEFAULT = socket -> {};

  /**
   * Configures the given socket.
   *
   * @param socket the socket to configure
   * @throws IOException if configuration fails
   */
  void configure(Socket socket) throws IOException;
}
