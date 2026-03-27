package io.bytestreams.exchange.core;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TransportFactory} that accepts incoming TCP connections on a {@link ServerSocket}.
 *
 * <p>Each call to {@link #create()} blocks until a client connects, applies the {@link
 * SocketConfigurator} to the accepted socket, and returns a {@link SocketTransport}.
 *
 * <p>The factory owns the {@link ServerSocket} and binds it eagerly in {@link Builder#build()}.
 * Close the factory to stop accepting connections.
 *
 * <p>Use {@link #builder(int)} to configure the bind address, backlog, and socket options.
 */
public final class ServerSocketTransportFactory implements TransportFactory, Closeable {

  private static final Logger log = LoggerFactory.getLogger(ServerSocketTransportFactory.class);

  private final ServerSocket serverSocket;
  private final SocketConfigurator configurator;

  private ServerSocketTransportFactory(ServerSocket serverSocket, SocketConfigurator configurator) {
    this.serverSocket = serverSocket;
    this.configurator = configurator;
  }

  public static Builder builder(int port) {
    if (port < 0 || port > 65535) {
      throw new IllegalArgumentException("port must be between 0 and 65535");
    }
    return new Builder(port);
  }

  /** Returns the local port this factory is bound to. */
  public int port() {
    return serverSocket.getLocalPort();
  }

  @Override
  public Transport create() throws IOException {
    Socket socket = serverSocket.accept();
    try {
      configurator.configure(socket);
      log.debug(
          "Accepted connection from {}:{}",
          socket.getInetAddress().getHostAddress(),
          socket.getPort());
      return new SocketTransport(socket);
    } catch (IOException e) {
      Closeables.closeQuietly(socket);
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    serverSocket.close();
  }

  public static final class Builder {
    private final int port;
    private String host = "0.0.0.0";
    private int backlog = 50;
    private SocketConfigurator configurator = SocketConfigurator.DEFAULT;

    private Builder(int port) {
      this.port = port;
    }

    public Builder host(String host) {
      this.host = Objects.requireNonNull(host, "host");
      return this;
    }

    public Builder backlog(int backlog) {
      this.backlog = backlog;
      return this;
    }

    public Builder socketConfigurator(SocketConfigurator configurator) {
      this.configurator = Objects.requireNonNull(configurator, "configurator");
      return this;
    }

    public ServerSocketTransportFactory build() throws IOException {
      ServerSocket serverSocket = new ServerSocket();
      try {
        serverSocket.bind(new InetSocketAddress(host, port), backlog);
      } catch (IOException e) {
        Closeables.closeQuietly(serverSocket);
        throw e;
      }
      log.info("Bound to {}:{}", host, serverSocket.getLocalPort());
      return new ServerSocketTransportFactory(serverSocket, configurator);
    }
  }
}
