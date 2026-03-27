package io.bytestreams.exchange.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TransportFactory} that creates client-side TCP connections.
 *
 * <p>Each call to {@link #create()} opens a new {@link Socket} to the configured host and port,
 * applies the {@link SocketConfigurator}, connects, and returns a {@link SocketTransport}.
 *
 * <p>Use {@link #builder(String, int)} to configure the target address, connect timeout, and socket
 * options.
 */
public final class ClientSocketTransportFactory implements TransportFactory {

  private static final Logger log = LoggerFactory.getLogger(ClientSocketTransportFactory.class);

  private final String host;
  private final int port;
  private final int connectTimeoutMillis;
  private final SocketConfigurator configurator;

  private ClientSocketTransportFactory(
      String host, int port, int connectTimeoutMillis, SocketConfigurator configurator) {
    this.host = host;
    this.port = port;
    this.connectTimeoutMillis = connectTimeoutMillis;
    this.configurator = configurator;
  }

  public static Builder builder(String host, int port) {
    return new Builder(Objects.requireNonNull(host, "host"), validatePort(port));
  }

  @Override
  public Transport create() throws IOException {
    Socket socket = new Socket();
    try {
      configurator.configure(socket);
      socket.connect(new InetSocketAddress(host, port), connectTimeoutMillis);
      log.debug("Connected to {}:{}", host, port);
      return new SocketTransport(socket);
    } catch (IOException e) {
      Closeables.closeQuietly(socket);
      throw e;
    }
  }

  private static int validatePort(int port) {
    if (port < 0 || port > 65535) {
      throw new IllegalArgumentException("port must be between 0 and 65535");
    }
    return port;
  }

  public static final class Builder {
    private final String host;
    private final int port;
    private int connectTimeoutMillis;
    private SocketConfigurator configurator = SocketConfigurator.DEFAULT;

    private Builder(String host, int port) {
      this.host = host;
      this.port = port;
    }

    public Builder connectTimeoutMillis(int connectTimeoutMillis) {
      this.connectTimeoutMillis = connectTimeoutMillis;
      return this;
    }

    public Builder socketConfigurator(SocketConfigurator configurator) {
      this.configurator = Objects.requireNonNull(configurator, "configurator");
      return this;
    }

    public ClientSocketTransportFactory build() {
      return new ClientSocketTransportFactory(host, port, connectTimeoutMillis, configurator);
    }
  }
}
