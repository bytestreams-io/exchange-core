package io.bytestreams.exchange.core;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Inet6Address;
import java.net.Socket;
import java.util.Objects;

public final class SocketTransport implements Transport {

  private final Socket socket;

  public SocketTransport(Socket socket) {
    this.socket = Objects.requireNonNull(socket, "socket");
  }

  @Override
  public InputStream inputStream() throws IOException {
    return socket.getInputStream();
  }

  @Override
  public OutputStream outputStream() throws IOException {
    return socket.getOutputStream();
  }

  @Override
  public Attributes attributes() {
    AttributesBuilder builder = Attributes.builder();
    builder.put(OTel.NETWORK_TRANSPORT, "tcp");
    if (socket.isConnected() && socket.getInetAddress() != null) {
      String networkType = socket.getInetAddress() instanceof Inet6Address ? "ipv6" : "ipv4";
      builder.put(OTel.NETWORK_TYPE, networkType);
      builder.put(OTel.NETWORK_PEER_ADDRESS, socket.getInetAddress().getHostAddress());
      builder.put(OTel.NETWORK_PEER_PORT, (long) socket.getPort());
    }
    return builder.build();
  }

  @Override
  public void close() throws IOException {
    socket.close();
  }
}
