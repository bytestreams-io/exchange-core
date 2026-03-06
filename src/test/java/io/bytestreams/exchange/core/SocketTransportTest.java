package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.Socket;
import org.junit.jupiter.api.Test;

class SocketTransportTest {

  @Test
  void inputStream_delegates_to_socket() throws IOException {
    Socket socket = mock(Socket.class);
    InputStream in = mock(InputStream.class);
    when(socket.getInputStream()).thenReturn(in);

    SocketTransport transport = new SocketTransport(socket);
    assertThat(transport.inputStream()).isSameAs(in);
  }

  @Test
  void outputStream_delegates_to_socket() throws IOException {
    Socket socket = mock(Socket.class);
    OutputStream out = mock(OutputStream.class);
    when(socket.getOutputStream()).thenReturn(out);

    SocketTransport transport = new SocketTransport(socket);
    assertThat(transport.outputStream()).isSameAs(out);
  }

  @Test
  void close_delegates_to_socket() throws IOException {
    Socket socket = mock(Socket.class);
    SocketTransport transport = new SocketTransport(socket);
    transport.close();
    verify(socket).close();
  }

  @Test
  void attributes_ipv4_socket() {
    Socket socket = mock(Socket.class);
    InetAddress addr = mock(InetAddress.class);
    when(socket.isConnected()).thenReturn(true);
    when(socket.getInetAddress()).thenReturn(addr);
    when(addr.getHostAddress()).thenReturn("192.168.1.1");
    when(socket.getPort()).thenReturn(8080);

    SocketTransport transport = new SocketTransport(socket);
    Attributes attrs = transport.attributes();

    assertThat(attrs.get(io.opentelemetry.api.common.AttributeKey.stringKey("network.transport")))
        .isEqualTo("tcp");
    assertThat(attrs.get(io.opentelemetry.api.common.AttributeKey.stringKey("network.type")))
        .isEqualTo("ipv4");
    assertThat(
            attrs.get(io.opentelemetry.api.common.AttributeKey.stringKey("network.peer.address")))
        .isEqualTo("192.168.1.1");
    assertThat(attrs.get(io.opentelemetry.api.common.AttributeKey.longKey("network.peer.port")))
        .isEqualTo(8080L);
  }

  @Test
  void attributes_ipv6_socket() {
    Socket socket = mock(Socket.class);
    Inet6Address addr = mock(Inet6Address.class);
    when(socket.isConnected()).thenReturn(true);
    when(socket.getInetAddress()).thenReturn(addr);
    when(addr.getHostAddress()).thenReturn("::1");
    when(socket.getPort()).thenReturn(9090);

    SocketTransport transport = new SocketTransport(socket);
    Attributes attrs = transport.attributes();

    assertThat(attrs.get(io.opentelemetry.api.common.AttributeKey.stringKey("network.type")))
        .isEqualTo("ipv6");
    assertThat(
            attrs.get(io.opentelemetry.api.common.AttributeKey.stringKey("network.peer.address")))
        .isEqualTo("::1");
    assertThat(attrs.get(io.opentelemetry.api.common.AttributeKey.longKey("network.peer.port")))
        .isEqualTo(9090L);
  }

  @Test
  void attributes_not_connected() {
    Socket socket = mock(Socket.class);
    when(socket.isConnected()).thenReturn(false);

    SocketTransport transport = new SocketTransport(socket);
    Attributes attrs = transport.attributes();

    assertThat(attrs.get(io.opentelemetry.api.common.AttributeKey.stringKey("network.transport")))
        .isEqualTo("tcp");
    assertThat(attrs.size()).isEqualTo(1);
  }

  @Test
  void attributes_connected_but_null_address() {
    Socket socket = mock(Socket.class);
    when(socket.isConnected()).thenReturn(true);
    when(socket.getInetAddress()).thenReturn(null);

    SocketTransport transport = new SocketTransport(socket);
    Attributes attrs = transport.attributes();

    assertThat(attrs.get(AttributeKey.stringKey("network.transport"))).isEqualTo("tcp");
    assertThat(attrs.get(AttributeKey.stringKey("network.type"))).isNull();
    assertThat(attrs.get(AttributeKey.stringKey("network.peer.address"))).isNull();
    assertThat(attrs.get(AttributeKey.longKey("network.peer.port"))).isNull();
  }

  @Test
  void constructor_rejects_null_socket() {
    assertThatThrownBy(() -> new SocketTransport(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("socket");
  }
}
