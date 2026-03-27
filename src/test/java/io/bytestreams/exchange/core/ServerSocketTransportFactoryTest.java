package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ServerSocketTransportFactoryTest {

  @Nested
  class Create {

    @Test
    void accepts_connection_and_returns_transport() throws Exception {
      try (ServerSocketTransportFactory factory = ServerSocketTransportFactory.builder(0).build()) {
        Socket client = new Socket("localhost", factory.port());
        try (Transport transport = factory.create()) {
          assertThat(transport).isInstanceOf(SocketTransport.class);
          assertThat(transport.inputStream()).isNotNull();
          assertThat(transport.outputStream()).isNotNull();
        } finally {
          client.close();
        }
      }
    }

    @Test
    void applies_socket_configurator() throws Exception {
      AtomicReference<Socket> capturedSocket = new AtomicReference<>();

      try (ServerSocketTransportFactory factory =
          ServerSocketTransportFactory.builder(0)
              .socketConfigurator(
                  socket -> {
                    capturedSocket.set(socket);
                    socket.setTcpNoDelay(true);
                    socket.setKeepAlive(true);
                  })
              .build()) {
        Socket client = new Socket("localhost", factory.port());
        try (Transport transport = factory.create()) {
          assertThat(capturedSocket.get()).isNotNull();
          assertThat(capturedSocket.get().getTcpNoDelay()).isTrue();
          assertThat(capturedSocket.get().getKeepAlive()).isTrue();
        } finally {
          client.close();
        }
      }
    }

    @Test
    void each_create_returns_new_transport() throws Exception {
      try (ServerSocketTransportFactory factory = ServerSocketTransportFactory.builder(0).build()) {
        Socket c1 = new Socket("localhost", factory.port());
        Socket c2 = new Socket("localhost", factory.port());
        try (Transport t1 = factory.create();
            Transport t2 = factory.create()) {
          assertThat(t1).isNotSameAs(t2);
        } finally {
          c1.close();
          c2.close();
        }
      }
    }

    @Test
    void closes_socket_when_configurator_throws() throws Exception {
      try (ServerSocketTransportFactory factory =
          ServerSocketTransportFactory.builder(0)
              .socketConfigurator(
                  socket -> {
                    throw new IOException("configurator failed");
                  })
              .build()) {
        Socket client = new Socket("localhost", factory.port());
        assertThatThrownBy(factory::create)
            .isInstanceOf(IOException.class)
            .hasMessage("configurator failed");
        client.close();
      }
    }

    @Test
    void throws_after_close() throws Exception {
      ServerSocketTransportFactory factory = ServerSocketTransportFactory.builder(0).build();
      factory.close();

      assertThatThrownBy(factory::create).isInstanceOf(IOException.class);
    }
  }

  @Nested
  class Lifecycle {

    @Test
    void port_returns_bound_port() throws Exception {
      try (ServerSocketTransportFactory factory = ServerSocketTransportFactory.builder(0).build()) {
        assertThat(factory.port()).isPositive();
      }
    }

    @Test
    void binds_to_specified_port() throws Exception {
      try (ServerSocketTransportFactory factory = ServerSocketTransportFactory.builder(0).build()) {
        int port = factory.port();
        assertThat(port).isBetween(1, 65535);
      }
    }

    @Test
    void close_closes_server_socket() throws Exception {
      ServerSocketTransportFactory factory = ServerSocketTransportFactory.builder(0).build();
      int port = factory.port();
      factory.close();

      assertThatThrownBy(() -> new Socket("localhost", port)).isInstanceOf(IOException.class);
    }
  }

  @Nested
  class BuilderValidation {

    @Test
    void rejects_negative_port() {
      assertThatThrownBy(() -> ServerSocketTransportFactory.builder(-1))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejects_port_above_65535() {
      assertThatThrownBy(() -> ServerSocketTransportFactory.builder(65536))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejects_null_host() {
      var builder = ServerSocketTransportFactory.builder(0);
      assertThatThrownBy(() -> builder.host(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejects_null_configurator() {
      var builder = ServerSocketTransportFactory.builder(0);
      assertThatThrownBy(() -> builder.socketConfigurator(null))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void build_fails_on_bind_error() throws Exception {
      try (ServerSocketTransportFactory first = ServerSocketTransportFactory.builder(0).build()) {
        int boundPort = first.port();
        assertThatThrownBy(() -> ServerSocketTransportFactory.builder(boundPort).build())
            .isInstanceOf(IOException.class);
      }
    }

    @Test
    void custom_host_and_backlog() throws Exception {
      try (ServerSocketTransportFactory factory =
          ServerSocketTransportFactory.builder(0).host("127.0.0.1").backlog(10).build()) {
        assertThat(factory.port()).isPositive();
      }
    }
  }
}
