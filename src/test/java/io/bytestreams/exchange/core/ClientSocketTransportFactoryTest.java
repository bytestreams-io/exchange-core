package io.bytestreams.exchange.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ClientSocketTransportFactoryTest {

  @Nested
  class Create {

    @Test
    void connects_to_server_and_returns_transport() throws Exception {
      try (ServerSocket server = new ServerSocket(0)) {
        ClientSocketTransportFactory factory =
            ClientSocketTransportFactory.builder("localhost", server.getLocalPort()).build();

        try (Transport transport = factory.create()) {
          assertThat(transport).isInstanceOf(SocketTransport.class);
          assertThat(transport.inputStream()).isNotNull();
          assertThat(transport.outputStream()).isNotNull();
        }
      }
    }

    @Test
    void applies_socket_configurator() throws Exception {
      try (ServerSocket server = new ServerSocket(0)) {
        AtomicReference<Socket> capturedSocket = new AtomicReference<>();

        ClientSocketTransportFactory factory =
            ClientSocketTransportFactory.builder("localhost", server.getLocalPort())
                .socketConfigurator(
                    socket -> {
                      capturedSocket.set(socket);
                      socket.setTcpNoDelay(true);
                      socket.setKeepAlive(true);
                    })
                .build();

        try (Transport transport = factory.create()) {
          assertThat(capturedSocket.get()).isNotNull();
          assertThat(capturedSocket.get().getTcpNoDelay()).isTrue();
          assertThat(capturedSocket.get().getKeepAlive()).isTrue();
        }
      }
    }

    @Test
    void throws_on_connection_refused() {
      ClientSocketTransportFactory factory =
          ClientSocketTransportFactory.builder("localhost", 1).build();

      assertThatThrownBy(factory::create).isInstanceOf(ConnectException.class);
    }

    @Test
    void closes_socket_on_connect_failure() {
      ClientSocketTransportFactory factory =
          ClientSocketTransportFactory.builder("localhost", 1).connectTimeoutMillis(100).build();

      assertThatThrownBy(factory::create).isInstanceOf(IOException.class);
    }

    @Test
    void each_create_returns_new_transport() throws Exception {
      try (ServerSocket server = new ServerSocket(0)) {
        ClientSocketTransportFactory factory =
            ClientSocketTransportFactory.builder("localhost", server.getLocalPort()).build();

        try (Transport t1 = factory.create();
            Transport t2 = factory.create()) {
          assertThat(t1).isNotSameAs(t2);
        }
      }
    }
  }

  @Nested
  class BuilderValidation {

    @Test
    void rejects_null_host() {
      assertThatThrownBy(() -> ClientSocketTransportFactory.builder(null, 8080))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejects_negative_port() {
      assertThatThrownBy(() -> ClientSocketTransportFactory.builder("localhost", -1))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejects_port_above_65535() {
      assertThatThrownBy(() -> ClientSocketTransportFactory.builder("localhost", 65536))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejects_null_configurator() {
      var builder = ClientSocketTransportFactory.builder("localhost", 8080);
      assertThatThrownBy(() -> builder.socketConfigurator(null))
          .isInstanceOf(NullPointerException.class);
    }
  }
}
