package io.bytestreams.exchange.core;

import static io.bytestreams.exchange.core.TestFixture.FRAMED_READER;
import static io.bytestreams.exchange.core.TestFixture.FRAMED_WRITER;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class PipelinedChannelIT {

  @Test
  void clientServerRoundTrip() throws Exception {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      int port = serverSocket.getLocalPort();
      Socket[] accepted = new Socket[1];

      CompletableFuture<ServerChannel<String, String>> serverChannelFuture =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  accepted[0] = serverSocket.accept();
                  var ch =
                      ServerChannel.<String, String>builder()
                          .transport(new SocketTransport(accepted[0]))
                          .requestReader(FRAMED_READER)
                          .responseWriter(FRAMED_WRITER)
                          .requestHandler((req, future) -> future.complete("echo:" + req))
                          .build();
                  ch.start();
                  return ch;
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });

      Socket clientSocket = new Socket("localhost", port);
      PipelinedChannel<String, String> client =
          PipelinedChannel.<String, String>builder()
              .transport(new SocketTransport(clientSocket))
              .requestWriter(FRAMED_WRITER)
              .responseReader(FRAMED_READER)
              .maxConcurrency(1)
              .defaultTimeout(Duration.ofSeconds(5))
              .build();
      client.start();

      serverChannelFuture.get(5, TimeUnit.SECONDS);

      CompletableFuture<String> response = client.request("hello");
      assertThat(response).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("echo:hello");

      clientSocket.close();
      accepted[0].close();
    }
  }
}
