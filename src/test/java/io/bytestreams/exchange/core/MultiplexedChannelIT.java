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

class MultiplexedChannelIT {

  @Test
  void multipleRequestsCompletedByServer() throws Exception {
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
                          .requestHandler((req, future) -> future.complete(req))
                          .build();
                  ch.start();
                  return ch;
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });

      Socket clientSocket = new Socket("localhost", port);
      MultiplexedChannel<String, String> client =
          MultiplexedChannel.<String, String>builder()
              .transport(new SocketTransport(clientSocket))
              .requestWriter(FRAMED_WRITER)
              .responseReader(FRAMED_READER)
              .requestIdExtractor(msg -> msg)
              .responseIdExtractor(msg -> msg)
              .defaultTimeout(Duration.ofSeconds(5))
              .build();
      client.start();

      serverChannelFuture.get(5, TimeUnit.SECONDS);

      CompletableFuture<String> r1 = client.request("A");
      CompletableFuture<String> r2 = client.request("B");
      CompletableFuture<String> r3 = client.request("C");

      assertThat(r1).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("A");
      assertThat(r2).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("B");
      assertThat(r3).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("C");

      clientSocket.close();
      accepted[0].close();
    }
  }
}
