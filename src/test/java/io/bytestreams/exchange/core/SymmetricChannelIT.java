package io.bytestreams.exchange.core;

import static io.bytestreams.exchange.core.TestFixture.FRAMED_READER;
import static io.bytestreams.exchange.core.TestFixture.FRAMED_WRITER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class SymmetricChannelIT {

  private static final Function<String, String> ID = msg -> msg.split(":")[0];

  private static String reply(String prefix, String req) {
    // Preserve the request ID as the first token so the response correlates correctly.
    // e.g. reply("B-reply", "reqA:hello") -> "reqA:B-reply:hello"
    int colon = req.indexOf(':');
    String id = req.substring(0, colon);
    String body = req.substring(colon + 1);
    return id + ":" + prefix + ":" + body;
  }

  @Test
  void bidirectionalExchange() throws Exception {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      int port = serverSocket.getLocalPort();

      CompletableFuture<SymmetricChannel<String>> peerBFuture = new CompletableFuture<>();

      VT.start(
          () -> {
            try {
              Socket accepted = serverSocket.accept();
              var ch =
                  SymmetricChannel.<String>symmetricBuilder()
                      .transport(new SocketTransport(accepted))
                      .writer(FRAMED_WRITER)
                      .reader(FRAMED_READER)
                      .idExtractor(ID)
                      .requestHandler((req, future) -> future.complete(reply("B-reply", req)))
                      .defaultTimeout(Duration.ofSeconds(5))
                      .build();
              ch.start();
              peerBFuture.complete(ch);
            } catch (IOException e) {
              peerBFuture.completeExceptionally(e);
            }
          },
          "peer-b-setup");

      Socket clientSocket = new Socket("localhost", port);
      SymmetricChannel<String> peerA =
          SymmetricChannel.<String>symmetricBuilder()
              .transport(new SocketTransport(clientSocket))
              .writer(FRAMED_WRITER)
              .reader(FRAMED_READER)
              .idExtractor(ID)
              .requestHandler((req, future) -> future.complete(reply("A-reply", req)))
              .defaultTimeout(Duration.ofSeconds(5))
              .build();
      peerA.start();

      SymmetricChannel<String> peerB = peerBFuture.get(5, TimeUnit.SECONDS);

      // A sends request to B
      CompletableFuture<String> aToB = peerA.request("reqA:hello");
      assertThat(aToB).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("reqA:B-reply:hello");

      // B sends request to A
      CompletableFuture<String> bToA = peerB.request("reqB:world");
      assertThat(bToA).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("reqB:A-reply:world");

      // Concurrent bidirectional
      CompletableFuture<String> a2 = peerA.request("reqA2:ping");
      CompletableFuture<String> b2 = peerB.request("reqB2:pong");
      assertThat(a2).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("reqA2:B-reply:ping");
      assertThat(b2).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("reqB2:A-reply:pong");

      clientSocket.close();
      peerA.close();
      peerB.close();
    }
  }
}
