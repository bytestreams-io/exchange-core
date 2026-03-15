package io.bytestreams.exchange.core;

import static io.bytestreams.exchange.core.TestFixture.FRAMED_READER;
import static io.bytestreams.exchange.core.TestFixture.FRAMED_WRITER;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class SocketAcceptorIT {

  @Test
  void acceptorCreatesChannelAndHandlesRequest() throws Exception {
    CountDownLatch channelCreated = new CountDownLatch(1);
    CompletableFuture<String> receivedRequest = new CompletableFuture<>();

    SocketAcceptor acceptor =
        SocketAcceptor.builder()
            .port(0)
            .channelFactory(
                transport -> {
                  ServerChannel<String, String> channel =
                      ServerChannel.<String, String>builder()
                          .transport(transport)
                          .requestReader(FRAMED_READER)
                          .responseWriter(FRAMED_WRITER)
                          .requestHandler(
                              (req, future) -> {
                                receivedRequest.complete(req);
                                future.complete("accepted:" + req);
                              })
                          .build();
                  channelCreated.countDown();
                  return channel;
                })
            .build();
    acceptor.start();

    Socket clientSocket = new Socket("localhost", acceptor.port());
    PipelinedChannel<String, String> client =
        PipelinedChannel.<String, String>builder()
            .transport(new SocketTransport(clientSocket))
            .requestWriter(FRAMED_WRITER)
            .responseReader(FRAMED_READER)
            .maxConcurrency(1)
            .defaultTimeout(Duration.ofSeconds(5))
            .build();
    client.start();

    channelCreated.await(5, TimeUnit.SECONDS);

    CompletableFuture<String> response = client.request("world");
    assertThat(response).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("accepted:world");
    assertThat(receivedRequest).isCompletedWithValue("world");

    clientSocket.close();
    acceptor.close().get(2, TimeUnit.SECONDS);
  }
}
