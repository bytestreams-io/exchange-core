package io.bytestreams.exchange.core;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link Channel} that sends requests and receives correlated responses.
 *
 * @param <REQ> the request message type
 * @param <RESP> the response message type
 */
public interface ClientChannel<REQ, RESP> extends Channel {
  /**
   * Sends a message using the channel's configured default timeout and returns a future that
   * completes when a correlated response arrives.
   *
   * @param message the message to send
   * @return a future that completes with the correlated response
   * @throws DuplicateCorrelationIdException if the correlation id is already pending
   */
  CompletableFuture<RESP> request(REQ message);

  /**
   * Sends a message with a per-request timeout override.
   *
   * @param message the message to send
   * @param timeout the timeout for this request; must be positive
   * @return a future that completes with the correlated response
   */
  CompletableFuture<RESP> request(REQ message, Duration timeout);
}
