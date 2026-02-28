package io.bytestreams.exchange.core;

import java.util.concurrent.CompletableFuture;

/**
 * Handles an incoming request by completing the given future with a response.
 *
 * <p>Exceptions thrown from {@link #handle} are caught by the channel's inner error-handling loop
 * and routed to the {@link ErrorHandler}. If the error handler returns {@code true} (the default),
 * the channel closes the transport. If it returns {@code false}, the channel logs the error and
 * continues processing. When possible, prefer completing the future exceptionally rather than
 * throwing — this records the error in metrics and tracing without consulting the error handler.
 *
 * @param <REQ> the request message type
 * @param <RESP> the response message type
 */
@FunctionalInterface
public interface RequestHandler<REQ, RESP> {
  void handle(REQ request, CompletableFuture<RESP> response);
}
