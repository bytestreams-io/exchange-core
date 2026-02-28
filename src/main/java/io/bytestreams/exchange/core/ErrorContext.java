package io.bytestreams.exchange.core;

import java.util.Objects;
import java.util.Optional;

/**
 * Structured context for a channel error, carrying the cause and any associated request/response.
 *
 * @param cause the throwable that triggered the error
 * @param request the request message, if associated with the error
 * @param response the response message, if associated with the error
 * @param <REQ> the request message type
 * @param <RESP> the response message type
 */
public record ErrorContext<REQ, RESP>(
    Throwable cause, Optional<REQ> request, Optional<RESP> response) {
  public ErrorContext {
    Objects.requireNonNull(cause, "cause");
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(response, "response");
  }
}
