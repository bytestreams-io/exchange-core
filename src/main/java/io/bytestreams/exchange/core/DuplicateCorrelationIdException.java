package io.bytestreams.exchange.core;

/** Thrown when a message's correlation id is already pending. */
public class DuplicateCorrelationIdException extends RuntimeException {

  private final String messageId;

  DuplicateCorrelationIdException(String messageId) {
    super("Duplicate correlation id: " + messageId);
    this.messageId = messageId;
  }

  public String getMessageId() {
    return messageId;
  }
}
