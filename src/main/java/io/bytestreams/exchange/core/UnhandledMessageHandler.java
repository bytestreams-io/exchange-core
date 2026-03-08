package io.bytestreams.exchange.core;

/**
 * Callback for inbound messages that could not be correlated to a pending request in a {@link
 * MultiplexedChannel}.
 *
 * @param <T> the message type
 */
@FunctionalInterface
public interface UnhandledMessageHandler<T> {
  static <T> UnhandledMessageHandler<T> noop() {
    return ignored -> {};
  }

  void onMessage(T message);
}
