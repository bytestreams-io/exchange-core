package io.bytestreams.exchange.core;

/**
 * Listener for {@link ReconnectingTransport} lifecycle events.
 *
 * <p>All methods have default no-op implementations. This interface is for application-level
 * concerns such as circuit breakers or UI indicators. Logging and OTel metrics are handled directly
 * by {@link ReconnectingTransport}.
 */
public interface ReconnectListener {

  /** Called when the underlying transport connection is lost. */
  default void onDisconnect(Throwable cause) {}

  /**
   * Called before each reconnect attempt.
   *
   * @param attempt the 1-based attempt number
   */
  default void onReconnecting(int attempt) {}

  /**
   * Called after a successful reconnection.
   *
   * @param attempt the attempt number that succeeded
   */
  default void onReconnected(int attempt) {}

  /**
   * Called when all reconnect attempts have been exhausted.
   *
   * @param attempt the total number of attempts made
   * @param lastCause the exception from the last failed attempt
   */
  default void onGaveUp(int attempt, Throwable lastCause) {}
}
