package io.bytestreams.exchange.core;

/**
 * Handles channel errors.
 *
 * <p>The default implementation returns {@code true}, which stops the channel (closes the
 * transport) on the first error. Override and return {@code false} for specific errors you wish to
 * tolerate — for example, transient I/O errors on a connection you want to keep alive.
 *
 * <p><b>Note:</b> This handler covers errors in the read and write loops, including exceptions
 * thrown by a {@link RequestHandler}. Only {@link Error} subclasses (e.g. {@link
 * OutOfMemoryError}) bypass this handler and are caught by the channel's outer safety net, which
 * closes the transport unconditionally.
 *
 * @param <REQ> the request message type
 * @param <RESP> the response message type
 */
public interface ErrorHandler<REQ, RESP> {

  /**
   * Called when an error occurs on the channel. Return {@code true} to stop the channel (close
   * transport); {@code false} to continue processing.
   *
   * @param channel the channel on which the error occurred
   * @param context the error context
   * @return {@code true} to stop, {@code false} to continue
   */
  default boolean stopOnError(Channel channel, ErrorContext<REQ, RESP> context) {
    return true;
  }
}
