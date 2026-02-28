package io.bytestreams.exchange.core;

import java.util.concurrent.CompletableFuture;

/** A logical messaging channel between two endpoints. */
public interface Channel {
  /**
   * Returns a stable identifier for this channel.
   *
   * @return the channel identifier
   */
  String id();

  /**
   * Returns the current lifecycle status of this channel.
   *
   * @return the channel status
   */
  ChannelStatus status();

  /**
   * Returns a future that completes when this channel is fully closed, without triggering close.
   *
   * @return a read-only view of the close future
   */
  CompletableFuture<Void> closeFuture();

  /**
   * Starts the channel's I/O loops. Must be called exactly once after construction.
   *
   * @throws IllegalStateException if the channel has already been started
   */
  void start();

  /**
   * Closes this channel gracefully.
   *
   * @return a future that completes when the channel is fully closed
   */
  CompletableFuture<Void> close();
}
