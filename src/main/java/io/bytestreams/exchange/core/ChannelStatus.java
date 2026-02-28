package io.bytestreams.exchange.core;

/** The lifecycle status of a {@link Channel}. */
public enum ChannelStatus {
  /** Constructed but not yet started. */
  INIT,
  /** Started and ready to send/receive. */
  READY,
  /** Shutting down gracefully; no new requests accepted. */
  CLOSING,
  /** Permanently closed. */
  CLOSED
}
