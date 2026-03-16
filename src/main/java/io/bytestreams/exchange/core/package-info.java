/**
 * Core interfaces and channels for application protocol messaging.
 *
 * <p>Provides the building blocks for request-response application protocols over any {@link
 * io.bytestreams.exchange.core.Transport}. Three correlation models are supported:
 *
 * <ul>
 *   <li><b>Lockstep</b> — one in-flight request, positional correlation (pipelined with capacity 1)
 *   <li><b>Pipelined</b> — N in-flight requests, strict ordering, positional correlation
 *   <li><b>Multiplexed</b> — N in-flight requests, relaxed ordering, explicit ID correlation
 * </ul>
 *
 * <h2>Interfaces</h2>
 *
 * <ul>
 *   <li>{@link io.bytestreams.exchange.core.Channel} — logical messaging channel
 *   <li>{@link io.bytestreams.exchange.core.ClientChannel} — client-side channel for sending
 *       requests
 *   <li>{@link io.bytestreams.exchange.core.Transport} — pluggable I/O transport
 *   <li>{@link io.bytestreams.exchange.core.ErrorHandler} — handles channel errors
 * </ul>
 *
 * <h2>Types</h2>
 *
 * <ul>
 *   <li>{@link io.bytestreams.exchange.core.ChannelStatus} — channel lifecycle states
 *   <li>{@link io.bytestreams.exchange.core.ErrorContext} — structured error information
 *   <li>{@link io.bytestreams.exchange.core.DuplicateCorrelationIdException} — duplicate
 *       correlation key
 * </ul>
 *
 * <h2>Extension Points</h2>
 *
 * <ul>
 *   <li>{@link io.bytestreams.exchange.core.AbstractChannel} — base class for building custom
 *       channel implementations with managed I/O loops, OTel instrumentation, and lifecycle
 * </ul>
 *
 * <h2>Implementations</h2>
 *
 * <ul>
 *   <li>{@link io.bytestreams.exchange.core.PipelinedChannel} — Transport-backed pipelined client
 *       channel
 *   <li>{@link io.bytestreams.exchange.core.MultiplexedChannel} — Transport-backed multiplexed
 *       client channel
 *   <li>{@link io.bytestreams.exchange.core.SymmetricChannel} — bidirectional symmetric channel
 *   <li>{@link io.bytestreams.exchange.core.ServerChannel} — server-side channel for receiving
 *       requests
 *   <li>{@link io.bytestreams.exchange.core.SocketTransport} — TCP socket transport
 *   <li>{@link io.bytestreams.exchange.core.SocketAcceptor} — server-side TCP connection acceptor
 * </ul>
 */
package io.bytestreams.exchange.core;
