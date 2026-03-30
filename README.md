# exchange-core

[![Build](https://github.com/bytestreams-io/exchange-core/actions/workflows/build.yaml/badge.svg)](https://github.com/bytestreams-io/exchange-core/actions/workflows/build.yaml)
[![CodeQL](https://github.com/bytestreams-io/exchange-core/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/bytestreams-io/exchange-core/actions/workflows/github-code-scanning/codeql)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=io.bytestreams.exchange%3Acore&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=io.bytestreams.exchange%3Acore)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=io.bytestreams.exchange%3Acore&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=io.bytestreams.exchange%3Acore)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=io.bytestreams.exchange%3Acore&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=io.bytestreams.exchange%3Acore)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=io.bytestreams.exchange%3Acore&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=io.bytestreams.exchange%3Acore)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=io.bytestreams.exchange%3Acore&metric=bugs)](https://sonarcloud.io/summary/new_code?id=io.bytestreams.exchange%3Acore)
[![codecov](https://codecov.io/gh/bytestreams-io/exchange-core/graph/badge.svg)](https://codecov.io/gh/bytestreams-io/exchange-core)
[![GitHub License](https://img.shields.io/github/license/bytestreams-io/exchange-core)](LICENSE)
[![Javadoc](https://img.shields.io/badge/javadoc-latest-blue)](https://bytestreams-io.github.io/exchange-core/)

Core interfaces and channels for request-response message exchange over any transport.

## Overview

exchange-core provides the building blocks for application protocols that follow a request-response pattern. It handles correlation, concurrency control, backpressure, graceful shutdown, and observability — so protocol implementations can focus on message encoding.

Three correlation models are supported:

| Model | In-Flight | Ordering | Correlation | Use Case |
|---|---|---|---|---|
| **Lockstep** | 1 | Strict | Positional | Simple command-response protocols |
| **Pipelined** | N | Strict | Positional | HTTP/1.1 pipelining, ordered batch protocols |
| **Multiplexed** | N | Relaxed | ID-based | HTTP/2, gRPC, custom binary protocols |

## Requirements

- Java 21+
- JPMS module: `io.bytestreams.exchange.core`

## Installation

```xml
<dependency>
  <groupId>io.bytestreams.exchange</groupId>
  <artifactId>core</artifactId>
  <version>${version}</version>
</dependency>
```

## Quick Start

### Define a wire format

Implement `MessageReader` and `MessageWriter` to define how messages are serialized. Here's a length-prefixed string codec:

```java
MessageWriter<String> writer = (msg, out) -> {
    byte[] data = msg.getBytes(StandardCharsets.UTF_8);
    out.write(ByteBuffer.allocate(4).putInt(data.length).array());
    out.write(data);
    out.flush();
};

MessageReader<String> reader = in -> {
    byte[] lenBuf = in.readNBytes(4);
    if (lenBuf.length < 4) throw new IOException("Unexpected end of stream");
    int len = ByteBuffer.wrap(lenBuf).getInt();
    byte[] data = in.readNBytes(len);
    if (data.length < len) throw new IOException("Unexpected end of stream");
    return new String(data, StandardCharsets.UTF_8);
};
```

### Pipelined client + server

```java
// --- Server side ---
ServerChannel<String, String> server = ServerChannel.<String, String>builder()
    .transport(new SocketTransport(acceptedSocket))
    .requestReader(reader)
    .responseWriter(writer)
    .requestHandler((req, future) -> future.complete("echo:" + req))
    .build();
server.start();

// --- Client side ---
PipelinedChannel<String, String> client = PipelinedChannel.<String, String>builder()
    .transport(new SocketTransport(new Socket("localhost", port)))
    .requestWriter(writer)
    .responseReader(reader)
    .maxConcurrency(1)              // lockstep: one in-flight request
    .defaultTimeout(Duration.ofSeconds(5))
    .build();
client.start();

CompletableFuture<String> response = client.request("hello");
// response completes with "echo:hello"
```

### Multiplexed client

```java
MultiplexedChannel<Request, Response> client = MultiplexedChannel.<Request, Response>builder()
    .transport(transport)
    .requestWriter(reqWriter)
    .responseReader(respReader)
    .requestIdExtractor(Request::id)
    .responseIdExtractor(Response::id)
    .maxConcurrency(100)
    .defaultTimeout(Duration.ofSeconds(10))
    .build();
client.start();

// Requests can be sent concurrently; responses complete out of order
CompletableFuture<Response> r1 = client.request(new Request("1", "ping"));
CompletableFuture<Response> r2 = client.request(new Request("2", "pong"));
```

### Symmetric (bidirectional) channel

Both sides can initiate requests on the same connection:

```java
SymmetricChannel<Message> channel = SymmetricChannel.<Message>symmetricBuilder()
    .transport(transport)
    .writer(msgWriter)
    .reader(msgReader)
    .idExtractor(Message::id)
    .requestHandler((inbound, future) -> future.complete(process(inbound)))
    .build();
channel.start();

// Send outbound requests
CompletableFuture<Message> reply = channel.request(new Message("1", "hello"));

// Inbound requests from the remote peer are dispatched to the requestHandler
```

### TCP server with Acceptor

Accept multiple client connections and create a channel per connection:

```java
ServerSocketTransportFactory transportFactory = ServerSocketTransportFactory.builder(8080)
    .socketConfigurator(socket -> {
        socket.setKeepAlive(true);
        socket.setTcpNoDelay(true);
    })
    .build();

Acceptor acceptor = Acceptor.builder(transportFactory)
    .channelFactory(transport -> ServerChannel.<String, String>builder()
        .transport(transport)
        .requestReader(reader)
        .responseWriter(writer)
        .requestHandler((req, future) -> future.complete("ok"))
        .build())
    .build();
acceptor.start();

// acceptor.close() gracefully shuts down all active channels and the transport factory
```

### Client with transport factory

Use `ClientSocketTransportFactory` for configurable client connections:

```java
ClientSocketTransportFactory factory = ClientSocketTransportFactory.builder("localhost", 8080)
    .connectTimeoutMillis(5000)
    .socketConfigurator(socket -> socket.setTcpNoDelay(true))
    .build();

Transport transport = factory.create();
PipelinedChannel<String, String> client = PipelinedChannel.<String, String>builder()
    .transport(transport)
    .requestWriter(writer)
    .responseReader(reader)
    .build();
client.start();
```

### Resilient client with auto-reconnect

Wrap a transport factory with `ResilientTransport` for automatic reconnection with backoff:

```java
ClientSocketTransportFactory factory = ClientSocketTransportFactory.builder("localhost", 8080)
    .connectTimeoutMillis(5000)
    .build();

ResilientTransport transport = ResilientTransport.builder(factory)
    .backoffStrategy(BackoffStrategy.exponential(Duration.ofMillis(100))
        .withMax(Duration.ofSeconds(30))
        .withJitter(0.5))
    .maxAttempts(10)
    .build();

PipelinedChannel<String, String> client = PipelinedChannel.<String, String>builder()
    .transport(transport)
    .requestWriter(writer)
    .responseReader(reader)
    .errorHandler((ch, ctx) -> false) // return false to tolerate errors and allow reconnect
    .build();
client.start();

// If the connection drops, ResilientTransport transparently reconnects
// on the next inputStream()/outputStream() call
```

## Channel Types

### PipelinedChannel

Positional (FIFO) correlation — responses are matched to requests in the order they were sent.

- `maxConcurrency(1)` gives lockstep behavior (one request at a time)
- `maxConcurrency(N)` allows N in-flight pipelined requests
- Cascade failure: if the channel encounters an error, all pending requests fail

### MultiplexedChannel

ID-based correlation — each request and response carries a message ID, allowing out-of-order completion.

- `requestIdExtractor` / `responseIdExtractor` extract the correlation ID
- Semaphore-based backpressure via `maxConcurrency`
- Uncorrelated responses are dispatched to an `UnhandledMessageHandler`
- Individual request isolation: one timeout doesn't affect other pending requests

### SymmetricChannel

Extends `MultiplexedChannel` for bidirectional peer-to-peer messaging where both sides use the same message type.

- Outbound requests via `request()` are multiplexed by message ID
- Inbound messages that don't match a pending outbound request are treated as new requests and dispatched to the `RequestHandler`
- OTel metrics distinguish `direction=outbound` vs `direction=inbound`

### ServerChannel

Receives requests and dispatches them to a `RequestHandler`. The handler completes a `CompletableFuture` with the response.

- Fully concurrent: multiple requests can be in flight simultaneously
- Tracks active requests for graceful shutdown
- Pairs naturally with `Acceptor` for TCP servers

## Architecture

Each channel runs two virtual threads:

```
                    ┌──────────────┐
  Transport ──────▶ │  Reader VT   │ ──▶ correlate / dispatch
  (InputStream)     └──────────────┘

                    ┌──────────────┐
  writeQueue ─────▶ │  Writer VT   │ ──▶ Transport
                    └──────────────┘     (OutputStream)
```

- **Reader thread** continuously reads messages from the transport and either correlates them to pending requests (client channels) or dispatches them to the request handler (server/symmetric channels).
- **Writer thread** drains the write queue and flushes messages to the transport.
- A `writeLock` ensures that registering a pending request and enqueuing the outbound message happen atomically.

### Lifecycle

Channels follow a four-state lifecycle:

```
INIT ──▶ READY ──▶ CLOSING ──▶ CLOSED
```

- `INIT` — constructed but not started
- `READY` — I/O loops are running
- `CLOSING` — graceful shutdown in progress; pending work drains before the channel fully closes
- `CLOSED` — all resources released, `closeFuture()` is complete

### Error Handling

Errors are routed to a pluggable `ErrorHandler`:

```java
ErrorHandler<REQ, RESP> handler = (channel, context) -> {
    log.warn("Error on {}: {}", channel.id(), context.cause().getMessage());
    return false; // return true to close the channel, false to continue
};
```

The `ErrorContext` record provides the error, the request (if available), and the response (if available).

### Transport

`Transport` is a simple interface over `InputStream`/`OutputStream` with OTel attributes:

```java
public interface Transport extends Closeable {
    InputStream inputStream() throws IOException;
    OutputStream outputStream() throws IOException;
    Attributes attributes();  // network metadata for OTel
}
```

`SocketTransport` is the built-in TCP implementation. Custom transports (TLS, in-memory, Unix domain sockets) can be plugged in by implementing this interface.

### TransportFactory

`TransportFactory` is a functional interface for creating new transport instances:

```java
@FunctionalInterface
public interface TransportFactory {
    Transport create() throws IOException;
}
```

Built-in implementations:

| Factory | Description |
|---|---|
| `ClientSocketTransportFactory` | Opens a new TCP connection to a remote host/port |
| `ServerSocketTransportFactory` | Accepts an incoming TCP connection on a bound port |

Both support `SocketConfigurator` for setting socket options (keepAlive, tcpNoDelay, timeouts, buffer sizes).

### ResilientTransport

A `Transport` decorator that automatically reconnects using a `TransportFactory` when the underlying connection fails. Reconnection happens at message boundaries — when `inputStream()` or `outputStream()` is called after a failure.

- Wrapper streams detect I/O failures and set a stale flag
- Next stream access triggers the reconnect loop with configurable backoff
- Thread-safe: concurrent reader/writer failures trigger only one reconnect
- OTel counters: `transport.reconnect.total`, `.success`, `.gave_up`

### BackoffStrategy

Composable backoff strategies for retry delays:

```java
// Fixed 500ms delay
BackoffStrategy.fixed(Duration.ofMillis(500))

// Exponential: 100ms, 200ms, 400ms, 800ms, ...
BackoffStrategy.exponential(Duration.ofMillis(100))

// Composed: exponential capped at 30s with 50% jitter
BackoffStrategy.exponential(Duration.ofMillis(100))
    .withMax(Duration.ofSeconds(30))
    .withJitter(0.5)
```

### Acceptor

A transport-agnostic accept loop that calls `TransportFactory.create()` repeatedly, passing each transport to a channel factory. Tracks active channels and closes them on shutdown.

- Works with any `TransportFactory` (TCP, Unix domain sockets, in-memory, etc.)
- Closes the factory on shutdown if it implements `Closeable`
- OTel metrics: `acceptor.connections.active`

## Observability

All channels are instrumented with [OpenTelemetry](https://opentelemetry.io/) tracing and metrics out of the box.

### Metrics

| Metric | Type | Description |
|---|---|---|
| `request.active` | UpDownCounter | Currently in-flight requests |
| `request.total` | Counter | Total requests (with error_type on failure) |
| `request.errors` | Counter | Failed requests |
| `request.duration` | Histogram | Request latency in milliseconds |
| `write_queue.size` | UpDownCounter | Pending outbound messages |
| `acceptor.connections.active` | UpDownCounter | Active connections (Acceptor) |
| `transport.reconnect.total` | Counter | Reconnect attempts (ResilientTransport) |
| `transport.reconnect.success` | Counter | Successful reconnections |
| `transport.reconnect.gave_up` | Counter | Exhausted max reconnect attempts |

### Metric Attributes

All metrics carry `channel_type`. Per-request metrics also include `message_type`. Additional attributes vary by channel type:

- **Symmetric**: `direction` (outbound/inbound)
- **Client channels**: `network.peer.address`
- **Acceptor**: per-connection transport attributes
- **On error**: `error_type` (exception class name)

### Traces

Each channel creates a root span. Individual requests/handlers create child spans linked to the channel span. Custom `Meter` and `Tracer` instances can be injected via the builder.

## Building

```bash
mvn verify
```

This runs unit tests, integration tests, code formatting checks (Google Java Format via Spotless), and enforces 100% code coverage via JaCoCo.

## License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)
