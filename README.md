# The Link HTTP

HTTP and WebSocket adapter for The Link. Browser and Server entry points remain
separate so Server dependencies do not enter the Browser module graph.

## Install

```sh
npm install @the-link/http
```

## Client

```ts
import { HttpClient } from "@the-link/http/client"

const client = new HttpClient("https://example.com/link")
const [total] = await client.$outbound.publish("sum", 20, 22)
```

Call `subscribe()` or `subscribeAsync()` to open a WebSocket subscription.

## Server

```ts
import { HttpServer } from "@the-link/http/server"

const server = new HttpServer()

server.$inbound.subscribe("sum", (left: number, right: number) => left + right)
server.prepareConnection()
```

`server.app` is the Hono application containing the adapter routes. Pass a Hono
WebSocket upgrader to `prepareConnection()` when subscriptions are required.

## Serialization

The adapter uses JSON encoded as UTF-8 bytes by default. Both sides allow the
policy to be replaced:

```ts
client.setSerialize(serialize)
client.setDeserialize(deserialize)

server.setSerialize(serialize)
server.setDeserialize(deserialize)
```

The adapter does not decide which policy an application must use.

## Streams

A `ReadableStream` in a message, in either direction, travels as a reference
when the application's codec carries streams (such as The Link MessagePack):
the adapter hands its codec a `streams` option on every message. The chunks do
not travel over the WebSocket, so a large stream never delays other messages:

- a stream the server sends is read by the client with GET `/stream/:id`,
  whose response body carries its chunks;
- a stream the client sends is written with POST `/stream/:id`, one chunk per
  request, and the server answers each request only once that chunk is taken,
  so the client sends no further than the server reads.

Canceling a received stream stops its source on the other side, and a failing
source fails the received stream with its message. A reference is read once;
one nobody reads is released after a minute. `prepareConnection()` registers
the stream routes.

## Development

```sh
bun install --frozen-lockfile
bun run verify
```

## License

MIT
