import { describe, expect, test } from "bun:test"
import { createBunWebSocket } from "hono/bun"
import { HttpServer } from "../src/server.js"
import ServerSocket from "../src/server-socket.js"
import { bytesToText } from "../src/transport.js"

describe("WebSocket subscription", () => {

    test("hands subscribers the HTTP request that opened the connection", async () => {

        const { upgradeWebSocket, websocket } = createBunWebSocket()
        const server = new HttpServer()
        const opened = Promise.withResolvers<ServerSocket>()

        server.onSubscribe(socket => { opened.resolve(socket as ServerSocket); return "ready" })
        server.prepareConnection(upgradeWebSocket)

        const listener = Bun.serve({ port: 0, fetch: server.app.fetch, websocket })

        try {
            const payload = bytesToText(new TextEncoder().encode(JSON.stringify({ current: { tab: 1 } })))
            const client = new WebSocket(`ws://localhost:${listener.port}/subscribe?payload=${payload}`, { headers: { "User-Agent": "Example/1.0" } } as never)
            const socket = await opened.promise

            expect(socket.payload).toEqual({ tab: 1 })
            expect(socket.request.headers.get("user-agent")).toBe("Example/1.0")
            expect(new URL(socket.request.url).pathname).toBe("/subscribe")

            client.close()
        }

        finally {
            listener.stop(true)
        }
    })
})
