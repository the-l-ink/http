import { afterEach, describe, expect, test } from "bun:test"
import type { UpgradeWebSocket, WSContext, WSEvents } from "hono/ws"
import { serializeJSON } from "@the-link/core"
import { HttpServer } from "../src/server.js"
import { bytesToText } from "../src/transport.js"

/** The address a client subscribes at, with no payload of its own. */
const subscribing = `/subscribe?payload=${encodeURIComponent(bytesToText(serializeJSON({ current: null })))}`

const realSetInterval = globalThis.setInterval
const realClearInterval = globalThis.clearInterval

afterEach(() => {
    globalThis.setInterval = realSetInterval
    globalThis.clearInterval = realClearInterval
})

describe("HTTP server keeps WebSockets alive", () => {

    test("pings an open socket every 30 seconds, and stops once it closes", async () => {

        // The timer the server starts, run by hand.
        const timers: { beat: () => void, delay: number, cleared: boolean }[] = []
        globalThis.setInterval = ((beat: () => void, delay: number) => { timers.push({ beat, delay, cleared: false }); return timers.length - 1 }) as unknown as typeof setInterval
        globalThis.clearInterval = ((index: number) => { if (timers[index]) timers[index].cleared = true }) as unknown as typeof clearInterval

        // An upgrade that hands the test the connection's events instead of a real socket.
        let events: WSEvents | undefined
        const upgradeWebSocket = (createEvents => async (context: Parameters<typeof createEvents>[0]) => {
            events = await createEvents(context)
            return context.text("upgraded")
        }) as UpgradeWebSocket

        const server = new HttpServer()
        server.onSubscribe(() => null)
        server.prepareConnection(upgradeWebSocket)
        await server.app.request(subscribing)

        let pings = 0
        const socket = { raw: { ping: () => pings++ }, readyState: 1, send() {}, close() {} } as unknown as WSContext
        await events!.onOpen!(new Event("open"), socket)

        expect(timers).toHaveLength(1)
        expect(timers[0].delay).toBe(30_000)

        timers[0].beat()
        timers[0].beat()
        expect(pings).toBe(2)

        await events!.onClose!(new CloseEvent("close"), socket)
        expect(timers[0].cleared).toBe(true)
    })

    test("leaves a socket that cannot ping as it is", async () => {

        let started = 0
        globalThis.setInterval = (() => { started++; return 0 }) as unknown as typeof setInterval

        let events: WSEvents | undefined
        const upgradeWebSocket = (createEvents => async (context: Parameters<typeof createEvents>[0]) => {
            events = await createEvents(context)
            return context.text("upgraded")
        }) as UpgradeWebSocket

        const server = new HttpServer()
        server.onSubscribe(() => null)
        server.prepareConnection(upgradeWebSocket)
        await server.app.request(subscribing)

        await events!.onOpen!(new Event("open"), { raw: {}, readyState: 1, send() {}, close() {} } as unknown as WSContext)

        expect(started).toBe(0)
    })
})
