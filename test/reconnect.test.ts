import { afterEach, describe, expect, test } from "bun:test"
import { serializeJSON } from "@the-link/core"
import { HttpClient } from "../src/client.js"

type Listener = (event: { code?: number, reason?: string, data?: unknown }) => void

/** A WebSocket the test drives: each one the client opens is kept, to be accepted or closed. */
class FakeWebSocket {
    public static opened: FakeWebSocket[] = []
    public binaryType = "blob"
    private readonly listeners = new Map<string, Listener[]>()

    public constructor(public readonly url: string) { FakeWebSocket.opened.push(this) }

    public addEventListener(type: string, listener: Listener) {
        this.listeners.set(type, [...this.listeners.get(type) ?? [], listener])
    }

    public removeEventListener() {}

    public send() {}

    public close() {}

    /** The server accepts the subscription. */
    public accept() {
        const data = serializeJSON({ type: "subscribe", data: null }).buffer
        for (const listener of this.listeners.get("message") ?? []) listener({ data })
    }

    /** The connection closes abnormally, as a refused upgrade or a lost network does. */
    public drop() {
        for (const listener of this.listeners.get("close") ?? []) listener({ code: 1006, reason: "" })
    }
}

const realWebSocket = globalThis.WebSocket
const realSetTimeout = globalThis.setTimeout

afterEach(() => {
    globalThis.WebSocket = realWebSocket
    globalThis.setTimeout = realSetTimeout
    FakeWebSocket.opened = []
})

describe("HTTP client reconnects", () => {

    test("waits longer after each attempt that fails, and starts over once one is accepted", async () => {

        const waits: number[] = []
        globalThis.WebSocket = FakeWebSocket as unknown as typeof WebSocket
        // Every wait passes at once; the test reads how long each would have been.
        globalThis.setTimeout = ((callback: () => void, delay: number) => { waits.push(delay); queueMicrotask(callback); return 0 }) as unknown as typeof setTimeout
        const settle = () => new Promise(resolve => realSetTimeout(resolve, 5))

        const client = new HttpClient("http://example.test/link")
        client.subscribe()

        // Seven attempts in a row fail before they are accepted.
        for (let attempt = 0; attempt < 7; attempt++) {
            FakeWebSocket.opened.at(-1)!.drop()
            await settle()
        }

        expect(waits).toEqual([2000, 4000, 8000, 16_000, 30_000, 30_000, 30_000])

        // One is accepted, then closes: the next attempt comes as soon as the first one did.
        FakeWebSocket.opened.at(-1)!.accept()
        await settle()
        FakeWebSocket.opened.at(-1)!.drop()
        await settle()

        expect(waits.at(-1)).toBe(1000)

        // And failing again grows from there.
        FakeWebSocket.opened.at(-1)!.drop()
        await settle()

        expect(waits.at(-1)).toBe(2000)
    })
})
