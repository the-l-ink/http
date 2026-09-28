import { afterEach, describe, expect, test } from "bun:test"
import type { CodecOptions } from "@the-link/core"
import { HttpClient } from "../src/client.js"
import { HttpServer } from "../src/server.js"
import { decodeFrames, encodeFrame } from "../src/streams.js"

/** JSON that carries streams as references, standing in for an application's codec. */
const encoder = new TextEncoder()
const decoder = new TextDecoder()

const serialize = (value: unknown, options?: CodecOptions) => encoder.encode(JSON.stringify(value, (_key, entry) =>
    entry instanceof ReadableStream ? { $stream: options?.streams?.export(entry) } : entry))

const deserialize = (bytes: Uint8Array, options?: CodecOptions) => JSON.parse(decoder.decode(bytes), (_key, entry) =>
    entry && typeof entry === "object" && typeof entry.$stream === "string" ? options?.streams?.import(entry.$stream) : entry)

const realFetch = globalThis.fetch

afterEach(() => { globalThis.fetch = realFetch })

/** A server and a client whose requests reach that server directly. */
function connected() {

    const server = new HttpServer()

    server.setSerialize(serialize)
    server.setDeserialize(deserialize)
    server.prepareConnection()

    globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => server.app.fetch(new Request(input, init))) as typeof fetch

    const client = new HttpClient("http://link.test")

    client.setSerialize(serialize)
    client.setDeserialize(deserialize)

    return { server, client }
}

function lines(count: number, state = { read: 0, canceled: null as unknown }) {

    return {
        state,
        stream: new ReadableStream<string>({
            pull(controller) {
                if (state.read === count) return controller.close()
                controller.enqueue(`line ${state.read++}`)
            },
            cancel(reason) { state.canceled = reason }
        }, { highWaterMark: 0 })
    }
}

async function all(stream: ReadableStream<unknown>) {

    const chunks: unknown[] = []

    for await (const chunk of stream) chunks.push(chunk)

    return chunks
}

const settle = () => new Promise(resolve => setTimeout(resolve, 30))

describe("HTTP streams", () => {

    test("a stream the server answers with is read by the client on its own request", async () => {

        const { server, client } = connected()

        server.$inbound.subscribe("tail", () => ({ size: 3, content: lines(3).stream }))

        const [answer] = await client.$outbound.publish("tail") as [{ size: number, content: ReadableStream }]

        expect(answer.size).toBe(3)
        expect(await all(answer.content)).toEqual(["line 0", "line 1", "line 2"])
    })

    test("a stream the client sends is read by the server, one taken chunk at a time", async () => {

        const { server, client } = connected()

        server.$inbound.subscribe("save", async (content: ReadableStream<string>) => (await all(content)).join(","))

        const [saved] = await client.$outbound.publish("save", lines(4).stream)

        expect(saved).toBe("line 0,line 1,line 2,line 3")
    })

    test("when the client stops reading, the server's source stops", async () => {

        const { server, client } = connected()
        const source = lines(1000)

        server.$inbound.subscribe("tail", () => source.stream)

        const [content] = await client.$outbound.publish("tail") as [ReadableStream<string>]
        const reader = content.getReader()

        await reader.read()
        await reader.cancel(new Error("Enough"))
        await settle()

        expect(source.state.canceled).toBeTruthy()
        expect(source.state.read).toBeLessThan(1000)
    })

    test("when the server stops reading, the client's source stops", async () => {

        const { server, client } = connected()
        const source = lines(1000)

        server.$inbound.subscribe("peek", async (content: ReadableStream<string>) => {
            const reader = content.getReader()
            const first = await reader.read()
            await reader.cancel(new Error("Enough"))
            return first.value
        })

        const [first] = await client.$outbound.publish("peek", source.stream)

        await settle()

        expect(first).toBe("line 0")
        expect(source.state.canceled).toBeTruthy()
        expect(source.state.read).toBeLessThan(1000)
    })

    test("a failing source fails the stream on the other side with its message", async () => {

        const { server, client } = connected()

        server.$inbound.subscribe("tail", () => new ReadableStream({ pull() { throw new Error("Disk failed") } }))

        const [content] = await client.$outbound.publish("tail") as [ReadableStream]

        await expect(all(content)).rejects.toThrow("Disk failed")
    })

    test("a stream is read once; an unknown reference is not available", async () => {

        const { server } = connected()
        const id = server.streams.export(lines(1).stream)

        expect((await server.app.request(`/stream/${id}`)).status).toBe(200)
        expect((await server.app.request(`/stream/${id}`)).status).toBe(404)
        expect((await server.app.request("/stream/unknown")).status).toBe(404)
    })

    test("frames survive being cut anywhere in transit", async () => {

        const frames = [encodeFrame(serialize, ["chunk", "a"]), encodeFrame(serialize, ["chunk", { b: 2 }]), encodeFrame(serialize, ["end"])]
        const joined = new Uint8Array(frames.reduce((sum, frame) => sum + frame.byteLength, 0))
        let offset = 0

        for (const frame of frames) { joined.set(frame, offset); offset += frame.byteLength }

        // Deliver the bytes one at a time.
        const bytes = new ReadableStream<Uint8Array>({ start(controller) { for (const byte of joined) controller.enqueue(new Uint8Array([byte])); controller.close() } })

        expect(await all(decodeFrames(deserialize, bytes))).toEqual([["chunk", "a"], ["chunk", { b: 2 }], ["end"]])
    })
})
