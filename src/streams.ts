import type { Bytes, Deserialize, Serialize, StreamReferences } from "@the-link/core"
import type { Context } from "hono"
import { v4 as uuidv4 } from "uuid"

/**
 * Streams between HttpServer and HttpClient.
 *
 * Messages carry only stream references; the chunks travel over their own
 * HTTP requests, so a large stream never delays the messages on the WebSocket:
 *
 * - a stream the server sends is read by the client with GET `/stream/:id`,
 *   whose response body carries the chunks as frames;
 * - a stream the client sends is written with one POST `/stream/:id` per
 *   frame, and the server answers each POST only once that chunk is taken, so
 *   the client never sends further than the server reads.
 *
 * Each frame is a 4-byte big-endian length followed by the codec's encoding of
 * `["chunk", value]`, `["end"]`, or `["error", message]`.
 */

type Frame = readonly ["chunk", unknown] | readonly ["end"] | readonly ["error", string]

/** Milliseconds a reference may wait for its reader before it is released. */
const idle = 60_000

/** Largest byte chunk carried in one frame; larger byte chunks are split. */
const chunkBytes = 1024 * 1024

export function encodeFrame(serialize: Serialize, frame: Frame): Bytes {

    const body = serialize(frame)
    const bytes = new Uint8Array(4 + body.byteLength)

    new DataView(bytes.buffer).setUint32(0, body.byteLength)
    bytes.set(body, 4)

    return bytes
}

/** Splits a byte stream into frames, however its bytes were cut in transit. */
export function decodeFrames(deserialize: Deserialize, source: ReadableStream<Uint8Array>): ReadableStream<Frame> {

    let buffer = new Uint8Array(0)
    const reader = source.getReader()

    return new ReadableStream<Frame>({

        async pull(controller) {

            for (;;) {

                if (buffer.byteLength >= 4) {

                    const length = new DataView(buffer.buffer, buffer.byteOffset, 4).getUint32(0)

                    if (buffer.byteLength >= 4 + length) {

                        const body = buffer.slice(4, 4 + length)

                        buffer = buffer.slice(4 + length)
                        controller.enqueue(deserialize(body) as Frame)

                        return
                    }
                }

                const { done, value } = await reader.read()

                if (done) {

                    if (buffer.byteLength) controller.error(new Error("The stream ended inside a frame"))
                    else controller.close()

                    return
                }

                const joined = new Uint8Array(buffer.byteLength + value.byteLength)

                joined.set(buffer)
                joined.set(value, buffer.byteLength)
                buffer = joined
            }
        },

        cancel(reason) { return reader.cancel(reason) }
    })
}

/** Reads a source chunk by chunk, splitting large byte chunks; canceling it cancels the source. */
function chunksOf(source: ReadableStream<unknown>) {

    const reader = source.getReader()
    let pending: Uint8Array | null = null

    return {

        async read(): Promise<{ done: true } | { done: false, value: unknown }> {

            let value: unknown = pending

            if (value === null) {

                const result = await reader.read()

                if (result.done) return { done: true }

                value = result.value
            }

            if (value instanceof Uint8Array && value.byteLength > chunkBytes) {

                pending = value.subarray(chunkBytes)

                return { done: false, value: value.subarray(0, chunkBytes) }
            }

            pending = null

            return { done: false, value }
        },

        cancel: (cause: unknown) => reader.cancel(cause).catch(() => undefined)
    }
}

function reason(exception: unknown, fallback: string) {

    return exception instanceof Error ? exception.message : typeof exception === "string" && exception ? exception : fallback
}

interface Incoming {
    controller: ReadableStreamDefaultController<unknown> | null
    frames: { frame: Frame, taken: () => void }[]
    canceled: string | null
    timer: ReturnType<typeof setTimeout>
}

/**
 * The server's side: references for streams it sends, and streams for references clients send.
 */
export class ServerStreams implements StreamReferences {

    private readonly outgoing = new Map<string, { stream: ReadableStream<unknown>, timer: ReturnType<typeof setTimeout> }>()

    private readonly incoming = new Map<string, Incoming>()

    public constructor(private readonly codec: () => { serialize: Serialize, deserialize: Deserialize }) { }

    public export(stream: ReadableStream<unknown>): string {

        const id = uuidv4()
        const timer = setTimeout(() => {

            this.outgoing.delete(id)
            stream.cancel(new Error("The stream was not read in time")).catch(() => undefined)

        }, idle)

        this.outgoing.set(id, { stream, timer })

        return id
    }

    public import(id: string): ReadableStream<unknown> {

        const entry = this.incomingEntry(id)

        if (entry.controller) throw new Error("The stream reference was already imported")

        return new ReadableStream<unknown>({

            start: controller => { entry.controller = controller },

            pull: () => this.deliver(id, entry),

            cancel: cause => {

                entry.canceled = reason(cause, "The stream was canceled")
                clearTimeout(entry.timer)

                // Pending writes learn at once; later ones find the stream gone.
                for (const { taken } of entry.frames.splice(0)) taken()

                setTimeout(() => this.incoming.delete(id), idle)
            }
        }, new CountQueuingStrategy({ highWaterMark: 1 }))
    }

    /** GET `/stream/:id`: the body carries a stream this server sent. */
    public async read(context: Context) {

        const id = context.req.param("id") ?? ""
        const entry = this.outgoing.get(id)

        if (!entry) return context.text("The stream is not available", 404)

        this.outgoing.delete(id)
        clearTimeout(entry.timer)

        const { serialize } = this.codec()
        const chunks = chunksOf(entry.stream)

        const body = new ReadableStream<Uint8Array>({

            async pull(controller) {

                try {

                    const next = await chunks.read()

                    if (next.done) {

                        controller.enqueue(encodeFrame(serialize, ["end"]))
                        controller.close()

                    } else {

                        controller.enqueue(encodeFrame(serialize, ["chunk", next.value]))
                    }

                } catch (exception) {

                    controller.enqueue(encodeFrame(serialize, ["error", reason(exception, "The stream failed")]))
                    controller.close()
                }
            },

            // The client stopped reading: stop the source.
            cancel: cause => chunks.cancel(cause)
        })

        return new Response(body, { headers: { "cache-control": "no-store", "content-type": "application/octet-stream" } })
    }

    /** POST `/stream/:id`: one frame of a stream a client sends; answered once the chunk is taken. */
    public async write(context: Context) {

        const id = context.req.param("id") ?? ""
        const entry = this.incomingEntry(id)

        if (entry.canceled) return context.text(entry.canceled, 410)

        let frame: Frame

        try {

            frame = this.codec().deserialize(new Uint8Array(await context.req.arrayBuffer())) as Frame

        } catch {

            return context.text("The stream frame is invalid", 400)
        }

        await new Promise<void>(taken => {

            entry.frames.push({ frame, taken })
            this.deliver(id, entry)
        })

        return entry.canceled ? context.text(entry.canceled, 410) : context.body(null, 204)
    }

    private incomingEntry(id: string) {

        let entry = this.incoming.get(id)

        if (!entry) {

            // A frame may arrive before the message that carries its reference: it waits for its reader.
            entry = { controller: null, frames: [], canceled: null, timer: setTimeout(() => this.expire(id), idle) }
            this.incoming.set(id, entry)
        }

        return entry
    }

    private expire(id: string) {

        const entry = this.incoming.get(id)

        if (!entry) return

        this.incoming.delete(id)
        entry.canceled = "The stream was not read in time"

        for (const { taken } of entry.frames.splice(0)) taken()
    }

    /** Hands the next waiting frame to the reader, when it wants one. */
    private deliver(id: string, entry: Incoming) {

        const controller = entry.controller

        if (!controller || (controller.desiredSize ?? 0) <= 0) return

        const next = entry.frames.shift()

        if (!next) return

        const [kind] = next.frame

        if (kind === "chunk") controller.enqueue(next.frame[1])
        else {

            clearTimeout(entry.timer)
            this.incoming.delete(id)

            if (kind === "end") controller.close()
            else controller.error(new Error(next.frame[1]))
        }

        next.taken()
    }
}

/**
 * The client's side: references for streams it sends, and streams for references the server sends.
 */
export class ClientStreams implements StreamReferences {

    public constructor(private readonly url: string, private readonly codec: () => { serialize: Serialize, deserialize: Deserialize }) { }

    public export(stream: ReadableStream<unknown>): string {

        const id = uuidv4()

        // The frames start at once; the server holds each POST until it reads that chunk.
        void this.send(id, stream)

        return id
    }

    public import(id: string): ReadableStream<unknown> {

        const url = `${this.url}/stream/${encodeURIComponent(id)}`
        const aborter = new AbortController()
        let frames: ReadableStreamDefaultReader<Frame> | null = null

        return new ReadableStream<unknown>({

            pull: async controller => {

                if (!frames) {

                    const response = await fetch(url, { signal: aborter.signal })

                    if (!response.ok || !response.body) throw new Error(await response.text() || "The stream is not available")

                    frames = decodeFrames(this.codec().deserialize, response.body).getReader()
                }

                const { done, value } = await frames.read()

                if (done) throw new Error("The stream ended without its end")

                if (value[0] === "chunk") controller.enqueue(value[1])
                else if (value[0] === "end") controller.close()
                else controller.error(new Error(value[1]))
            },

            // Stop reading the body, which releases the server's source, and the request with it.
            cancel: async cause => {

                await frames?.cancel(cause).catch(() => undefined)
                aborter.abort(cause)
            }

        }, new CountQueuingStrategy({ highWaterMark: 1 }))
    }

    private async send(id: string, stream: ReadableStream<unknown>) {

        const url = `${this.url}/stream/${encodeURIComponent(id)}`
        const { serialize } = this.codec()
        const post = async (frame: Frame) => {

            const response = await fetch(url, { method: "POST", body: encodeFrame(serialize, frame).subarray(4) })

            if (response.status === 410) throw Object.assign(new Error(await response.text()), { canceled: true })

            if (!response.ok) throw new Error(await response.text())
        }

        const chunks = chunksOf(stream)

        try {

            for (;;) {

                const next = await chunks.read()

                if (next.done) break

                await post(["chunk", next.value])
            }

            await post(["end"])

        } catch (exception) {

            // The server stopped reading: release the source. Anything else fails the stream there.
            if ((exception as { canceled?: boolean }).canceled) await chunks.cancel(exception)
            else await post(["error", reason(exception, "The stream failed")]).catch(() => undefined)
        }
    }
}
