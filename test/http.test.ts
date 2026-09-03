import { describe, expect, test } from "bun:test"
import { HttpServer } from "../src/server.js"

describe("HTTP adapter", () => {

    test("publishes through the HTTP boundary", async () => {

        const server = new HttpServer()

        server.$inbound.subscribe("sum", (left: number, right: number) => left + right)
        server.prepareConnection()

        const response = await server.app.request("/publish", {
            method: "POST",
            body: JSON.stringify(["sum", 20, 22])
        })

        expect(response.status).toBe(200)
        expect(await response.json()).toEqual([42])
    })

    test("returns publication failures", async () => {

        const server = new HttpServer()

        server.enableDebugging()
        server.$inbound.subscribe("fail", () => { throw new TypeError("Expected failure") })
        server.prepareConnection()

        const response = await server.app.request("/publish", {
            method: "POST",
            body: JSON.stringify(["fail"])
        })

        expect(response.status).toBe(500)
        expect(await response.text()).toBe("Expected failure")
    })

    test("uses the serialization policy selected by the application", async () => {

        const server = new HttpServer()
        const encoder = new TextEncoder()
        const decoder = new TextDecoder()
        const serialize = (value: unknown) => Uint8Array.from(encoder.encode(JSON.stringify(value))).reverse()
        const deserialize = (bytes: Uint8Array) => JSON.parse(decoder.decode(Uint8Array.from(bytes).reverse()))

        server.setSerialize(serialize)
        server.setDeserialize(deserialize)
        server.$inbound.subscribe("value", (value: number) => value)
        server.prepareConnection()

        const response = await server.app.request("/publish", {
            method: "POST",
            body: serialize(["value", 42])
        })

        expect(deserialize(new Uint8Array(await response.arrayBuffer()))).toEqual([42])
    })
})
