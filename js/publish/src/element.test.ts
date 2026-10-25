import { expect, mock, spyOn, test } from "bun:test";
import * as Moq from "@moq/net";

mock.module("./audio/capture-worklet.ts?worklet", () => ({ default: async () => "blob:capture" }));

mock.module("./video/capture-worker.ts?worklet", () => ({ default: async () => "blob:worker" }));

// Stand-ins for the DOM globals the element touches, installed until the returned restore runs.
// The first import binds the base class, so every test installs the same one.
function installDom(): () => void {
	const originalElement = globalThis.HTMLElement;
	const originalRegistry = globalThis.customElements;
	const originalObserver = globalThis.MutationObserver;
	const originalVideo = globalThis.HTMLVideoElement;
	const registry = new Map<string, CustomElementConstructor>();
	globalThis.HTMLElement = class {
		querySelector() {
			return null;
		}
	} as unknown as typeof HTMLElement;
	globalThis.MutationObserver = class {
		observe() {}
		disconnect() {}
	} as unknown as typeof MutationObserver;
	globalThis.HTMLVideoElement = class {} as unknown as typeof HTMLVideoElement;
	globalThis.customElements = {
		define: (name: string, ctor: CustomElementConstructor) => {
			registry.set(name, ctor);
		},
		get: (name: string) => registry.get(name),
	} as unknown as CustomElementRegistry;
	return () => {
		globalThis.HTMLElement = originalElement;
		globalThis.customElements = originalRegistry;
		globalThis.MutationObserver = originalObserver;
		globalThis.HTMLVideoElement = originalVideo;
	};
}

test("an invalid URL attribute warns and clears the publish URL", async () => {
	const restore = installDom();
	const set = mock(() => {});
	const warn = spyOn(console, "warn").mockImplementation(() => {});
	try {
		const { default: Element } = await import("./element");
		expect(() =>
			Element.prototype.attributeChangedCallback.call({ connection: { url: { set } } }, "url", null, "http://["),
		).not.toThrow();
		expect(set).toHaveBeenCalledWith(undefined);
		expect(warn).toHaveBeenCalledTimes(1);
	} finally {
		warn.mockRestore();
		restore();
	}
});

// A refused name idles the element rather than broadcasting at the root or the previous name.
test("an invalid name attribute idles the element until a valid one arrives", async () => {
	const restore = installDom();
	// Effects rerun in a later task, so each check waits one out.
	const settle = () => new Promise((resolve) => setTimeout(resolve, 0));
	try {
		const { default: Element } = await import("./element");
		const el = new Element();
		try {
			// Invalid markup, parsed before the element connects.
			expect(() => el.attributeChangedCallback("name", null, "/room")).toThrow();
			el.connectedCallback();
			await settle();
			expect(el.connection.enabled.peek()).toBe(false);

			el.attributeChangedCallback("name", "/room", "room");
			await settle();
			expect(el.connection.enabled.peek()).toBe(true);
			expect(el.name).toBe(Moq.Path.from("room"));

			expect(() => el.attributeChangedCallback("name", "room", "room//cam")).toThrow();
			await settle();
			expect(el.connection.enabled.peek()).toBe(false);

			el.attributeChangedCallback("name", "room//cam", "room/cam");
			await settle();
			expect(el.connection.enabled.peek()).toBe(true);
			expect(el.name).toBe(Moq.Path.from("room/cam"));
		} finally {
			el.signals.close();
		}
	} finally {
		restore();
	}
});
