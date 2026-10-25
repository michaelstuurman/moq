import { expect, mock, spyOn, test } from "bun:test";
import * as Moq from "@moq/net";

// Stand-ins for the DOM globals the element touches. Like a browser, setting an attribute to a new
// value calls back into `attributeChangedCallback`, so reflection loops are exercised.
const registry = new Map<string, CustomElementConstructor>();
globalThis.HTMLElement = class {
	readonly style = {};
	readonly #attributes = new Map<string, string>();

	setAttribute(name: string, value: string) {
		const old = this.#attributes.get(name) ?? null;
		this.#attributes.set(name, value);
		if (old !== value) this.attributeChangedCallback(name, old, value);
	}

	removeAttribute(name: string) {
		const old = this.#attributes.get(name) ?? null;
		this.#attributes.delete(name);
		if (old !== null) this.attributeChangedCallback(name, old, null);
	}

	attributeChangedCallback(_name: string, _old: string | null, _value: string | null) {}
	querySelector() {
		return null;
	}
	appendChild() {}
	getBoundingClientRect() {
		return { width: 0, height: 0 };
	}
} as unknown as typeof HTMLElement;
globalThis.MutationObserver = class {
	observe() {}
	disconnect() {}
} as unknown as typeof MutationObserver;
globalThis.ResizeObserver = class {
	observe() {}
	disconnect() {}
} as unknown as typeof ResizeObserver;
globalThis.document = { createElement: () => ({ style: {} }) } as unknown as Document;
globalThis.customElements = {
	define: (name: string, ctor: CustomElementConstructor) => {
		registry.set(name, ctor);
	},
	get: (name: string) => registry.get(name),
} as unknown as CustomElementRegistry;

// The audio worklet is inlined by a bundler plugin, which the test runner has no equivalent for.
mock.module("./audio/render-worklet.ts?worklet", () => ({ default: async () => "blob:worklet" }));

// A framework binds the raw node (Svelte's `bind:this`, React's ref) and reads properties from it.
// Until the browser upgrades that node, every field the class sets is absent, so `el.broadcast` is
// undefined. Registering while the entrypoint evaluates is what makes the upgrade land first: any
// page that imports it before mounting can only ever see an upgraded element.
//
// `require` finishes the module graph synchronously, so a registration deferred to a microtask, an
// event, or a nested dynamic import leaves the registry empty here.
test("importing the entrypoint registers <moq-watch> before the import finishes", () => {
	require("./element");
	expect(registry.get("moq-watch")).toBeDefined();
});

for (const [attribute, value, expected] of [
	["url", "http://[", undefined],
	["volume", "foo", 0.5],
	["volume", "Infinity", 0.5],
	["volume", "1oops", 0.5],
	["volume", "0x10", 0.5],
	["volume", "-0.1", 0.5],
] as const) {
	test(`invalid ${attribute}=${value} warns and uses the default`, () => {
		const Element = require("./element").default;
		const set = mock(() => {});
		const target = { connection: { url: { set } }, controls: { audio: { volume: { set } } } };
		const warn = spyOn(console, "warn").mockImplementation(() => {});
		try {
			expect(() => Element.prototype.attributeChangedCallback.call(target, attribute, null, value)).not.toThrow();
			expect(set).toHaveBeenCalledWith(expected);
			expect(warn).toHaveBeenCalledTimes(1);
		} finally {
			warn.mockRestore();
		}
	});
}

for (const [value, expected] of [
	["moq-transport-14", ["moq-transport-14"]],
	[" moq-lite-06 , moq-transport-22 ", ["moq-lite-06", "moq-transport-22"]],
	["", undefined],
	[" , ", undefined],
	[null, undefined],
] as const) {
	test(`versions=${JSON.stringify(value)} offers ${JSON.stringify(expected)}`, () => {
		const Element = require("./element").default;
		const set = mock(() => {});
		Element.prototype.attributeChangedCallback.call({ connection: { versions: { set } } }, "versions", "x", value);
		expect(set).toHaveBeenCalledWith(expected);
	});
}

// A refused name idles the element rather than watching the root or the previous name.
test("an invalid name attribute idles the element until a valid one arrives", async () => {
	const Element = require("./element").default;
	// Effects rerun in a later task, so each check waits one out.
	const settle = () => new Promise((resolve) => setTimeout(resolve, 0));
	const el = new Element();
	try {
		// Invalid markup, parsed before the element connects and before reflection first runs.
		expect(() => el.setAttribute("name", "/room")).toThrow();
		el.connectedCallback();
		await settle();
		expect(el.connection.enabled.peek()).toBe(false);

		el.setAttribute("name", "room");
		await settle();
		expect(el.connection.enabled.peek()).toBe(true);
		expect(el.name).toBe(Moq.Path.from("room"));

		expect(() => el.setAttribute("name", "room//cam")).toThrow();
		await settle();
		expect(el.connection.enabled.peek()).toBe(false);

		el.setAttribute("name", "room/cam");
		await settle();
		expect(el.connection.enabled.peek()).toBe(true);
		expect(el.name).toBe(Moq.Path.from("room/cam"));
	} finally {
		el.signals.close();
	}
});
