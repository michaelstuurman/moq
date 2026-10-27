import { expect, jest, onTestFinished, test } from "bun:test";
import * as Connection from "../connection/index.ts";
import { createMockTransportPair, textFrame } from "../mock.ts";
import { Producer as OriginProducer } from "../origin.ts";
import * as Path from "../path.ts";
import { Reader, Stream, Writer } from "../stream.ts";
import { Tail } from "../tail.ts";
import { Timescale } from "../time.ts";
import * as track from "../track.ts";
import { wireOf } from "../wire.ts";
import { Fetch, FetchOk } from "./fetch.ts";
import { Join } from "./join.ts";
import { decodeFetchObject, FetchFrame, type GroupFlags, Group as GroupMessage } from "./object.ts";
import type { MessageLocation } from "./parameters.ts";
import { RequestError } from "./request.ts";
import { NativeSession } from "./session.ts";
import { Subscribe, SubscribeOk } from "./subscribe.ts";
import { Subscriber } from "./subscriber.ts";
import { ALPN, Version } from "./version.ts";

const VERSION = Version.DRAFT_18;
const ALIAS = 9n;
/** The group SUBSCRIBE_OK names as Largest in most cases below. */
const GROUP = 3;

const text = (s: string) => new TextEncoder().encode(s);
const untext = (b: Uint8Array | undefined) => (b ? new TextDecoder().decode(b) : undefined);

/** Accept the next bidi stream the subscriber opens, or undefined if none comes soon. */
async function nextStream(transport: WebTransport, wait = 200): Promise<Stream | undefined> {
	const reader =
		transport.incomingBidirectionalStreams.getReader() as ReadableStreamDefaultReader<WebTransportBidirectionalStream>;
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		const next = await Promise.race([
			reader.read(),
			new Promise<undefined>((resolve) => {
				timer = setTimeout(() => resolve(undefined), wait);
			}),
		]);
		if (!next || next.done) return undefined;
		return new Stream({ readable: next.value.readable, writable: next.value.writable, version: VERSION });
	} finally {
		clearTimeout(timer);
		reader.releaseLock();
	}
}

/**
 * A subscriber with one track answered with Largest `largest`, and the joining FETCH it
 * sent in response, still unanswered.
 */
async function join(
	largest: MessageLocation | null = { groupId: BigInt(GROUP), objectId: 1n },
	options?: track.Subscription,
) {
	const pair = createMockTransportPair(ALPN.DRAFT_18);
	const session = new NativeSession(pair.server, VERSION, true);
	const subscriber = new Subscriber({ session });
	const consumer = subscriber.consume(Path.from("room")).track("video").subscribe(options);
	onTestFinished(() => {
		consumer.close();
		session.close();
	});

	const peer = await nextStream(pair.client);
	if (!peer) throw new Error("the subscriber never subscribed");
	expect(await peer.reader.u53()).toBe(Subscribe.id);
	const subscribe = await Subscribe.decode(peer.reader, VERSION);
	await peer.writer.u53(SubscribeOk.id);
	await new SubscribeOk({ trackAlias: ALIAS, largest: largest ?? undefined }).encode(peer.writer, VERSION);

	const fetching = await nextStream(pair.client);
	let fetch: Fetch | undefined;
	if (fetching) {
		expect(await fetching.reader.u53()).toBe(Fetch.id);
		fetch = await Fetch.decode(fetching.reader, VERSION);
	}
	return { subscriber, consumer, subscribe, peer, fetching, fetch, client: pair.client };
}

/** The standalone FETCH a join falls back to, once its joining FETCH failed. */
async function fallback(client: WebTransport): Promise<{ stream: Stream; fetch: Fetch }> {
	const stream = await nextStream(client, 2_000);
	if (!stream) throw new Error("no standalone FETCH");
	expect(await stream.reader.u53()).toBe(Fetch.id);
	return { stream, fetch: await Fetch.decode(stream.reader, VERSION) };
}

async function accept(fetching: Stream | undefined) {
	if (!fetching) throw new Error("no joining FETCH");
	await fetching.writer.u53(FetchOk.id);
	await new FetchOk({ endOfTrack: false, endLocation: { group: BigInt(GROUP), object: 2n } }).encode(
		fetching.writer,
		VERSION,
	);
}

async function refuse(fetching: Stream | undefined) {
	if (!fetching) throw new Error("no joining FETCH");
	await fetching.writer.u53(RequestError.id);
	await new RequestError({ errorCode: 0x4, reasonPhrase: "not here" }).encode(fetching.writer, VERSION);
}

/** A fetch stream after its type: the header naming `requestId`, then `payloads` from object 0. */
async function head(requestId: bigint, payloads: string[], group = GROUP): Promise<Reader> {
	const bytes = await Writer.collect(VERSION, async (w) => {
		await w.u62(requestId);
		for (const [object, payload] of payloads.entries()) {
			await new FetchFrame({ payload: text(payload) }).encode(
				w,
				{ group, object, first: object === 0 },
				undefined,
				VERSION,
			);
		}
	});
	return new Reader(undefined, bytes, VERSION);
}

function flags(firstObject: boolean): GroupFlags {
	return {
		hasExtensions: false,
		hasSubgroup: false,
		hasSubgroupObject: false,
		hasEnd: true,
		hasPriority: true,
		firstObject,
	};
}

/** A live subgroup stream for `group` starting at Object ID `start`. */
function liveStream(group: number, start: number, payloads: string[]): [GroupMessage, Reader] {
	const bytes: number[] = [];
	for (const [i, payload] of payloads.entries()) {
		const p = text(payload);
		// Every field here is under 64, so each is a one-byte varint.
		bytes.push(i === 0 ? start : 0, p.byteLength, ...p);
	}
	const header = new GroupMessage({
		trackAlias: ALIAS,
		groupId: group,
		subGroupId: 0,
		publisherPriority: 0,
		flags: flags(start === 0),
	});
	return [header, new Reader(undefined, new Uint8Array(bytes), VERSION)];
}

async function frames(group: { readFrame(): Promise<{ payload: Uint8Array } | undefined> } | undefined) {
	const out: (string | undefined)[] = [];
	for (;;) {
		const frame = await group?.readFrame();
		if (!frame) return out;
		out.push(untext(frame.payload));
	}
}

test("a draft-18 join subscribes at Largest Object and fetches Largest's group", async () => {
	const { subscribe, fetch } = await join();
	expect(subscribe.filter).toEqual({ kind: "nextObject" });
	expect(fetch?.type).toEqual({ kind: "relativeJoining", subscription: subscribe.requestId, offset: 0n });
	expect(fetch?.groupOrder).toBe("ascending");
	expect(fetch?.requestId).not.toBe(subscribe.requestId);
});

test("an empty track sends no joining FETCH", async () => {
	const { fetching } = await join(null);
	expect(fetching).toBeUndefined();
});

test("a next-group join subscribes at Next Group Start and fetches nothing", async () => {
	const { subscriber, consumer, subscribe, fetching } = await join(undefined, { join: "next" });
	expect(subscribe.filter).toEqual({ kind: "relative", groups: 0n });
	expect(fetching).toBeUndefined();

	// The group in progress mid-stream is dropped as ever; the next one plays from object 0.
	await subscriber.handleGroup(...liveStream(GROUP, 2, ["t2"]));
	await subscriber.handleGroup(...liveStream(GROUP + 1, 0, ["n0", "n1"]));
	const group = await consumer.ordered().nextGroup();
	expect(group?.sequence).toBe(GROUP + 1);
	expect(await frames(group)).toEqual(["n0", "n1"]);
});

test("a next-group join skips the group in progress a lenient publisher sends whole", async () => {
	const { subscriber, consumer } = await join(undefined, { join: "next" });
	await subscriber.handleGroup(...liveStream(GROUP, 0, ["w0", "w1", "w2"]));
	await subscriber.handleGroup(...liveStream(GROUP + 1, 0, ["n0"]));
	const group = await consumer.ordered().nextGroup();
	expect(group?.sequence).toBe(GROUP + 1);
	expect(await frames(group)).toEqual(["n0"]);
});

test("a head that lands before its tail is continued by it", async () => {
	const { subscriber, consumer, fetching, fetch } = await join();
	if (!fetch) throw new Error("no fetch");
	await accept(fetching);

	await subscriber.handleFetch(await head(fetch.requestId, ["h0", "h1"]));
	await subscriber.handleGroup(...liveStream(GROUP, 2, ["t2", "t3"]));

	const group = await consumer.ordered().nextGroup();
	expect(group?.sequence).toBe(GROUP);
	expect(await frames(group)).toEqual(["h0", "h1", "t2", "t3"]);
});

test("a tail that lands before its head waits for it", async () => {
	const { subscriber, consumer, fetching, fetch } = await join();
	if (!fetch) throw new Error("no fetch");

	const tail = subscriber.handleGroup(...liveStream(GROUP, 2, ["t2"]));
	await accept(fetching);
	await subscriber.handleFetch(await head(fetch.requestId, ["h0", "h1"]));
	await tail;

	const group = await consumer.ordered().nextGroup();
	expect(await frames(group)).toEqual(["h0", "h1", "t2"]);
});

test("a catalog written once is readable from the head alone", async () => {
	const { subscriber, consumer, fetching, fetch } = await join({ groupId: 0n, objectId: 0n });
	if (!fetch) throw new Error("no fetch");
	await accept(fetching);

	// No live stream ever comes: the group was written before the subscription.
	await subscriber.handleFetch(await head(fetch.requestId, ['{"video":{}}'], 0));

	const group = await consumer.ordered().nextGroup();
	expect(group?.sequence).toBe(0);
	expect(untext((await group?.readFrame())?.payload)).toBe('{"video":{}}');
});

test("an empty tail finishes a group the head already holds whole", async () => {
	const { subscriber, consumer, fetching, fetch } = await join();
	if (!fetch) throw new Error("no fetch");
	await accept(fetching);

	await subscriber.handleFetch(await head(fetch.requestId, ["h0", "h1"]));
	const [header] = liveStream(GROUP, 2, []);
	header.flags = flags(false);
	await subscriber.handleGroup(header, new Reader(undefined, new Uint8Array(), VERSION));

	const group = await consumer.ordered().nextGroup();
	expect(await frames(group)).toEqual(["h0", "h1"]);
	expect(await group?.closed).toBeNull();
});

test("a joining FETCH a relay cannot resolve falls back to a standalone FETCH of the same range", async () => {
	const { subscriber, consumer, fetching, client } = await join();
	await refuse(fetching);

	const { stream, fetch } = await fallback(client);
	// Largest is {3, 1}: objects 0 and 1, spelled as draft 18 does (the last object plus one).
	expect(fetch.type).toEqual({
		kind: "standalone",
		trackNamespace: Path.from("room"),
		trackName: "video",
		start: { group: BigInt(GROUP), object: 0n },
		end: { group: BigInt(GROUP), object: 2n },
	});
	expect(fetch.groupOrder).toBe("ascending");
	await accept(stream);

	await subscriber.handleFetch(await head(fetch.requestId, ["h0", "h1"]));
	await subscriber.handleGroup(...liveStream(GROUP, 2, ["t2"]));

	const group = await consumer.ordered().nextGroup();
	expect(group?.sequence).toBe(GROUP);
	expect(await frames(group)).toEqual(["h0", "h1", "t2"]);
});

test("a tail parked on a refused joining FETCH is stitched from the standalone one", async () => {
	const { subscriber, consumer, fetching, client } = await join();

	const tail = subscriber.handleGroup(...liveStream(GROUP, 2, ["t2"]));
	await refuse(fetching);
	const { stream, fetch } = await fallback(client);
	await accept(stream);
	await subscriber.handleFetch(await head(fetch.requestId, ["h0", "h1"]));
	await tail;

	const group = await consumer.ordered().nextGroup();
	expect(await frames(group)).toEqual(["h0", "h1", "t2"]);
});

for (const outcome of ["both refused", "an empty joining FETCH", "an empty standalone FETCH"] as const) {
	test(`${outcome} costs only the group's head`, async () => {
		const { subscriber, consumer, fetching, fetch, client } = await join();
		if (!fetch) throw new Error("no fetch");
		if (outcome === "an empty joining FETCH") {
			// Answered, so there is nothing to repeat: no standalone FETCH follows.
			await accept(fetching);
			await subscriber.handleFetch(await head(fetch.requestId, []));
			expect(await nextStream(client)).toBeUndefined();
		} else {
			await refuse(fetching);
			const standalone = await fallback(client);
			if (outcome === "both refused") {
				await refuse(standalone.stream);
			} else {
				await accept(standalone.stream);
				await subscriber.handleFetch(await head(standalone.fetch.requestId, []));
			}
			// One retry, no loop.
			expect(await nextStream(client)).toBeUndefined();
		}

		// The mid-group tail has nothing to stitch onto, so it is dropped; the next group plays.
		await subscriber.handleGroup(...liveStream(GROUP, 2, ["t2"]));
		await subscriber.handleGroup(...liveStream(GROUP + 1, 0, ["n0"]));

		const group = await consumer.ordered().nextGroup();
		expect(group?.sequence).toBe(GROUP + 1);
		expect(await frames(group)).toEqual(["n0"]);
	});
}

test("unanswered joining and standalone FETCHes time out and cost only the head", async () => {
	jest.useFakeTimers();
	onTestFinished(() => jest.useRealTimers());
	const { subscriber, consumer, client } = await join();

	const tail = subscriber.handleGroup(...liveStream(GROUP, 2, ["t2"]));
	jest.advanceTimersByTime(10_001);
	await fallback(client);
	jest.advanceTimersByTime(10_001);
	await tail;
	await subscriber.handleGroup(...liveStream(GROUP + 1, 0, ["n0"]));

	const group = await consumer.ordered().nextGroup();
	expect(group?.sequence).toBe(GROUP + 1);
});

test("a whole group that precedes the head wins the sequence", async () => {
	const { subscriber, consumer, fetching, fetch } = await join();
	if (!fetch) throw new Error("no fetch");
	await accept(fetching);

	await subscriber.handleGroup(...liveStream(GROUP, 0, ["w0", "w1", "w2"]));
	await subscriber.handleFetch(await head(fetch.requestId, ["h0", "h1"]));

	const group = await consumer.ordered().nextGroup();
	expect(await frames(group)).toEqual(["w0", "w1", "w2"]);
});

test("a whole group for a headed sequence is refused, with its head", async () => {
	const { subscriber, consumer, fetching, fetch } = await join();
	if (!fetch) throw new Error("no fetch");
	await accept(fetching);

	await subscriber.handleFetch(await head(fetch.requestId, ["h0", "h1"]));
	await subscriber.handleGroup(...liveStream(GROUP, 0, ["w0"]));

	const group = await consumer.ordered().nextGroup();
	expect(group?.sequence).toBe(GROUP);
	expect(await group?.closed).toBeInstanceOf(Error);
});

test("a head that does not meet its tail is refused", async () => {
	const { subscriber, consumer, fetching, fetch } = await join();
	if (!fetch) throw new Error("no fetch");
	await accept(fetching);

	await subscriber.handleFetch(await head(fetch.requestId, ["h0", "h1"]));
	// Object 2 is missing.
	await subscriber.handleGroup(...liveStream(GROUP, 3, ["t3"]));

	const group = await consumer.ordered().nextGroup();
	expect(await group?.closed).toBeInstanceOf(Error);
});

test("a joining FETCH spanning another group drops the head", async () => {
	const { subscriber, consumer, fetching, fetch } = await join();
	if (!fetch) throw new Error("no fetch");
	await accept(fetching);

	const bytes = await Writer.collect(VERSION, async (w) => {
		await w.u62(fetch.requestId);
		await new FetchFrame({ payload: text("h0") }).encode(
			w,
			{ group: GROUP, object: 0, first: true },
			undefined,
			VERSION,
		);
		await new FetchFrame({ payload: text("x0") }).encode(
			w,
			{ group: GROUP + 1, object: 0, first: false, previousGroup: GROUP },
			undefined,
			VERSION,
		);
	});
	await subscriber.handleFetch(new Reader(undefined, bytes, VERSION));

	const group = await consumer.ordered().nextGroup();
	expect(await group?.closed).toBeInstanceOf(Error);
});

test("a clean end finishes a head nothing claimed", () => {
	const t = new Tail();
	const j = new Join(0, t);
	const producer = new track.Producer("catalog").createGroup(0);
	expect(j.open(() => producer)).toBe(producer);
	producer.writeFrame({ payload: text("c") });
	j.wrote();
	j.finish();
	expect(j.outstanding).toBe(true);
	j.end();
	expect(producer.closed.peek()).toBeNull();
	expect(j.outstanding).toBe(false);
});

test("fetch objects decode as FetchFrame wrote them", async () => {
	const bytes = await Writer.collect(VERSION, async (w) => {
		await new FetchFrame({ payload: text("a") }).encode(
			w,
			{ group: 5, object: 0, first: true },
			undefined,
			VERSION,
		);
		await new FetchFrame({ payload: new Uint8Array() }).encode(
			w,
			{ group: 5, object: 1, first: false },
			undefined,
			VERSION,
		);
	});
	const reader = new Reader(undefined, bytes, VERSION);
	const first = await reader.decode((c) => decodeFetchObject(c, undefined));
	expect(first).toMatchObject({ kind: "object", group: 5, object: 0, priority: 0, subgroup: "zero" });
	const second = await reader.decode((c) => decodeFetchObject(c, undefined));
	expect(second).toMatchObject({ kind: "object", group: undefined, object: undefined, payload: new Uint8Array() });
});

// End to end against the JS publisher, which answers the joining FETCH from its cache. It
// would also replay the group to the old Absolute Start {0, 0} subscription, so the test checks
// the heads came through the FETCH.
async function publishing() {
	const origin = new OriginProducer();
	const pair = createMockTransportPair(ALPN.DRAFT_18);
	const url = new URL("https://localhost:4443/test");
	const [client, server] = await Promise.all([
		Connection.connect({ url, transport: pair.client }),
		Connection.accept({ transport: pair.server, url, publish: origin.consume() }),
	]);
	onTestFinished(() => {
		client.abort();
		server.abort();
	});

	const publication = origin.createBroadcast(Path.from("room"));
	publication.announce();
	const broadcast = publication.broadcast;
	// Written once, before anyone subscribed, and left open: only the FETCH can deliver it.
	const catalog = broadcast.createTrack("catalog.json", { timescale: Timescale.MILLI });
	catalog.appendGroup().writeFrame(textFrame("catalog"));
	const video = broadcast.createTrack("video", { timescale: Timescale.MILLI });
	const group = video.appendGroup();
	group.writeFrame(textFrame("v0"));
	group.writeFrame(textFrame("v1"));

	const sent = jest.spyOn(Fetch.prototype, "encode");
	onTestFinished(() => sent.mockRestore());
	return { remote: wireOf(client).consume(Path.from("room")), video, group, sent };
}

test("a late JS subscriber fetches a JS publisher's current groups from object 0", async () => {
	const { remote, group, sent } = await publishing();
	const catalogGroup = await remote.track("catalog.json").subscribe().recvGroup();
	expect(catalogGroup?.sequence).toBe(0);
	expect(untext((await catalogGroup?.readFrame())?.payload)).toBe("catalog");

	const videoGroup = await remote.track("video").subscribe().recvGroup();
	expect(videoGroup?.sequence).toBe(0);
	group.writeFrame(textFrame("v2"));
	group.finish();
	expect(await frames(videoGroup)).toEqual(["v0", "v1", "v2"]);
	expect(sent.mock.calls).toHaveLength(2);
});

test("a late JS subscriber joins JS media at the next group, and still fetches the catalog", async () => {
	const { remote, video, group, sent } = await publishing();
	const catalogGroup = await remote.track("catalog.json").subscribe().recvGroup();
	expect(untext((await catalogGroup?.readFrame())?.payload)).toBe("catalog");

	const subscriber = remote.track("video").subscribe({ join: "next" });
	// The JS publisher ignores Next Group Start on draft 18 and sends group 0 whole anyway.
	const waiting = subscriber.recvGroup();
	await new Promise((resolve) => setTimeout(resolve, 20));
	group.writeFrame(textFrame("v2"));
	group.finish();
	const next = video.appendGroup();
	next.writeFrame(textFrame("n0"));
	next.finish();
	const videoGroup = await waiting;
	expect(videoGroup?.sequence).toBe(1);
	expect(await frames(videoGroup)).toEqual(["n0"]);
	// Only the catalog's.
	expect(sent.mock.calls).toHaveLength(1);
});
