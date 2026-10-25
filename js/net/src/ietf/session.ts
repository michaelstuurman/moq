import { SessionCode, SessionError } from "../error.ts";
import { type OpenOptions, Stream } from "../stream.ts";
import type { IetfVersion } from "./version.ts";

/**
 * The most runs of skipped request IDs a peer may leave before the session closes.
 *
 * Each request holds a stream, so stream credit bounds the live ones, but a skipped ID stays
 * remembered after the requests around it end. Forgetting one would make it usable again, so
 * a peer that keeps skipping loses the session instead. Matches moq-net.
 */
export const REQUEST_HISTORY = 240_000;

/** Opens request streams and allocates request IDs. Implemented by {@link NativeSession}; tests substitute their own. */
export interface Session {
	openBi(options?: Partial<OpenOptions>): Stream | Promise<Stream>;
	acceptBi(): Promise<Stream | undefined>;
	nextRequestId(signal?: AbortSignal): Promise<bigint | undefined>;
	/** Spend a request ID the peer used without holding a request open, closing the session if it cannot. */
	spendRequest(id: bigint): void;
	close(): void;
	readonly version: IetfVersion;
	/** Whether we opened the session. */
	readonly client: boolean;
}

/** A thin wrapper around WebTransport: each request gets its own bidi stream. */
export class NativeSession implements Session {
	#quic: WebTransport;
	// moq-transport reserves even request IDs for the client and odd for the server,
	// so the two peers' ID spaces never overlap.
	#requestId: bigint;
	// The peer's IDs, of the other parity. No window bounds them, yet an ID stays spent.
	#spent: SpentIds;
	readonly version: IetfVersion;
	readonly client: boolean;

	constructor(quic: WebTransport, version: IetfVersion, client: boolean) {
		this.#quic = quic;
		this.version = version;
		this.client = client;
		this.#requestId = client ? 0n : 1n;
		this.#spent = new SpentIds(client ? 1n : 0n);
	}

	spendRequest(id: bigint): void {
		try {
			this.#spent.spend(id);
		} catch (err) {
			if (err instanceof SessionError) this.#quic.close({ closeCode: err.code, reason: err.message });
			throw err;
		}
	}

	async openBi(options?: Partial<OpenOptions>): Promise<Stream> {
		return Stream.open(this.#quic, { ...options, version: this.version });
	}

	async acceptBi(): Promise<Stream | undefined> {
		return Stream.accept(this.#quic, this.version);
	}

	async nextRequestId(signal?: AbortSignal): Promise<bigint | undefined> {
		signal?.throwIfAborted();
		const id = this.#requestId;
		this.#requestId += 2n;
		return id;
	}

	/** Closes the underlying WebTransport session. */
	close() {
		this.#quic.close();
	}
}

/** Runs per chunk before it splits, so a split or removal shifts at most one chunk. */
const RUN_CHUNK = 256;

/**
 * Skipped request IDs as sorted, disjoint `[start, end)` runs of one parity.
 *
 * A jump costs one run however far it skips, and only an ID landing inside a run splits it,
 * so the runs never outnumber the requests received. They are kept in chunks so a split
 * costs the chunk it lands in rather than shifting the whole table.
 */
class MissingRuns {
	#chunks: [bigint, bigint][][] = [];
	#size = 0;

	/** How many runs are remembered. */
	get size(): number {
		return this.#size;
	}

	/** Whether `id` was skipped and not yet received. */
	has(id: bigint): boolean {
		return this.#find(id) !== undefined;
	}

	/** Whether taking `id` leaves one more run, landing strictly inside its run. */
	splits(id: bigint): boolean {
		const found = this.#find(id);
		if (!found) return false;
		const [start, end] = this.#chunks[found[0]][found[1]];
		return start < id && id + 2n < end;
	}

	/** Mark a skipped `id` received, splitting its run. */
	take(id: bigint): void {
		const found = this.#find(id);
		if (!found) return;
		const [index, run] = found;
		const chunk = this.#chunks[index];
		const [start, end] = chunk[run];
		const rest: [bigint, bigint][] = [];
		if (start < id) rest.push([start, id]);
		if (id + 2n < end) rest.push([id + 2n, end]);
		this.#size += rest.length - 1;
		chunk.splice(run, 1, ...rest);
		if (chunk.length === 0) this.#chunks.splice(index, 1);
		else if (chunk.length > 2 * RUN_CHUNK) this.#chunks.splice(index + 1, 0, chunk.splice(RUN_CHUNK));
	}

	/** Record `[start, end)` as skipped, above every existing run. */
	push(start: bigint, end: bigint): void {
		this.#size++;
		const last = this.#chunks.at(-1);
		if (last && last.length < 2 * RUN_CHUNK) last.push([start, end]);
		else this.#chunks.push([[start, end]]);
	}

	/** The chunk and run holding `id`. */
	#find(id: bigint): [number, number] | undefined {
		const index = lastAtOrBelow(this.#chunks, id, (chunk) => chunk[0][0]);
		if (index < 0) return undefined;
		const chunk = this.#chunks[index];
		const run = lastAtOrBelow(chunk, id, (run) => run[0]);
		return run >= 0 && id < chunk[run][1] ? [index, run] : undefined;
	}
}

/**
 * The peer's spent request IDs: every ID of its parity below `#next`, except the skipped runs.
 * Mirrors Rust's `Spent`.
 */
export class SpentIds {
	#next: bigint;
	#missing = new MissingRuns();
	// No window bounds the runs here, and forgetting one would make its IDs usable again.
	#limit: number;

	constructor(first: bigint, limit = REQUEST_HISTORY) {
		this.#next = first;
		this.#limit = limit;
	}

	/** Spend `id`, throwing the session error a reused, wrong-parity, or unrecordable ID closes with. */
	spend(id: bigint): void {
		// No peer reaches 2^62 requests; refusing past it matches Rust, whose `next` would overflow.
		const unused = id < 1n << 62n && (id >= this.#next || this.#missing.has(id));
		if (id % 2n !== this.#next % 2n || !unused) throw new SessionError(SessionCode.InvalidRequestId);
		const splits = id < this.#next ? this.#missing.splits(id) : id > this.#next;
		if (splits && this.#missing.size >= this.#limit) {
			throw new SessionError(SessionCode.Internal, { reason: "request ID history exhausted" });
		}
		if (id < this.#next) {
			this.#missing.take(id);
			return;
		}
		if (id > this.#next) this.#missing.push(this.#next, id);
		this.#next = id + 2n;
	}
}

/** The last index whose key is at or below `id`, or -1, in an array sorted by key. */
function lastAtOrBelow<T>(items: T[], id: bigint, key: (item: T) => bigint): number {
	let lo = 0;
	let hi = items.length;
	while (lo < hi) {
		const mid = (lo + hi) >>> 1;
		if (key(items[mid]) <= id) lo = mid + 1;
		else hi = mid;
	}
	return lo - 1;
}
