// Request-ID admission cost across independent sessions and reordered IDs.
import { SpentIds } from "../src/ietf/session.ts";

/**
 * Every ID in the window: in order, reversed (one skip to the top, then each fill
 * shortens it), or scattered (the top first, then every other ID descending, so each
 * splits the lowest skipped run and the remembered runs grow to half the window).
 */
function order(mode: string, window: number): bigint[] {
	const all = Array.from({ length: window }, (_, id) => BigInt(id * 2));
	if (mode === "ordered") return all;
	if (mode === "reordered") return all.reverse();
	const top = all[window - 1];
	const odd = all.filter((id, index) => index % 2 === 1 && id !== top).reverse();
	const even = all.filter((id, index) => index % 2 === 0 && id !== top).reverse();
	return [top, ...odd, ...even];
}

// No MAX_REQUEST_ID window bounds the IDs, yet skipped runs are remembered to refuse reuse.
console.log("mode,sessions,requests,admit_us,accepted");
for (const mode of ["ordered", "reordered", "scattered"]) {
	for (const sessions of [1, 8, 32]) {
		for (const requests of [8, 128, 220_000]) {
			const ids = order(mode, requests);
			const rounds = Math.max(1, Math.floor(8192 / requests));
			let elapsed = 0;
			let accepted = 0;
			for (let round = 0; round < rounds; round++) {
				const receivers = Array.from({ length: sessions }, () => new SpentIds(0n));
				const start = performance.now();
				for (const receiver of receivers) {
					for (const id of ids) {
						receiver.spend(id);
						accepted++;
					}
				}
				elapsed += performance.now() - start;
			}
			console.log(
				`${mode},${sessions},${requests},${((elapsed * 1000) / rounds).toFixed(2)},${accepted / rounds}`,
			);
		}
	}
}
