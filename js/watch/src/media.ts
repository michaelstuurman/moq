import type { Milli } from "@moq/clock";
import type { Container } from "@moq/hang";
import type * as Moq from "@moq/net";
import { Error as NetError } from "@moq/net";
import type { Effect, Getter } from "@moq/signals";

/**
 * Where media joins a track already under way: at the next group, so playback starts at the
 * live edge rather than up to a group behind it. The catalog and other tracks that describe
 * rather than play keep the default, the current group, which may be the only one for a long
 * time.
 *
 * @internal
 */
export const MEDIA_JOIN: Moq.Track.Join = "next";

/**
 * Open a media subscription with its max delay on the initial request and every update.
 *
 * @internal
 */
export function subscribeMedia(
	effect: Effect,
	props: {
		broadcast: Moq.Broadcast.Consumer;
		track: string;
		priority: number;
		maxDelay: Getter<Milli>;
	},
): Moq.Track.Subscriber | undefined {
	if (effect.get(props.broadcast.closed) !== undefined) return;
	const subscription = (maxDelay: Milli) => ({ priority: props.priority, maxDelay, join: MEDIA_JOIN });
	const subscriber = props.broadcast.track(props.track).subscribe(subscription(props.maxDelay.peek()));
	effect.cleanup(() => subscriber.close());

	effect.run((inner) => {
		subscriber.update(subscription(inner.get(props.maxDelay)));
	});

	return subscriber;
}

/** Read the next media frame, ending playback when its subscription is reset. @internal */
export async function nextMedia(consumer: Container.Consumer) {
	try {
		return await consumer.next();
	} catch (err) {
		if (!(err instanceof NetError.Stream)) throw err;
		// The subscription is over, even when other tracks on the session are still live.
		console.debug("media subscription ended", err);
		return undefined;
	}
}

/** The source a fatal decode failure belongs to. @internal */
export interface Failure {
	/** The catalog broadcast's instance. */
	broadcast: unknown;
	/** The instance of the broadcast the rendition reads. */
	media: unknown;
	track: string;
	identity: string;
}

/**
 * Whether the selection moved off a failed source. A rendition that is only unavailable (muted by
 * its publisher, or its broadcast override offline) keeps the failure, so it is not retried.
 *
 * @internal
 */
export function failureMoved(failure: Failure, selection: Partial<Failure>): boolean {
	if (selection.broadcast !== failure.broadcast) return true;
	if (selection.track === undefined || selection.identity === undefined) return false;
	if (selection.track !== failure.track || selection.identity !== failure.identity) return true;
	return selection.media !== undefined && selection.media !== failure.media;
}
