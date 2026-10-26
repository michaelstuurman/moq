//! One rendition's segments, as a `Producer`/[`Consumer`] pair.
//!
//! Each rendition lists its own HLS records (see [`hang::hls`]): record `n` is segment `n`, holding
//! the groups from its record's to the next record's. The rendition's record watcher appends a row
//! once the next record bounds it. Two things read the window:
//!
//! * the HTTP serve path, synchronously, to render a media playlist and resolve a segment's
//!   groups (nothing here touches media bytes on that path); and
//! * a [`Consumer`] cursor, for a recorder that wants every segment *with its media*, in
//!   order, exactly once. `next()` waits for the next row, FETCHes and transmuxes its groups
//!   (via [`Rendition`]), and yields the CMAF bytes.
//!
//! A segment is addressed by its record track's tag and its number (the `seg/{tag}.{segment}.m4s`
//! URI); the number alone is its `EXT-X-MEDIA-SEQUENCE` and the recorder cursor's position. A
//! publisher that aligns its renditions numbers the same span of content time alike on every
//! rendition, so the number means the same on every rendition, edge, and reload.

use moq_clock::Wall;
use std::collections::VecDeque;
use std::sync::Arc;
use std::task::{Poll, ready};
use std::time::Duration;

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::Rendition;
use crate::Result;

/// A segment as its URL names it: `seg/[{generation}.]{tag}.{sequence}.m4s`, or `t{pts}` in
/// place of the sequence for the DASH `$Time$` form. Parsed by
/// [`Route::parse`](super::Route::parse) and answered by [`Rendition::segment`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Id {
	/// The publisher run the URL names, if the broadcaster carries one.
	pub generation: Option<String>,
	/// A short hash of the name of the record track numbering the segment.
	pub tag: String,
	/// Where the segment sits in that numbering.
	pub at: At,
}

/// Where a segment sits in its record track's numbering: two names for the same bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum At {
	/// The segment's number, as HLS playlists address it.
	Sequence(u64),
	/// The segment's start in milliseconds, as the DASH manifest addresses it.
	Pts(u64),
}

impl Id {
	/// Parse a segment file name without its `.m4s` extension.
	pub(crate) fn parse(stem: &str) -> Option<Self> {
		let (generation, tag, at) = match stem.split('.').collect::<Vec<_>>().as_slice() {
			[tag, at] => (None, tag.to_string(), *at),
			[generation, tag, at] => (Some(generation.to_string()), tag.to_string(), *at),
			_ => return None,
		};
		let at = match at.strip_prefix('t') {
			Some(pts) => At::Pts(pts.parse().ok()?),
			None => At::Sequence(at.parse().ok()?),
		};
		Some(Self { generation, tag, at })
	}
}

/// The producing side of a rendition's segment window.
///
/// The record watcher appends rows via [`push`](Self::push) and marks the stream
/// [`end`](Self::end)ed. Cheap to share behind an `Arc`; the window state lives in a
/// [`kio::Producer`] so a [`Consumer`] can await changes without a separate signal.
pub(crate) struct Producer {
	state: kio::Producer<State>,
}

struct State {
	/// Rows within the window, oldest first. Every row is a complete segment.
	rows: VecDeque<Row>,
	/// The first listed segment, or the segment that would follow an empty trimmed window.
	sequence: u64,
	/// The newest discontinuity seen, retained when the playlist window becomes empty.
	discontinuity: u64,
	/// The record track ended: the rendition is over (`EXT-X-ENDLIST`).
	ended: bool,
	/// The record track or the catalog failed, so every listing refuses with this error.
	failed: Option<crate::Error>,
	/// Bumped whenever the window is cleared, so a cursor can tell its position is stale.
	resets: u64,
	/// Total rows appended, so a cursor can detect eviction even before its first read.
	produced: u64,
}

/// Numbers the broadcast's content breaks: segments after a stop, or after records nobody saw,
/// start a new discontinuity.
///
/// Held once per broadcaster and keyed by segment number, so every rendition marks segment `n`
/// with the same sequence however many segments each one listed: the first rendition to stamp a
/// segment decides it.
#[derive(Default)]
pub(crate) struct Discontinuities {
	/// The sequence stamped on each recent segment.
	stamped: std::collections::BTreeMap<u64, u64>,
	/// The highest sequence stamped so far.
	sequence: u64,
}

impl Discontinuities {
	/// The sequence of segment `segment`, which starts a new one when `broken`.
	pub fn stamp(&mut self, segment: u64, broken: bool) -> u64 {
		if let Some(sequence) = self.stamped.get(&segment) {
			return *sequence;
		}
		let previous = self
			.stamped
			.range(..segment)
			.next_back()
			.map_or(self.sequence, |(_, sequence)| *sequence);
		let sequence = match broken {
			true => previous.max(self.sequence) + 1,
			false => previous,
		};
		self.sequence = self.sequence.max(sequence);
		self.stamped.insert(segment, sequence);
		// Every rendition lists at most a window's worth, so older stamps are never asked for.
		while self.stamped.len() > 4 * MAX_SEGMENTS {
			self.stamped.pop_first();
		}
		sequence
	}

	/// Forget every stamp, for a publisher run that restarts its numbering.
	pub fn restart(&mut self) {
		self.stamped.clear();
		self.sequence += 1;
	}
}

/// The most segments a playlist window lists, however short they are.
///
/// A fresh subscriber to a `moq-mux` record track is only promised this many recent records (the
/// checkpoint each record group restates), so an edge that joined long ago must not list more
/// records than one joining now. The dense-records test fails if the two drift apart.
pub(crate) const MAX_SEGMENTS: usize = 256;

/// Whether a window of `len` rows, whose rows after the oldest span `rest`, should drop its
/// oldest: it lists more than [`MAX_SEGMENTS`], or the rest still covers `window`.
pub(crate) fn evicts(window: Duration, len: usize, rest: Duration) -> bool {
	len > MAX_SEGMENTS || (len >= 2 && rest >= window)
}

/// The URI tag naming the segments one config muxes from a record track: a hash of the track name
/// and the normalized `config`, so every edge derives the same one and it never contains a URI
/// separator. A reconfigured rendition keeps its record track but muxes it differently, so its
/// segments get new URLs rather than new bytes under the old ones.
pub(crate) fn tag(track: &str, config: &[u8]) -> String {
	let hash = Sha256::new()
		.chain_update(track)
		.chain_update([0])
		.chain_update(config)
		.finalize();
	// 32 bits: the tag only has to tell apart the configs one rendition name ever carries.
	hash[..4].iter().map(|b| format!("{b:02x}")).collect()
}

/// One playlist segment: its number, timing, and the groups it holds.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Row {
	/// The record's window index, used to mirror exact window trims.
	pub index: u64,
	/// The record's sequence: its `EXT-X-MEDIA-SEQUENCE` and, with the tag, its URI
	/// (`seg/{tag}.{segment}.m4s`).
	pub segment: u64,
	/// The [tag](tag) of the record track numbering the segment.
	pub tag: Arc<str>,
	/// The groups the segment holds: from its record's group up to the next record's.
	pub groups: std::ops::Range<u64>,
	/// Presentation duration.
	pub duration: Duration,
	/// The segment's starting presentation timestamp.
	pub pts: moq_net::Timestamp,
	/// The segment's ending presentation timestamp (`pts + duration`), for window eviction.
	pub end: Duration,
	/// The broadcast's [`Discontinuities`] sequence for this segment.
	pub discontinuity: u64,
	/// Content stopped here: a zero-duration record holding no media, listed as a gap so the
	/// numbering stays consecutive.
	pub stop: bool,
}

/// A consistent read of the window, for rendering one playlist (the serve path only).
#[cfg_attr(
	not(feature = "server"),
	allow(dead_code, reason = "only the `server` feature uses it")
)]
pub(crate) struct Window {
	/// The `EXT-X-MEDIA-SEQUENCE` of the first listed segment.
	pub sequence: u64,
	/// The first listed discontinuity, or the newest seen when the window is empty.
	pub discontinuity: u64,
	/// Listed segments, oldest first.
	pub segments: Vec<Row>,
	/// Whether the record track (and so the playlist) has ended.
	pub ended: bool,
	/// Why the window failed, if it did.
	pub failed: Option<crate::Error>,
}

/// The next segment a [`Consumer`] should emit, resolved from the window.
enum Next {
	/// A segment is ready to fetch.
	Ready(Row),
	/// No further segment will ever appear (the record track ended or failed).
	Done,
	/// Nothing new yet; wait for the next window change.
	Pending,
}

impl State {
	/// Snapshot the current window. Every row is a complete segment, so all are listed.
	#[cfg_attr(
		not(feature = "server"),
		allow(dead_code, reason = "only the `server` feature uses it")
	)]
	fn window(&self) -> Window {
		Window {
			sequence: self.sequence,
			discontinuity: self.rows.front().map_or(self.discontinuity, |row| row.discontinuity),
			segments: self.rows.iter().cloned().collect(),
			ended: self.ended,
			failed: self.failed.clone(),
		}
	}

	/// The row at a cursor's append position, refusing any lost prefix or reset.
	fn next_at(&self, position: (u64, u64)) -> Result<Next> {
		let first = self.produced - self.rows.len() as u64;
		if position.0 != self.resets || position.1 < first {
			return Err(crate::Error::RecordingLagged);
		}
		Ok(match self.rows.get((position.1 - first) as usize) {
			Some(row) => Next::Ready(row.clone()),
			None if self.ended || self.failed.is_some() => Next::Done,
			None => Next::Pending,
		})
	}
}

impl Producer {
	/// An empty window whose first segment is numbered `start` or later: those before it belong
	/// to another window, which [`numbered`](Self::numbered) reports as never listed here.
	pub fn new(start: u64) -> Self {
		Self {
			state: kio::Producer::new(State {
				rows: VecDeque::new(),
				sequence: start,
				discontinuity: 0,
				ended: false,
				failed: None,
				resets: 0,
				produced: 0,
			}),
		}
	}

	/// Append a row, evicting the front of the window past `window` (see [`evicts`]).
	pub fn push(&self, row: Row, window: Duration) {
		let Ok(mut state) = self.state.write() else {
			return;
		};

		if let Some(back) = state.rows.back() {
			// A backward jump in pts or segment number means the publisher restarted its records;
			// the old window can't be stitched onto the new one, so start over.
			if Duration::from(row.pts) < Duration::from(back.pts) || row.segment <= back.segment {
				tracing::warn!("HLS records jumped backwards; resetting the playlist window");
				state.rows.clear();
				state.resets += 1;
			}
		}

		state.discontinuity = row.discontinuity;
		state.rows.push_back(row);
		state.produced += 1;

		loop {
			let rest = match state.rows.get(1) {
				Some(second) => state.rows.back().unwrap().end.saturating_sub(second.pts.into()),
				None => Duration::ZERO,
			};
			if !evicts(window, state.rows.len(), rest) {
				break;
			}
			state.rows.pop_front();
		}
		state.sequence = state.rows.front().unwrap().segment;
	}

	/// Retire the prefix through segment `through`, so the playlist stays consecutive.
	pub fn retire(&self, through: u64) {
		if let Ok(mut state) = self.state.write() {
			state.rows.retain(|row| row.segment > through);
			state.sequence = state
				.rows
				.front()
				.map_or_else(|| state.sequence.max(through.saturating_add(1)), |row| row.segment);
		}
	}

	/// Clear every retained row after an unrecoverable gap in the records.
	pub fn clear(&self) {
		if let Ok(mut state) = self.state.write() {
			state.rows.clear();
			state.resets += 1;
		}
	}

	/// Mark the records finished: the playlist gets `EXT-X-ENDLIST` and cursors end once drained.
	pub fn end(&self) {
		if let Ok(mut state) = self.state.write() {
			state.ended = true;
		}
	}

	/// Mark the window failed with `err`: every listing refuses with it instead of ending, and
	/// cursors return it once drained. No row is pushed after.
	pub fn fail(&self, err: crate::Error) {
		if let Ok(mut state) = self.state.write() {
			state.failed.get_or_insert(err);
		}
	}

	/// Why the window failed, if it did.
	pub fn failure(&self) -> Option<crate::Error> {
		self.state.read().failed.clone()
	}

	/// Close the channel: no more rows will arrive. A [`Consumer`] drains the segments it
	/// can still see and then errors unless [`end`](Self::end) or [`fail`](Self::fail) confirmed
	/// how it ended. The serve path keeps reading the frozen window.
	pub fn close(&self) {
		let _ = self.state.close();
	}

	/// Snapshot the current window (serve path).
	#[cfg_attr(
		not(feature = "server"),
		allow(dead_code, reason = "only the `server` feature uses it")
	)]
	pub fn window(&self) -> Window {
		self.state.read().window()
	}

	/// The row of segment `segment` numbered by the record track tagged `tag`, or `None` if it
	/// isn't in the window.
	pub fn row(&self, tag: &str, segment: u64) -> Option<Row> {
		let state = self.state.read();
		state
			.rows
			.iter()
			.find(|row| row.segment == segment && &*row.tag == tag)
			.cloned()
	}

	/// The row of segment `segment`, whatever its tag, with whether the window holds every
	/// segment from `segment` on: the alignment check needs both.
	pub fn numbered(&self, segment: u64) -> Lookup {
		let state = self.state.read();
		let (Some(front), Some(back)) = (state.rows.front(), state.rows.back()) else {
			return match segment < state.sequence || state.ended || state.failed.is_some() {
				true => Lookup::Never,
				false => Lookup::Pending,
			};
		};
		if segment < front.segment {
			return Lookup::Never;
		}
		if segment > back.segment {
			return match state.ended || state.failed.is_some() {
				true => Lookup::Never,
				false => Lookup::Pending,
			};
		}
		match state.rows.iter().find(|row| row.segment == segment) {
			Some(row) => Lookup::Found(row.clone()),
			None => Lookup::Missing(Duration::from(front.pts)..back.end),
		}
	}

	/// The number of the segment whose `pts` is exactly `time` milliseconds (DASH `$Time$`
	/// addressing), or `None` when no listed row starts there. Exact match is safe because the
	/// rendered `S@t` and this lookup convert the same [`Row::pts`] the same way. A stop is never
	/// rendered, so it never answers, even where content resumes at its pts.
	pub fn segment_number_at(&self, tag: &str, time: u64) -> Option<u64> {
		let state = self.state.read();
		state
			.rows
			.iter()
			.find(|row| !row.stop && row.pts.as_millis() == time as u128 && &*row.tag == tag)
			.map(|row| row.segment)
	}

	/// The first groups of the newest segments holding media, to bootstrap an inline-parameter-set
	/// codec's init from (a segment starts at a group, and a group at a keyframe), in the order to
	/// try them: the newest whose group a later one followed, which is complete and so read from
	/// cache at once, then the newest when only a stop bounds it and its group may still be open.
	pub fn init_groups(&self) -> Vec<u64> {
		let state = self.state.read();
		let rows = &state.rows;
		// A row is listed once its successor arrives, so with nothing after it, a later segment
		// already started.
		let complete = |i: usize| {
			let groups = &rows[i].groups;
			groups.end > groups.start + 1 || rows.get(i + 1).is_none_or(|next| !next.stop)
		};
		let mut media = (0..rows.len()).rev().filter(|&i| !rows[i].stop);
		let Some(newest) = media.next() else {
			return Vec::new();
		};
		let newest_group = rows[newest].groups.start;
		if complete(newest) {
			return vec![newest_group];
		}
		let older = media.find(|&i| complete(i)).map(|i| rows[i].groups.start);
		older.into_iter().chain([newest_group]).collect()
	}

	/// Whether the playlist has anything to serve yet (at least one segment, or the records
	/// already ended).
	#[cfg_attr(
		not(feature = "server"),
		allow(dead_code, reason = "only the `server` feature uses it")
	)]
	pub fn is_playable(&self) -> bool {
		let state = self.state.read();
		state.ended || !state.rows.is_empty()
	}

	/// Poll until [`is_playable`](Self::is_playable) or the window failed, for the serve path's
	/// long-poll.
	pub fn poll_playable(&self, waiter: &kio::Waiter) -> Poll<()> {
		let poll = self.state.poll_ref(waiter, |state| {
			if state.ended || state.failed.is_some() || !state.rows.is_empty() {
				Poll::Ready(())
			} else {
				Poll::Pending
			}
		});
		match poll {
			// Ready, or the channel closed (no more rows will arrive): stop waiting either way.
			Poll::Ready(_) => Poll::Ready(()),
			Poll::Pending => Poll::Pending,
		}
	}

	/// Poll until the window fails.
	#[cfg_attr(
		not(feature = "server"),
		allow(dead_code, reason = "only the `server` feature uses it")
	)]
	pub fn poll_failed(&self, waiter: &kio::Waiter) -> Poll<()> {
		match self.state.poll_ref(waiter, |state| match state.failed {
			Some(_) => Poll::Ready(()),
			None => Poll::Pending,
		}) {
			Poll::Ready(Ok(())) => Poll::Ready(()),
			_ => Poll::Pending,
		}
	}

	/// Poll until the window changes from `seen` rows produced, ends, or fails, for a check that
	/// waits on another rendition's window.
	pub fn poll_changed(&self, waiter: &kio::Waiter, seen: u64) -> Poll<()> {
		match self.state.poll_ref(waiter, |state| {
			if state.produced != seen || state.ended || state.failed.is_some() {
				Poll::Ready(())
			} else {
				Poll::Pending
			}
		}) {
			Poll::Ready(_) => Poll::Ready(()),
			Poll::Pending => Poll::Pending,
		}
	}

	/// How many rows were ever appended.
	pub fn produced(&self) -> u64 {
		self.state.read().produced
	}

	/// A cursor over segments, starting from the oldest still in the window.
	pub fn subscribe(&self, rendition: Arc<Rendition>) -> Consumer {
		let consumer = self.state.consume();
		let state = self.state.read();
		Consumer {
			state: consumer,
			rendition,
			position: (state.resets, state.produced - state.rows.len() as u64),
			feed: None,
		}
	}
}

/// What one window says about a segment number.
#[derive(Debug)]
pub(crate) enum Lookup {
	/// The window lists it.
	Found(Row),
	/// The window holds the numbers around it but not it, spanning this content time.
	Missing(std::ops::Range<Duration>),
	/// It may still arrive.
	Pending,
	/// The window will never list it: evicted already, or the records ended before it.
	Never,
}

/// A segment with its transmuxed media, yielded by a [`Consumer`].
pub struct Segment {
	/// The segment number (its URI is `seg/{tag}.{segment}.m4s`).
	pub segment: u64,
	/// The transmuxed CMAF fragment (`moof`+`mdat`), fetched on demand by [`Consumer::next`],
	/// or `None` for a gap: the rendition has no media for this span (`EXT-X-GAP`), but the
	/// segment keeps its number.
	pub media: Option<Bytes>,
	/// Presentation duration.
	pub duration: Duration,
	/// Wall-clock start time, when the catalog advertises its broadcast clock.
	pub program_date_time: Option<Wall>,
	/// The absolute discontinuity sequence within this broadcaster.
	///
	/// Use the first retained segment's value as `EXT-X-DISCONTINUITY-SEQUENCE`, then write
	/// `current - previous` `EXT-X-DISCONTINUITY` tags before each later segment; a skipped
	/// epoch makes that more than one. Cursors share this sequence regardless of when
	/// they start or which segments they skip. Recreating the broadcaster starts a new
	/// namespace: start a new recording or map it into a recording-wide sequence.
	pub discontinuity: u64,
}

/// A cursor over one rendition's segments, in order.
///
/// Obtained from [`Rendition::segments`](super::Rendition::segments). Each
/// [`next`](Self::next) awaits the next segment, then FETCHes and transmuxes it through the
/// path the HTTP serve path uses. The cursor also holds a live subscription to the media
/// track, so each group reaches the cache as it is published and that FETCH is a hit. A
/// cursor reads every group anyway, so this costs no extra traffic, and a source without
/// FETCH (moq-lite before 05) can serve a group no other way.
pub struct Consumer {
	state: kio::Consumer<State>,
	rendition: Arc<Rendition>,
	/// The reset epoch and next append position; errors leave it unchanged.
	position: (u64, u64),
	feed: Option<Feed>,
}

/// The live subscription a [`Consumer`] holds on its rendition's media track.
struct Feed {
	/// What it subscribed through: a re-issued sibling bind gets a fresh feed.
	binding: Arc<moq_mux::Binding>,
	state: FeedState,
}

enum FeedState {
	/// Waiting for the binding to resolve the broadcast.
	Binding,
	Subscribing(moq_net::track::Subscribing),
	Live(Box<moq_net::track::Subscriber>),
	/// The track can't be subscribed; fetches fail on their own.
	Failed,
}

impl Consumer {
	/// The rendition's CMAF init segment, built once and cached; `None` until it can be built
	/// (an inline-parameter-set codec needs the first segment first).
	pub async fn init(&self) -> Result<Option<Bytes>> {
		self.rendition.latest_init().await
	}

	/// The next segment, with its media; `None` only once the rendition ends cleanly, so a
	/// recording that reads to `None` is complete.
	///
	/// Once the rendition's records fail, the cursor drains the segments it still covers, then
	/// fails every call with [`RecordsFailed`](crate::Error::RecordsFailed), as
	/// [`Rendition::playlist`] does; a failed catalog likewise with
	/// [`CatalogFailed`](crate::Error::CatalogFailed). A denied referenced broadcast fails every
	/// call with its authorization error.
	///
	/// Waits for the next segment's alignment to settle, then FETCHes and transmuxes its groups.
	/// A stop or a misaligned segment is yielded without media, so every segment number is
	/// accounted for. Missing window rows or unavailable media fail the recording instead of
	/// silently omitting segments. Fetch/transmux errors leave the cursor in place so the caller
	/// can retry. A bound instance's retained media drains after End or Restart; a replacement's
	/// media is never read under the old rows.
	pub async fn next(&mut self) -> Result<Option<Segment>> {
		let Some(row) = kio::wait(|waiter| self.poll_next(waiter)).await? else {
			return Ok(None);
		};
		kio::wait(|waiter| {
			// The refusal settles the binding, not this rendition's records, so watch both.
			self.rendition.poll_media(waiter);
			self.rendition.authorize()?;
			self.rendition.poll_resolved(waiter, &row).map(Ok::<_, crate::Error>)
		})
		.await?;
		let media = if self.rendition.is_gap(&row) {
			None
		} else {
			let media = self.rendition.fetch(&row).await?;
			Some(media.ok_or(crate::Error::SegmentUnavailable { segment: row.segment })?)
		};
		self.position.1 += 1;
		Ok(Some(Segment {
			segment: row.segment,
			media,
			duration: row.duration,
			program_date_time: self.rendition.wall_clock(row.pts),
			discontinuity: row.discontinuity,
		}))
	}

	fn poll_next(&mut self, waiter: &kio::Waiter) -> Poll<Result<Option<Row>>> {
		self.poll_feed(waiter);
		// A refused binding never held media, so there is nothing to drain. `poll_feed` registered
		// the wake for a refusal that arrives while the window waits for rows.
		if let Err(err) = self.rendition.authorize() {
			return Poll::Ready(Err(err));
		}
		let found = match ready!(self.state.poll(waiter, |state| -> Poll<Result<Option<Row>>> {
			match state.next_at(self.position)? {
				Next::Ready(row) => Poll::Ready(Ok(Some(row))),
				Next::Done => Poll::Ready(Ok(None)),
				Next::Pending => Poll::Pending,
			}
		})) {
			Ok(found) => found?,
			Err(_) => return Poll::Ready(Err(crate::Error::IncompleteRecording)),
		};
		if found.is_some() {
			return Poll::Ready(Ok(found));
		}
		Poll::Ready(self.rendition.check().map(|()| None))
	}

	/// Hold a live subscription on the media track the rendition is bound to now.
	fn poll_feed(&mut self, waiter: &kio::Waiter) {
		// Also ends the window on an ended sibling, which nothing else may wake this cursor for.
		let binding = self.rendition.poll_media(waiter);
		let feed = match &mut self.feed {
			Some(feed) if Arc::ptr_eq(&feed.binding, &binding) => feed,
			feed => feed.insert(Feed {
				binding,
				state: FeedState::Binding,
			}),
		};
		loop {
			feed.state = match &mut feed.state {
				FeedState::Binding => match self.rendition.poll_subscribe(&feed.binding, waiter) {
					Poll::Ready(Some(subscribing)) => FeedState::Subscribing(subscribing),
					Poll::Ready(None) => FeedState::Failed,
					Poll::Pending => return,
				},
				FeedState::Subscribing(subscribing) => match subscribing.poll_ok(waiter) {
					Poll::Ready(Ok(subscriber)) => FeedState::Live(Box::new(subscriber)),
					Poll::Ready(Err(_)) => FeedState::Failed,
					Poll::Pending => return,
				},
				FeedState::Live(subscriber) => {
					// Drop each group as it arrives: the cache keeps it for the fetch, and a
					// subscription that resumed across routes only pulls while it is read.
					while let Poll::Ready(Ok(Some(_))) = subscriber.poll_recv_group(waiter) {}
					return;
				}
				FeedState::Failed => return,
			};
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn row(segment: u64, group: u64, pts_ms: u64, duration_ms: u64) -> Row {
		let pts = moq_net::Timestamp::from_millis(pts_ms).unwrap();
		Row {
			index: segment,
			segment,
			tag: Arc::from(tag("video.hls", b"")),
			groups: group..group + 1,
			duration: Duration::from_millis(duration_ms),
			pts,
			end: Duration::from(pts) + Duration::from_millis(duration_ms),
			discontinuity: 0,
			stop: false,
		}
	}

	// Content that resumes at the pts it stopped shares a `$Time$` with the stop, which the MPD
	// omits, so the time names the resumed segment.
	#[test]
	fn a_time_names_the_resumed_segment_not_the_stop() {
		let live = Producer::new(0);
		let window = Duration::from_secs(30);
		live.push(row(0, 0, 0, 4_000), window);
		let mut stop = row(1, 1, 4_000, 0);
		stop.groups = 1..1;
		stop.stop = true;
		live.push(stop, window);
		live.push(row(2, 1, 4_000, 4_000), window);
		let tag = tag("video.hls", b"");
		assert_eq!(live.segment_number_at(&tag, 4_000), Some(2));
		assert_eq!(live.segment_number_at(&tag, 0), Some(0));
	}

	#[test]
	fn every_row_is_listed() {
		let live = Producer::new(0);
		live.push(row(0, 0, 0, 2_000), Duration::from_secs(30));
		live.push(row(1, 1, 2_000, 2_000), Duration::from_secs(30));

		let window = live.window();
		assert_eq!(window.sequence, 0);
		assert!(!window.ended);
		assert_eq!(window.segments.len(), 2, "rows are complete segments; all are listed");
		assert_eq!(window.segments[0].segment, 0);
		assert_eq!(window.segments[0].duration, Duration::from_secs(2));

		live.end();
		assert!(live.window().ended);
	}

	#[test]
	fn window_evicts_and_advances_sequence() {
		let live = Producer::new(0);
		let window = Duration::from_secs(4);
		for i in 0..6u64 {
			live.push(row(i, i, i * 2_000, 2_000), window);
		}

		let snapshot = live.window();
		// Segments still cover >= 4s after eviction, and the sequence is the first listed
		// segment's number.
		assert!(snapshot.sequence > 0);
		let span: Duration = snapshot.segments.iter().map(|s| s.duration).sum();
		assert!(span >= Duration::from_secs(4));
		assert_eq!(snapshot.segments.first().unwrap().segment, snapshot.sequence);
	}

	#[test]
	fn retiring_a_prefix_advances_the_playlist_sequence() {
		let live = Producer::new(0);
		let window = Duration::from_secs(30);
		for i in 0..4u64 {
			live.push(row(i, i, i * 2_000, 2_000), window);
		}

		live.retire(1);
		let snapshot = live.window();
		assert_eq!(snapshot.sequence, 2);
		assert_eq!(
			snapshot.segments.iter().map(|row| row.segment).collect::<Vec<_>>(),
			vec![2, 3]
		);

		live.retire(3);
		let snapshot = live.window();
		assert_eq!(snapshot.sequence, 4);
		assert!(snapshot.segments.is_empty());
	}

	#[test]
	fn a_skipped_source_range_clears_rows_before_the_next_segment() {
		let live = Producer::new(0);
		live.push(row(4, 4, 8_000, 2_000), Duration::from_secs(10));
		live.clear();
		live.push(row(10, 10, 20_000, 2_000), Duration::from_secs(10));

		let snapshot = live.window();
		assert_eq!(snapshot.sequence, 10);
		assert_eq!(
			snapshot.segments.iter().map(|row| row.segment).collect::<Vec<_>>(),
			vec![10]
		);
	}

	#[test]
	fn renditions_agree_on_discontinuities_whatever_they_listed() {
		let mut breaks = Discontinuities::default();
		assert_eq!(breaks.stamp(0, false), 0);
		assert_eq!(breaks.stamp(1, false), 0);
		assert_eq!(breaks.stamp(2, true), 1, "the segment after a stop starts a new one");
		assert_eq!(breaks.stamp(3, false), 1);
		// A rendition that joined later, or saw records the first one skipped, takes the stamps
		// already made rather than its own view.
		assert_eq!(breaks.stamp(2, false), 1);
		assert_eq!(breaks.stamp(3, true), 1);
		assert_eq!(breaks.stamp(4, false), 1);
	}

	#[test]
	fn a_restart_starts_a_new_discontinuity() {
		let mut breaks = Discontinuities::default();
		assert_eq!(breaks.stamp(5, false), 0);
		breaks.restart();
		assert_eq!(
			breaks.stamp(0, false),
			1,
			"the new run's numbering is unrelated to the old"
		);
	}

	#[test]
	fn rows_and_the_init_groups() {
		let live = Producer::new(0);
		let window = Duration::from_secs(30);
		assert!(live.init_groups().is_empty());
		live.push(row(0, 0, 0, 1_000), window);
		assert_eq!(live.init_groups(), [0], "a listed row's successor already started");
		live.push(
			Row {
				stop: true,
				..row(1, 1, 1_000, 0)
			},
			window,
		);

		let tag = tag("video.hls", b"");
		assert_eq!(live.row(&tag, 0).unwrap().groups, 0..1);
		assert_eq!(live.row(&tag, 7), None, "unknown segments miss");
		assert_eq!(live.row("00000000", 0), None, "another record track's numbering misses");
		assert_eq!(live.init_groups(), [0], "a stop holds no media");
		live.push(row(2, 1, 1_000, 1_000), window);
		live.push(row(3, 2, 2_000, 1_000), window);
		live.push(
			Row {
				stop: true,
				..row(4, 3, 3_000, 0)
			},
			window,
		);
		assert_eq!(
			live.init_groups(),
			[1, 2],
			"a group only a stop bounds may be open, so a complete one goes first"
		);
	}

	#[test]
	fn backwards_jump_resets_the_window() {
		let live = Producer::new(0);
		let window = Duration::from_secs(30);
		live.push(row(0, 0, 10_000, 2_000), window);
		live.push(row(1, 1, 12_000, 2_000), window);
		live.push(row(2, 2, 1_000, 2_000), window); // restart: pts rewound

		let snapshot = live.window();
		assert_eq!(snapshot.segments.len(), 1, "the window restarted at the new row");
		assert_eq!(snapshot.segments[0].segment, 2);

		// A segment number that rewinds (a restarted publisher) resets the same way.
		live.push(row(0, 0, 2_000, 2_000), window);
		assert_eq!(live.window().segments.len(), 1);
		assert_eq!(live.state.read().resets, 2);
	}

	#[test]
	fn lookups_tell_pending_from_never() {
		let live = Producer::new(0);
		let window = Duration::from_secs(30);
		assert!(matches!(live.numbered(0), Lookup::Pending));
		live.push(row(3, 3, 6_000, 2_000), window);
		live.push(row(5, 5, 10_000, 2_000), window);
		assert!(matches!(live.numbered(2), Lookup::Never), "evicted or never listed");
		assert!(matches!(live.numbered(3), Lookup::Found(row) if row.segment == 3));
		assert!(
			matches!(live.numbered(4), Lookup::Missing(span) if span == (Duration::from_secs(6)..Duration::from_secs(12)))
		);
		assert!(matches!(live.numbered(6), Lookup::Pending));
		live.end();
		assert!(matches!(live.numbered(6), Lookup::Never));
	}

	#[test]
	fn cursor_walks_segments() {
		let live = Producer::new(0);
		let window = Duration::from_secs(30);
		live.push(row(0, 0, 0, 2_000), window);
		live.push(row(1, 1, 2_000, 2_000), window);

		let Next::Ready(first) = live.state.read().next_at((0, 0)).unwrap() else {
			panic!("expected a segment");
		};
		assert_eq!(first.segment, 0);
		let next = |segment| (0, segment);
		let Next::Ready(second) = live.state.read().next_at(next(1)).unwrap() else {
			panic!("expected a segment");
		};
		assert_eq!(second.segment, 1);
		assert!(
			matches!(live.state.read().next_at(next(2)).unwrap(), Next::Pending),
			"nothing further while live"
		);

		live.end();
		assert!(matches!(live.state.read().next_at(next(2)).unwrap(), Next::Done));
	}

	#[test]
	fn cursors_refuse_lost_rows_and_resets() {
		let live = Producer::new(0);
		let window = Duration::from_secs(30);
		for segment in 5..8 {
			live.push(row(segment, segment, segment * 2_000, 2_000), window);
		}
		// One cursor consumed the first row, another has not read anything yet.
		live.retire(5);
		assert!(matches!(
			live.state.read().next_at((0, 0)),
			Err(crate::Error::RecordingLagged)
		));
		assert!(matches!(live.state.read().next_at((0, 1)), Ok(Next::Ready(_))));
		live.retire(7);
		assert!(matches!(
			live.state.read().next_at((0, 1)),
			Err(crate::Error::RecordingLagged)
		));
		assert!(matches!(live.state.read().next_at((0, 3)), Ok(Next::Pending)));
		live.clear();
		live.push(row(0, 0, 0, 2_000), window);
		assert!(matches!(
			live.state.read().next_at((0, 3)),
			Err(crate::Error::RecordingLagged)
		));
		assert!(matches!(live.state.read().next_at((1, 3)), Ok(Next::Ready(_))));
	}

	#[test]
	fn tags_tell_record_tracks_apart() {
		assert_ne!(tag("video.hls", b""), tag("video2.hls", b""));
		assert_ne!(tag("video.hls", b"a"), tag("video.hls", b"b"));
		// The separator keeps a name's tail from passing for a config's head.
		assert_ne!(tag("video.hls", b"x"), tag("video.hlsx", b""));
		assert_eq!(tag("video.hls", b"").len(), 8);
		assert!(tag("a.b/c", b"").chars().all(|c| c.is_ascii_hexdigit()));
	}
}
