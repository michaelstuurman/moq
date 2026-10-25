//! Opt-in HLS segment records: where each of a rendition's HLS segments starts.
//!
//! Nothing here runs unless the application enables it with
//! [`catalog::Config::with_hls`](crate::catalog::Config::with_hls). Then every video and audio
//! rendition the catalog mints gets a track of [`hang::hls::Record`]s, one per segment, and its
//! catalog entry gets an [`hls`](hang::catalog::Hls) field naming that track and the nominal
//! segment duration. An edge renders HLS (or DASH) playlists from the records without subscribing
//! to media.
//!
//! ## Segments
//!
//! A segment starts at a group boundary and holds whole groups, so a record is the segment's first
//! group and pts, published as the segment starts. A segment ends at the first group boundary at
//! least [`Config::duration_min`] (4s by default) past its start, so short groups (audio) pack into
//! one segment and long ones (video GOPs) get one each. A hint fixes the nominal duration to its
//! smallest multiple at or beyond that minimum; without one, the first segment's duration declares
//! it.
//!
//! Content stopping (a skipped group sequence, a break marker from
//! [`container::Producer::discontinuity`](crate::container::Producer::discontinuity), or the end of
//! the track) publishes a zero-duration record where it stopped, so the segment before it ends there
//! rather than absorbing the pause.
//!
//! ## Alignment
//!
//! HLS needs segment `n` to start at the same pts in every rendition. The first track enrolled
//! paces the segments; every later track follows it, starting a segment at its first group at or
//! after each of the pacer's segment starts (even one it reported before the pacer did), and
//! stopping when the pacer stops content. A follower records
//! nothing before the pacer's first segment, so renditions that start together and share group
//! boundaries (aligned GOPs, audio grouped on them) number the same segments alike. A follower
//! keeps at most a ceiling of boundaries waiting on the other track, so one that stalls costs
//! alignment, not memory. If the pacer's producer goes away, the next enrolled track still
//! recording takes over.
//!
//! ## Wiring
//!
//! [`catalog::Producer`](crate::catalog::Producer) owns the broadcast's record tracks and advertises
//! each in its rendition's `hls` field. On the read side, [`Consumer::subscribe`] reads one
//! rendition's records and yields decoded [`Event`]s. On the wire each record track is an
//! uncompressed [`moq_json::window`] whose groups restate the recent records.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use hang::hls::Record;

use moq_net::{Timescale, Timestamp};

/// The default [`Config::duration_min`] (4 seconds).
pub const DEFAULT_DURATION_MIN: Duration = Duration::from_secs(4);

/// The safety ceiling before an unhinted pacer declares its duration, unless the minimum is longer.
const DEFAULT_DURATION_CEILING: Duration = Duration::from_secs(10);

/// A pacer's segments may overrun their target to reach a group boundary, but not indefinitely.
const DURATION_CEILING_MULTIPLIER: u32 = 3;

/// Recent records repeated when the window track rolls to a new group: one more than the 256
/// segments an HLS edge lists, since a segment is listed only once the record after it arrives.
const CHECKPOINT_RECORDS: usize = 257;

/// The wire timescale for record `pts`/`duration`: milliseconds.
const TIMESCALE: Timescale = Timescale::MILLI;

/// How a broadcast's HLS segments are cut.
///
/// A segment ends at the first group boundary at least [`duration_min`](Self::duration_min) past
/// its start. A hint fixes the nominal duration to its smallest multiple at or beyond that minimum.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Config {
	/// The shortest a segment may be before a group boundary ends it.
	///
	/// A floor rather than a target on purpose: a floor is always satisfiable (wait for the next
	/// group), so no group is split to honor it.
	pub duration_min: Duration,

	/// The application's nominal group duration, when known.
	pub duration_hint: Option<Duration>,
}

impl Config {
	/// Set [`duration_min`](Self::duration_min).
	pub fn with_duration_min(mut self, duration: Duration) -> Self {
		self.duration_min = duration;
		self
	}

	/// Set [`duration_hint`](Self::duration_hint).
	pub fn with_duration_hint(mut self, duration: impl Into<Option<Duration>>) -> Self {
		self.duration_hint = duration.into();
		self
	}
}

impl Default for Config {
	fn default() -> Self {
		Self {
			duration_min: DEFAULT_DURATION_MIN,
			duration_hint: None,
		}
	}
}

/// A duration in wire units, rounded up so a nominal duration never understates itself.
fn units(duration: Duration) -> u64 {
	u64::try_from((duration.as_micros() * TIMESCALE.as_u64() as u128).div_ceil(1_000_000)).unwrap_or(u64::MAX)
}

fn wire(pts: Timestamp) -> u64 {
	pts.as_scale(TIMESCALE) as u64
}

fn elapsed(start: Timestamp, pts: Timestamp) -> Duration {
	Duration::from_micros(u64::try_from(pts.as_micros().saturating_sub(start.as_micros())).unwrap_or(u64::MAX))
}

/// What a pacer did, for its followers to mirror.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Mark {
	/// A segment started at this pts.
	Start(Timestamp),
	/// Content stopped.
	Stop,
}

/// Builds one track's records from reports of the frames it published.
///
/// Reports arrive in group order: [`frame`](Self::frame) for each frame (or at least the first of
/// each group), [`end`](Self::end) wherever the content is known to extend, and
/// [`stop`](Self::stop) where it stops. Records queue until [`Iterator::next`] takes them.
pub(crate) struct Segmenter {
	config: Config,
	/// Fixed nominal duration; absent until an unhinted track's first segment closes. Kept unrounded
	/// so pacing hits a fractional-millisecond GOP's boundary; only the wire declaration rounds.
	declared: Option<Duration>,
	/// The number the next record gets.
	sequence: u64,
	/// The open segment's start: absent before the first segment and after a stop.
	open: Option<Timestamp>,
	/// The newest reported group.
	last: Option<u64>,
	/// The newest reported content time in the open segment.
	frontier: Option<Timestamp>,
	/// Boundaries not yet reached, in order.
	cuts: VecDeque<Timestamp>,
	/// The newest boundary declared, so a repeat of it (several sources declaring the same ones)
	/// is ignored.
	cut: Option<Timestamp>,
	/// A follower's group starts after every cut it knows of, oldest first: a pacer reporting later
	/// may still cut at one. Empty whenever `cuts` holds a boundary past the open segment.
	undecided: VecDeque<(u64, Timestamp)>,
	/// The newest reported content time, whether or not a segment is open, so a segment a late cut
	/// starts reaches it.
	reported: Option<Timestamp>,
	/// Whether boundaries come only from cuts: a follower, or a pacer whose source declares them.
	manual: bool,
	/// Whether this track follows a pacer, starting a segment only at one of its cuts.
	following: bool,
	ready: VecDeque<Record>,
	/// What this segmenter did since last asked, for followers to mirror.
	marks: Vec<Mark>,
	closed: bool,
}

impl Segmenter {
	/// A segmenter numbering its records from zero.
	pub fn new(config: Config) -> Self {
		let declared = config.duration_hint.filter(|hint| !hint.is_zero()).map(|hint| {
			let multiple = config.duration_min.as_nanos().div_ceil(hint.as_nanos()).max(1);
			hint.saturating_mul(u32::try_from(multiple).unwrap_or(u32::MAX))
		});
		Self {
			config,
			declared,
			sequence: 0,
			open: None,
			last: None,
			frontier: None,
			cuts: VecDeque::new(),
			cut: None,
			undecided: VecDeque::new(),
			reported: None,
			manual: false,
			following: false,
			ready: VecDeque::new(),
			marks: Vec::new(),
			closed: false,
		}
	}

	/// Follow a pacer's segments instead of pacing them; see [`follow`](Self::follow).
	pub fn following(mut self) -> Self {
		self.manual = true;
		self.following = true;
		self
	}

	/// Number the next record `sequence`, continuing a re-enrolled track.
	pub fn with_sequence(mut self, sequence: u64) -> Self {
		self.sequence = sequence;
		self
	}

	/// The number the next record gets.
	pub fn sequence(&self) -> u64 {
		self.sequence
	}

	/// The fixed nominal segment duration, once known.
	pub fn declared(&self) -> Option<Duration> {
		self.declared
	}

	fn target(&self) -> Duration {
		self.declared
			.filter(|value| !value.is_zero())
			.unwrap_or(self.config.duration_min)
	}

	fn ceiling(&self) -> Duration {
		self.declared
			.filter(|value| !value.is_zero())
			.and_then(|value| value.checked_mul(DURATION_CEILING_MULTIPLIER))
			.unwrap_or(DEFAULT_DURATION_CEILING.max(self.config.duration_min))
	}

	/// Report a frame of group `group`, presented at `pts`.
	///
	/// Fails with [`HlsGroup`](crate::Error::HlsGroup) when `group` precedes the previous report's:
	/// segments hold whole groups in order, so that frame could never be listed.
	pub fn frame(&mut self, group: u64, pts: Timestamp) -> crate::Result<()> {
		if self.closed {
			return Ok(());
		}
		if let Some(last) = self.last {
			if group < last {
				return Err(crate::Error::HlsGroup { group, last });
			}
			if group != last {
				self.group(group, last, pts);
			}
		} else {
			self.open_at(group, pts);
		}
		self.last = Some(group);
		self.report(pts);
		Ok(())
	}

	/// Content reached `pts`.
	fn report(&mut self, pts: Timestamp) {
		if self
			.reported
			.is_none_or(|reported| pts.as_micros() > reported.as_micros())
		{
			self.reported = Some(pts);
		}
		if self.open.is_some() {
			self.advance(pts);
		}
	}

	/// Group `group` starts at `pts`, after group `last`.
	fn group(&mut self, group: u64, last: u64, pts: Timestamp) {
		let Some(start) = self.open else {
			self.open_at(group, pts);
			return;
		};
		if group != last + 1 {
			// A skipped sequence stops the segment where its content reached, so the skipped time
			// is not absorbed into it and the reader never has to guess what lies between.
			self.stop_at(last + 1);
			self.open_at(group, pts);
			return;
		}
		let elapsed = elapsed(start, pts);
		let ceiling = !self.following && elapsed >= self.ceiling();
		if ceiling || self.boundary(start, pts, elapsed) {
			self.declared.get_or_insert(elapsed);
			self.begin(group, pts);
		} else if self.following && self.cuts.is_empty() {
			self.undecide(group, pts);
		}
	}

	/// Keep a follower's group at `pts` for a cut the pacer may still report.
	///
	/// A pacer starts a segment at least once per ceiling while it reports, so a group more than a
	/// ceiling behind this one could only meet a pacer stalled that long: forgetting it bounds the
	/// queue, and that late cut starts the segment at a later group instead.
	fn undecide(&mut self, group: u64, pts: Timestamp) {
		let horizon = self.ceiling();
		while self
			.undecided
			.front()
			.is_some_and(|(_, start)| elapsed(*start, pts) > horizon)
		{
			self.undecided.pop_front();
		}
		self.undecided.push_back((group, pts));
	}

	/// With no segment open, start one with group `group` at `pts`, unless a follower has no cut
	/// to start it at.
	fn open_at(&mut self, group: u64, pts: Timestamp) {
		if self.following {
			// A follower starts only where the pacer did, so both number the segment alike.
			let mut started = false;
			while self.cuts.front().is_some_and(|cut| cut.as_micros() <= pts.as_micros()) {
				self.cuts.pop_front();
				started = true;
			}
			if !started {
				// With no cut ahead, the pacer may still report one at or before this group.
				if self.cuts.is_empty() {
					self.undecide(group, pts);
				}
				return;
			}
		}
		self.begin(group, pts);
	}

	/// Start a segment with group `group` at `pts`.
	fn begin(&mut self, group: u64, pts: Timestamp) {
		self.open = Some(pts);
		self.frontier = Some(pts);
		self.ready.push_back(Record::new(self.sequence, group, wire(pts)));
		self.marks.push(Mark::Start(pts));
		self.sequence += 1;
	}

	/// Whether a group starting at `pts` ends the segment that started at `start`.
	fn boundary(&mut self, start: Timestamp, pts: Timestamp, elapsed: Duration) -> bool {
		while self
			.cuts
			.front()
			.is_some_and(|cut| cut.as_micros() <= start.as_micros())
		{
			self.cuts.pop_front();
		}
		if self.cuts.front().is_some_and(|cut| cut.as_micros() <= pts.as_micros()) {
			while self.cuts.front().is_some_and(|cut| cut.as_micros() <= pts.as_micros()) {
				self.cuts.pop_front();
			}
			return true;
		}
		!self.manual && elapsed >= self.target()
	}

	/// Report that the content extends to `pts`, without a frame starting there.
	///
	/// A frame report says where content starts; the stop after the last segment would otherwise
	/// land at its last frame's start.
	pub fn end(&mut self, pts: Timestamp) {
		if !self.closed {
			self.report(pts);
		}
	}

	fn advance(&mut self, pts: Timestamp) {
		if self
			.frontier
			.is_none_or(|frontier| pts.as_micros() > frontier.as_micros())
		{
			self.frontier = Some(pts);
		}
	}

	/// Declare a boundary at `pts`: the segment ends at the first group starting at or after it.
	///
	/// The first cut takes over from [`Config::duration_min`] pacing for good, since pacing would
	/// otherwise end a segment just before the caller declares where it really ends. A cut at or
	/// before an earlier one is ignored, so several sources declaring the same boundaries cost
	/// nothing.
	pub fn cut(&mut self, pts: Timestamp) {
		if self.closed {
			return;
		}
		self.manual = true;
		if self.cut.is_some_and(|cut| pts.as_micros() <= cut.as_micros()) {
			return;
		}
		self.cut = Some(pts);
		// A follower that reported past this cut before hearing it decides those groups now: the
		// ones before it are not boundaries, and its first group at or after it starts a segment.
		while self
			.undecided
			.front()
			.is_some_and(|(_, start)| start.as_micros() < pts.as_micros())
		{
			self.undecided.pop_front();
		}
		match self.undecided.pop_front() {
			Some((group, start)) => {
				if let Some(open) = self.open {
					self.declared.get_or_insert(elapsed(open, start));
				}
				self.begin(group, start);
				if let Some(reported) = self.reported {
					self.advance(reported);
				}
			}
			None => {
				// A follower that stopped reporting keeps only the cuts within a ceiling of this one:
				// its next group past them all starts one segment either way, and one landing
				// between them was already that far behind the pacer's numbering.
				if self.following {
					let horizon = self.ceiling();
					while self.cuts.front().is_some_and(|cut| elapsed(*cut, pts) > horizon) {
						self.cuts.pop_front();
					}
				}
				self.cuts.push_back(pts);
			}
		}
	}

	/// Content stopped before group `group`: publish a zero-duration record where it reached.
	///
	/// The next group starts a new segment. Nothing happens while no segment is open.
	pub fn stop(&mut self, group: u64) {
		if !self.closed {
			self.stop_at(group);
		}
	}

	fn stop_at(&mut self, group: u64) {
		// What a follower reported past its last cut stops with it.
		self.undecided.clear();
		let Some(start) = self.open.take() else {
			return;
		};
		// One stopped early says nothing about the cadence, so the target falls back to the minimum.
		self.declared.get_or_insert(self.config.duration_min);
		let end = self.frontier.take().unwrap_or(start);
		self.ready
			.push_back(Record::new(self.sequence, group, wire(end)).with_duration(0));
		self.marks.push(Mark::Stop);
		self.sequence += 1;
	}

	/// Stop after the newest group, then ignore every later report.
	pub fn close(&mut self) {
		if let Some(last) = self.last {
			self.stop_at(last + 1);
		}
		self.closed = true;
	}

	/// Mirror what a pacer did.
	fn follow(&mut self, mark: Mark) {
		match mark {
			Mark::Start(pts) => self.cut(pts),
			Mark::Stop => {
				if let Some(last) = self.last {
					self.stop(last + 1);
				}
			}
		}
	}

	/// Take over pacing from a pacer that went away.
	fn pace(&mut self) {
		self.manual = false;
		self.following = false;
		self.cuts.clear();
		// Groups no cut decided stay in the open segment, as a pacer's do until its target.
		self.undecided.clear();
	}
}

/// Yields each record once; `None` means none is ready yet, not that none ever will be.
impl Iterator for Segmenter {
	type Item = Record;

	fn next(&mut self) -> Option<Record> {
		self.ready.pop_front()
	}
}

/// Publishes one track's records onto its HLS record track, for a publisher that segments
/// itself; [`catalog::Config::with_hls`](crate::catalog::Config::with_hls) records them for you.
///
/// Records must be pushed in sequence order, starting at the window's next index.
pub struct Producer {
	sink: moq_json::window::Producer<Record>,
	/// The record track, to abort.
	track: moq_net::track::Producer,
}

impl Producer {
	/// The properties a record track is created with.
	pub fn info() -> moq_net::track::Info {
		moq_net::track::Info::default()
			.with_timescale(moq_net::Timescale::MILLI)
			.with_priority(hang::catalog::PRIORITY.catalog)
			// A joiner asks for a fresh group once the ops outweigh eight headers.
			.with_refresh(8)
	}

	/// Publish onto `track`, starting at record zero.
	pub fn new(track: moq_net::track::Producer) -> Self {
		let config = moq_json::window::ProducerConfig::default().with_checkpoint_records(CHECKPOINT_RECORDS);
		Self {
			sink: moq_json::window::Producer::new(track.clone(), config),
			track,
		}
	}

	/// Append `record`, which must be the window's next sequence.
	pub fn push(&mut self, record: &Record) -> crate::Result<()> {
		let next = self.sink.range().end;
		if record.sequence != next {
			return Err(crate::Error::HlsSequence {
				expected: next,
				actual: record.sequence,
			});
		}
		self.refresh()?;
		self.sink
			.push(moq_net::Timed::from(record).at(moq_net::Timestamp::now()))
			.map_err(json)
	}

	/// Finish the record track.
	pub fn finish(&mut self) -> crate::Result<()> {
		self.sink.finish().map_err(json)
	}

	/// Abort the record track with `err`, so readers see the cause rather than a complete record.
	pub(crate) fn abort(self, err: moq_net::Error) {
		// Idempotent: a track already closed keeps its first reason.
		let _ = self.track.abort(err);
	}

	/// Restate the window in a fresh group if a joiner wants one.
	///
	/// Checked on each edit rather than driven by a task, since nothing here owns a runtime. A
	/// record track gains a record every segment, so a joiner waits at most about one segment.
	fn refresh(&mut self) -> crate::Result<()> {
		match self.sink.poll_refresh(&kio::Waiter::noop()) {
			Poll::Ready(Err(err)) => Err(json(err)),
			_ => Ok(()),
		}
	}
}

/// Surface a transport failure as one, rather than wrapped in the JSON layer.
fn json(err: moq_json::Error) -> crate::Error {
	match err {
		moq_json::Error::Net(err) => err.into(),
		err => err.into(),
	}
}

/// Runs once, with the nominal duration, when an unhinted track learns it.
type OnDeclare = Box<dyn FnOnce(Duration) + Send>;

/// A live track's segmenter and the record track it publishes to.
struct Live {
	segmenter: Segmenter,
	/// Publishes an unhinted declaration with the first complete segment.
	on_declare: Option<OnDeclare>,
	/// `None` once a publish failed: the records are an optional sidecar, so they stop instead.
	output: Option<Producer>,
	/// Whether a [`Recorder`] owns the segmenter, so a second enrollment can't share it.
	recording: bool,
	/// The enrollment that last claimed it, so the earliest still recording takes over pacing.
	enrolled: u64,
}

/// What a [`Live::publish`] left for its caller to act on once every lock is released.
#[must_use]
struct Published {
	/// What the segmenter did, for followers to mirror.
	marks: Vec<Mark>,
	/// A declaration to announce: the callback takes the catalog's lock, which is taken before ours.
	declare: Option<(OnDeclare, Duration)>,
}

impl Published {
	/// Announce any declaration, returning the marks.
	fn fire(self) -> Vec<Mark> {
		if let Some((on_declare, duration)) = self.declare {
			on_declare(duration);
		}
		self.marks
	}
}

impl Live {
	/// Publish ready records.
	fn publish(&mut self) -> Published {
		for record in self.segmenter.by_ref() {
			let Some(output) = self.output.as_mut() else {
				continue;
			};
			if let Err(err) = output.push(&record) {
				tracing::warn!(%err, "HLS record publish failed; dropping the record track");
				let _ = output.finish();
				self.output = None;
			}
		}
		let declare = match self.segmenter.declared() {
			Some(duration) => self.on_declare.take().map(|on_declare| (on_declare, duration)),
			None => None,
		};
		Published {
			marks: std::mem::take(&mut self.segmenter.marks),
			declare,
		}
	}

	/// Abort the record track with `err`, recording nothing more.
	fn abort(&mut self, err: moq_net::Error) {
		self.on_declare = None;
		self.segmenter.closed = true;
		if let Some(output) = self.output.take() {
			output.abort(err);
		}
	}

	fn finish(&mut self) {
		// Nothing advertises a declaration once the tracks finish.
		self.on_declare = None;
		self.segmenter.close();
		let _ = self.publish();
		if let Some(output) = self.output.as_mut() {
			let _ = output.finish();
		}
	}
}

struct State {
	/// Each enrolled track's record track name and live state, by media track name. Kept after its
	/// recorder drops, so a re-enrolled track continues its numbering.
	tracks: BTreeMap<String, (String, Arc<Mutex<Live>>)>,
	/// The track pacing the segments, if any is recording.
	pacer: Option<String>,
	/// Enrollments so far, numbering each.
	enrolled: u64,
	finished: bool,
}

/// A broadcast's HLS record tracks, one per enrolled rendition.
///
/// Each enrollment creates the track's record track and returns the [`Recorder`] its producer
/// reports through. `Clone` shares the set.
#[derive(Clone)]
pub(crate) struct Tracks {
	broadcast: moq_net::broadcast::Producer,
	state: Arc<Mutex<State>>,
}

impl Tracks {
	/// Record tracks for tracks of `broadcast`.
	pub fn new(broadcast: &moq_net::broadcast::Producer) -> Self {
		Self {
			broadcast: broadcast.clone(),
			state: Arc::new(Mutex::new(State {
				tracks: BTreeMap::new(),
				pacer: None,
				enrolled: 0,
				finished: false,
			})),
		}
	}

	/// Enroll the track `name`, its segments cut by `config`.
	///
	/// Creates its record track, named by [`hang::hls::default_name`], on first enrollment. The
	/// first track recording paces the segments and the rest follow it. Enrolling a name again
	/// after its recorder dropped continues its numbering for a new producer. Errors when the name's
	/// recorder is still alive, the record track name is taken, or the tracks finished.
	pub fn track(&self, name: &str, config: Config) -> crate::Result<Recorder> {
		let mut state = self.state.lock().unwrap();
		if state.finished {
			return Err(moq_net::Error::Closed.into());
		}
		state.enrolled += 1;
		let enrolled = state.enrolled;
		let pacing = state.pacer.is_none();
		let segmenter = |sequence: u64| {
			let segmenter = Segmenter::new(config.clone()).with_sequence(sequence);
			match pacing {
				true => segmenter,
				false => segmenter.following(),
			}
		};

		let live = match state.tracks.get(name) {
			Some((_, existing)) => {
				// A re-enrolled track is a new producer whose group sequences may restart, so it gets a
				// fresh segmenter continuing the numbering and the declaration already advertised.
				let mut live = existing.lock().unwrap();
				if live.recording {
					return Err(moq_net::Error::Duplicate.into());
				}
				let declared = live.segmenter.declared();
				live.segmenter = segmenter(live.segmenter.sequence());
				if declared.is_some() {
					live.segmenter.declared = declared;
				}
				live.recording = true;
				live.enrolled = enrolled;
				drop(live);
				existing.clone()
			}
			None => {
				let track = self
					.broadcast
					.create_track(hang::hls::default_name(name).as_str(), Producer::info())?;
				let live = Arc::new(Mutex::new(Live {
					segmenter: segmenter(0),
					on_declare: None,
					output: Some(Producer::new(track)),
					recording: true,
					enrolled,
				}));
				state
					.tracks
					.insert(name.to_string(), (hang::hls::default_name(name), live.clone()));
				live
			}
		};
		if pacing {
			state.pacer = Some(name.to_string());
		}
		Ok(Recorder {
			name: name.to_string(),
			live,
			state: self.state.clone(),
		})
	}

	/// The record track and nominal duration advertised for `name`, once its duration is known.
	pub fn hls(&self, name: &str) -> Option<hang::catalog::Hls> {
		let state = self.state.lock().unwrap();
		let (track, live) = state.tracks.get(name)?;
		let duration = live.lock().unwrap().segmenter.declared()?;
		Some(hang::catalog::Hls::new(track.clone(), round(duration)))
	}

	/// Stop every open segment now, publishing any declaration it fixes.
	pub fn flush(&self) {
		let published: Vec<_> = {
			let state = self.state.lock().unwrap();
			state
				.tracks
				.values()
				.map(|(_, live)| {
					let mut live = live.lock().unwrap();
					if let Some(last) = live.segmenter.last {
						live.segmenter.stop(last + 1);
					}
					live.publish()
				})
				.collect()
		};
		for published in published {
			published.fire();
		}
	}

	/// Finish the record track of `name`, whose recorder dropped and whose owner never enrolls the
	/// name again, and forget it, so a producer minting a name per track holds only its live ones.
	///
	/// Readers already on the record track read it to its end. Nothing happens for a name never
	/// enrolled; a name whose recorder is still alive errors.
	pub fn retire(&self, name: &str) -> crate::Result<()> {
		let mut state = self.state.lock().unwrap();
		let Some((_, live)) = state.tracks.get(name) else {
			return Ok(());
		};
		let mut live = live.lock().unwrap();
		if live.recording {
			return Err(moq_net::Error::Duplicate.into());
		}
		live.finish();
		drop(live);
		state.tracks.remove(name);
		Ok(())
	}

	/// How many record tracks are held, live or kept for a re-enrollment.
	#[cfg(test)]
	pub fn len(&self) -> usize {
		self.state.lock().unwrap().tracks.len()
	}

	/// Abort every record track with `err`. Later enrollments fail.
	pub fn abort(&self, err: &moq_net::Error) {
		let mut state = self.state.lock().unwrap();
		state.finished = true;
		for (_, live) in state.tracks.values() {
			live.lock().unwrap().abort(err.clone());
		}
	}

	/// Stop every open segment and finish every record track. Later enrollments fail.
	pub fn finish(&self) {
		let mut state = self.state.lock().unwrap();
		state.finished = true;
		for (_, live) in state.tracks.values() {
			live.lock().unwrap().finish();
		}
	}
}

/// A nominal duration as the catalog advertises it, in whole wire units rounded up.
fn round(duration: Duration) -> Duration {
	Duration::from_millis(units(duration))
}

/// Reports one track's frames into its live record track.
///
/// Move-only: it is the track's reporting handle, minted by [`Tracks::track`] and held by a
/// rendition's [`container::Producer`](crate::container::Producer). Dropping it stops the open
/// segment; the record track stays open until [`Tracks::finish`].
pub(crate) struct Recorder {
	name: String,
	live: Arc<Mutex<Live>>,
	state: Arc<Mutex<State>>,
}

impl Recorder {
	/// Run `callback` once when an unhinted track learns its fixed duration.
	pub fn on_declare(&mut self, callback: impl FnOnce(Duration) + Send + 'static) {
		self.live.lock().unwrap().on_declare = Some(Box::new(callback));
	}

	fn report(&mut self, f: impl FnOnce(&mut Segmenter)) {
		let published = {
			let mut live = self.live.lock().unwrap();
			f(&mut live.segmenter);
			live.publish()
		};
		self.lead(&published.fire());
	}

	/// Hand a pacer's marks to every other track still recording.
	fn lead(&self, marks: &[Mark]) {
		if marks.is_empty() {
			return;
		}
		let followers: Vec<_> = {
			let state = self.state.lock().unwrap();
			if state.pacer.as_deref() != Some(self.name.as_str()) {
				return;
			}
			state
				.tracks
				.iter()
				.filter(|(name, _)| **name != self.name)
				.map(|(_, (_, live))| live.clone())
				.collect()
		};
		for follower in followers {
			let published = {
				let mut live = follower.lock().unwrap();
				if !live.recording {
					continue;
				}
				for mark in marks {
					live.segmenter.follow(*mark);
				}
				live.publish()
			};
			// Only the pacer leads, so a follower's own marks go nowhere.
			published.fire();
		}
	}

	/// Report a frame of group `group`, presented at `pts`; see [`Segmenter::frame`].
	///
	/// A report the segmenter cannot place ends the record track with an error rather than
	/// publishing records that silently omit frames. Media publishing is unaffected.
	pub fn frame(&mut self, group: u64, pts: Timestamp) {
		let published = {
			let mut live = self.live.lock().unwrap();
			if let Err(err) = live.segmenter.frame(group, pts) {
				tracing::warn!(%err, "HLS report rejected; dropping the record track");
				live.segmenter.closed = true;
				// Dropped without a finish, so subscribers see the record track fail rather than end.
				live.output = None;
			}
			live.publish()
		};
		self.lead(&published.fire());
	}

	/// Report that the content extends to `pts`; see [`Segmenter::end`].
	pub fn end(&mut self, pts: Timestamp) {
		self.report(|segmenter| segmenter.end(pts));
	}

	/// Report that content stopped before group `group`; see [`Segmenter::stop`].
	pub fn stop(&mut self, group: u64) {
		self.report(|segmenter| segmenter.stop(group));
	}

	/// Declare a boundary at `pts` on this track; see [`Segmenter::cut`].
	pub fn cut(&mut self, pts: Timestamp) {
		self.report(|segmenter| segmenter.cut(pts));
	}
}

impl Drop for Recorder {
	fn drop(&mut self) {
		let published = {
			let mut live = self.live.lock().unwrap();
			if let Some(last) = live.segmenter.last {
				live.segmenter.stop(last + 1);
			}
			live.recording = false;
			let published = live.publish();
			// The callback holds the catalog, which holds these tracks: an enrollment that ends
			// without declaring must drop it, or the broadcast never closes.
			live.on_declare = None;
			published
		};
		// A pacer going away says nothing about where the others' content stops, so its last stop
		// isn't mirrored: each follower stops where its own content does, or paces on.
		published.fire();

		// The earliest enrollment still recording takes over pacing, or nobody does until the next.
		let mut state = self.state.lock().unwrap();
		if state.pacer.as_deref() != Some(self.name.as_str()) {
			return;
		}
		let next = state
			.tracks
			.iter()
			.filter_map(|(name, (_, live))| {
				let live = live.lock().unwrap();
				live.recording.then_some((live.enrolled, name.clone()))
			})
			.min();
		state.pacer = next.map(|(_, name)| name);
		if let Some(pacer) = &state.pacer {
			state.tracks[pacer].1.lock().unwrap().segmenter.pace();
		}
	}
}

/// One decoded HLS record, with real [`Timestamp`]s.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Entry {
	/// The segment's number, consecutive within its track.
	pub sequence: u64,

	/// The group the segment starts with.
	pub group: u64,

	/// The segment's start.
	pub pts: Timestamp,

	/// The segment's duration, when it does not run to the next record's pts. Zero marks where
	/// content stopped: the segment holds no media.
	pub duration: Option<Duration>,
}

impl Entry {
	/// Decode a record, converting its timing out of milliseconds.
	///
	/// A pts the timescale can't represent is an error rather than a substituted value: silently
	/// moving a timestamp would misdirect the playlist.
	pub fn decode(record: Record) -> crate::Result<Self> {
		Ok(Self {
			sequence: record.sequence,
			group: record.group,
			pts: Timestamp::new(record.pts, TIMESCALE)?,
			duration: record.duration.map(Duration::from_millis),
		})
	}

	/// Whether the segment holds no media: content stopped at its start.
	pub fn is_stop(&self) -> bool {
		self.duration.is_some_and(|duration| duration.is_zero())
	}
}

/// One change to the visible record window.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Event {
	/// A record became visible at this window index, which equals its sequence.
	Push {
		/// Absolute window index assigned to the record.
		index: u64,
		/// The decoded record.
		entry: Entry,
	},
	/// These previously visible window indices were removed.
	Pop(std::ops::Range<u64>),
	/// These indices were removed before this consumer saw them.
	Skip(std::ops::Range<u64>),
}

/// Reads one rendition's HLS records, yielding decoded [`Event`]s in window order.
pub struct Consumer {
	inner: moq_json::window::Consumer<Record>,
}

impl Consumer {
	/// Subscribe to the record track a rendition's [`hls`](hang::catalog::Hls) field names.
	pub async fn subscribe(broadcast: &moq_net::broadcast::Consumer, hls: &hang::catalog::Hls) -> crate::Result<Self> {
		let track = broadcast.track(&hls.track)?.subscribe(None).await?;
		Ok(Self {
			inner: moq_json::window::Consumer::new(track, moq_json::window::ConsumerConfig::default()),
		})
	}

	fn decode_event(event: moq_json::window::Event<Record>) -> crate::Result<Event> {
		Ok(match event {
			moq_json::window::Event::Push { index, value } => {
				// The sequence is the segment's URL and media sequence, so one that disagrees with
				// its position would list two segments under one number.
				if value.sequence != index {
					return Err(crate::Error::HlsSequence {
						expected: index,
						actual: value.sequence,
					});
				}
				Event::Push {
					index,
					entry: Entry::decode(value)?,
				}
			}
			moq_json::window::Event::Pop(range) => Event::Pop(range),
			moq_json::window::Event::Skip(range) => Event::Skip(range),
			_ => unreachable!("unknown record window event"),
		})
	}

	/// Get the next window event, or `None` once the track ends.
	pub async fn next(&mut self) -> crate::Result<Option<Event>> {
		match self.inner.next().await? {
			Some(event) => Ok(Some(Self::decode_event(event.value)?)),
			None => Ok(None),
		}
	}

	/// Poll for the next window event, without blocking.
	pub fn poll_next(&mut self, waiter: &kio::Waiter) -> Poll<crate::Result<Option<Event>>> {
		match self.inner.poll_next(waiter)? {
			// Each record carries its own pts, so the frame timestamp adds nothing.
			Poll::Ready(Some(event)) => Poll::Ready(Self::decode_event(event.value).map(Some)),
			Poll::Ready(None) => Poll::Ready(Ok(None)),
			Poll::Pending => Poll::Pending,
		}
	}
}

#[cfg(test)]
mod test {
	use super::*;

	/// Media whose encoder reports a 4s cadence, so it declares before any segment.
	fn hinted() -> Config {
		Config::default().with_duration_hint(Duration::from_secs(4))
	}

	fn ms(v: u64) -> Timestamp {
		Timestamp::from_millis(v).unwrap()
	}

	/// Each record as `(group, pts, duration)` in wire units, checking the numbering.
	fn drain(segmenter: &mut Segmenter) -> Vec<(u64, u64, Option<u64>)> {
		let mut out = Vec::new();
		for record in segmenter.by_ref() {
			assert_eq!(
				record.sequence,
				out.len() as u64 + first(&out),
				"records are numbered consecutively"
			);
			out.push((record.group, record.pts, record.duration));
		}
		out
	}

	fn first(_: &[(u64, u64, Option<u64>)]) -> u64 {
		0
	}

	/// Groups `range`, one frame each, `step` ms apart.
	fn groups(segmenter: &mut Segmenter, range: std::ops::Range<u64>, step: u64) {
		for group in range {
			segmenter.frame(group, ms(group * step)).unwrap();
		}
	}

	#[test]
	fn long_groups_get_a_segment_each() {
		let mut segmenter = Segmenter::new(Config::default());
		groups(&mut segmenter, 0..3, 5_000);
		assert_eq!(
			drain(&mut segmenter),
			[(0, 0, None), (1, 5_000, None), (2, 10_000, None)]
		);
		assert_eq!(segmenter.declared(), Some(Duration::from_secs(5)));
	}

	#[test]
	fn short_groups_pack_up_to_the_minimum() {
		let mut segmenter = Segmenter::new(Config::default());
		groups(&mut segmenter, 0..10, 1_000);
		assert_eq!(
			drain(&mut segmenter),
			[(0, 0, None), (4, 4_000, None), (8, 8_000, None)]
		);
	}

	#[test]
	fn a_hint_declares_its_smallest_multiple_at_or_past_the_minimum() {
		let segmenter = Segmenter::new(Config::default().with_duration_hint(Duration::from_millis(1_500)));
		assert_eq!(segmenter.declared(), Some(Duration::from_millis(4_500)));
		assert_eq!(Segmenter::new(hinted()).declared(), Some(Duration::from_secs(4)));
	}

	#[test]
	fn a_stopped_first_segment_declares_the_minimum() {
		let mut segmenter = Segmenter::new(Config::default());
		groups(&mut segmenter, 0..2, 1_000);
		segmenter.close();
		assert_eq!(segmenter.declared(), Some(DEFAULT_DURATION_MIN));
		assert_eq!(drain(&mut segmenter), [(0, 0, None), (2, 1_000, Some(0))]);
	}

	#[test]
	fn the_end_extends_the_stop() {
		let mut segmenter = Segmenter::new(hinted());
		groups(&mut segmenter, 0..2, 1_000);
		segmenter.end(ms(2_000));
		segmenter.close();
		assert_eq!(drain(&mut segmenter), [(0, 0, None), (2, 2_000, Some(0))]);
		// Nothing reports after the close.
		segmenter.frame(5, ms(9_000)).unwrap();
		assert!(drain(&mut segmenter).is_empty());
	}

	#[test]
	fn a_skipped_sequence_stops_the_segment() {
		let mut segmenter = Segmenter::new(hinted());
		groups(&mut segmenter, 0..2, 1_000);
		segmenter.end(ms(1_500));
		segmenter.frame(5, ms(60_000)).unwrap();
		assert_eq!(
			drain(&mut segmenter),
			[(0, 0, None), (2, 1_500, Some(0)), (5, 60_000, None)]
		);
	}

	#[test]
	fn a_stop_ends_the_segment_and_the_next_group_starts_one() {
		let mut segmenter = Segmenter::new(hinted());
		groups(&mut segmenter, 0..2, 1_000);
		segmenter.end(ms(2_000));
		segmenter.stop(2);
		segmenter.frame(3, ms(10_000)).unwrap();
		assert_eq!(
			drain(&mut segmenter),
			[(0, 0, None), (2, 2_000, Some(0)), (3, 10_000, None)]
		);
	}

	#[test]
	fn the_ceiling_ends_a_pacer_waiting_on_cuts() {
		let mut segmenter = Segmenter::new(hinted());
		segmenter.cut(ms(100_000));
		groups(&mut segmenter, 0..20, 1_000);
		// Three declared durations: the source's cut never came.
		assert_eq!(drain(&mut segmenter), [(0, 0, None), (12, 12_000, None)]);
	}

	#[test]
	fn explicit_cuts_override_the_pacing() {
		let mut segmenter = Segmenter::new(Config::default());
		segmenter.cut(ms(2_000));
		segmenter.cut(ms(3_000));
		groups(&mut segmenter, 0..5, 1_000);
		assert_eq!(
			drain(&mut segmenter),
			[(0, 0, None), (2, 2_000, None), (3, 3_000, None)]
		);
	}

	#[test]
	fn a_report_that_goes_backwards_is_refused() {
		let mut segmenter = Segmenter::new(hinted());
		segmenter.frame(3, ms(0)).unwrap();
		segmenter.frame(3, ms(10)).unwrap();
		assert!(matches!(
			segmenter.frame(2, ms(20)),
			Err(crate::Error::HlsGroup { group: 2, last: 3 })
		));
	}

	#[test]
	fn a_follower_starts_at_the_pacers_segments() {
		let mut follower = Segmenter::new(Config::default()).following();
		// Audio before the pacer's first segment is not recorded.
		follower.frame(0, ms(0)).unwrap();
		follower.follow(Mark::Start(ms(100)));
		for (group, pts) in [(1, 90), (2, 110), (3, 2_000), (4, 4_090), (5, 4_110)] {
			follower.frame(group, ms(pts)).unwrap();
		}
		// The pacer's second segment starts after the follower already reported group 5 past it,
		// which still starts the follower's.
		follower.follow(Mark::Start(ms(4_100)));
		// The follower's own source declaring the same boundary again changes nothing.
		follower.cut(ms(4_100));
		follower.frame(6, ms(4_130)).unwrap();
		follower.cut(ms(4_100));
		follower.follow(Mark::Stop);
		assert_eq!(
			drain(&mut follower),
			[(2, 110, None), (5, 4_110, None), (7, 4_130, Some(0))]
		);
	}

	#[test]
	fn a_stalled_pacer_bounds_what_its_follower_keeps() {
		let mut follower = Segmenter::new(hinted()).following();
		follower.follow(Mark::Start(ms(0)));
		// 200s of audio past the pacer's only cut.
		groups(&mut follower, 0..10_000, 20);
		// A ceiling (three 4s segments) of groups waits for the pacer.
		assert_eq!(follower.undecided.len(), 601);
		// The pacer resuming that far behind starts the follower's next segment at its oldest kept
		// group, so both still number it alike.
		follower.follow(Mark::Start(ms(5_000)));
		assert_eq!(drain(&mut follower), [(0, 0, None), (9_399, 187_980, None)]);
	}

	#[test]
	fn an_idle_follower_bounds_the_cuts_it_keeps() {
		let mut follower = Segmenter::new(hinted()).following();
		for segment in 0..10_000 {
			follower.follow(Mark::Start(ms(segment * 4_000)));
		}
		assert_eq!(follower.cuts.len(), 4);
		follower.frame(0, ms(39_996_000)).unwrap();
		assert_eq!(drain(&mut follower), [(0, 39_996_000, None)]);
	}

	// Aligned GOPs whose follower frame is reported just before the pacer's: each group the
	// follower reported ahead still starts its segment once the pacer's cut arrives.
	#[tokio::test]
	async fn a_follower_reporting_first_keeps_the_pacers_boundaries() {
		let broadcast = moq_net::broadcast::Info::new().produce();
		let tracks = Tracks::new(&broadcast);
		let mut pacer = tracks.track("video0", hinted()).unwrap();
		let mut follower = tracks.track("video1", hinted()).unwrap();
		for group in 0..4 {
			follower.frame(group, ms(group * 4_000));
			pacer.frame(group, ms(group * 4_000));
		}
		let records = |name: &str| {
			let state = tracks.state.lock().unwrap();
			let live = state.tracks[name].1.lock().unwrap();
			live.segmenter.sequence()
		};
		assert_eq!(records("video1"), records("video0"));
		drop((pacer, follower));
		tracks.finish();

		let consumer = broadcast.consume();
		let read = async |name: &str| {
			let hls = tracks.hls(name).unwrap();
			let mut consumer = Consumer::subscribe(&consumer, &hls).await.unwrap();
			let mut out = Vec::new();
			while let Some(Event::Push { entry, .. }) = consumer.next().await.unwrap() {
				out.push((entry.sequence, entry.group, Duration::from(entry.pts).as_millis()));
			}
			out
		};
		let video0 = read("video0").await;
		assert_eq!(&video0[..4], [(0, 0, 0), (1, 1, 4_000), (2, 2, 8_000), (3, 3, 12_000)]);
		assert_eq!(read("video1").await, video0);
	}

	#[tokio::test]
	async fn followers_number_segments_like_the_pacer() {
		let broadcast = moq_net::broadcast::Info::new().produce();
		let tracks = Tracks::new(&broadcast);
		let mut video = tracks.track("video", hinted()).unwrap();
		let mut audio = tracks.track("audio", hinted()).unwrap();
		// Audio runs ahead of video, grouped every 20ms.
		for step in 0..450u64 {
			let pts = step * 20;
			audio.frame(step, ms(pts + 5));
			if pts % 2_000 == 0 {
				video.frame(pts / 2_000, ms(pts));
			}
		}
		drop((video, audio));
		tracks.finish();

		let consumer = broadcast.consume();
		let read = async |name: &str| {
			let hls = tracks.hls(name).unwrap();
			let mut consumer = Consumer::subscribe(&consumer, &hls).await.unwrap();
			let mut out = Vec::new();
			while let Some(event) = consumer.next().await.unwrap() {
				let Event::Push { entry, .. } = event else {
					panic!("unexpected event {event:?}");
				};
				out.push((entry.sequence, Duration::from(entry.pts).as_millis(), entry.is_stop()));
			}
			out
		};
		let video = read("video").await;
		let audio = read("audio").await;
		assert_eq!(
			video,
			[(0, 0, false), (1, 4_000, false), (2, 8_000, false), (3, 8_000, true)]
		);
		// Each audio segment starts at its first group at or after the video segment's, though
		// audio reported that group before video reported its start.
		assert_eq!(
			audio,
			[(0, 5, false), (1, 4_005, false), (2, 8_005, false), (3, 8_985, true)]
		);
	}

	#[tokio::test]
	async fn the_next_enrollment_takes_over_pacing() {
		let broadcast = moq_net::broadcast::Info::new().produce();
		let tracks = Tracks::new(&broadcast);
		let video = tracks.track("video", hinted()).unwrap();
		let mut audio = tracks.track("audio", hinted()).unwrap();
		drop(video);
		groups_on(&mut audio, 0..9, 1_000);
		let live = tracks.state.lock().unwrap().tracks["audio"].1.clone();
		let next = live.lock().unwrap().segmenter.sequence();
		assert_eq!(next, 3, "audio paced its own segments at 0, 4s, and 8s");
	}

	fn groups_on(recorder: &mut Recorder, range: std::ops::Range<u64>, step: u64) {
		for group in range {
			recorder.frame(group, ms(group * step));
		}
	}

	#[test]
	fn an_unhinted_track_is_advertised_once_declared() {
		let broadcast = moq_net::broadcast::Info::new().produce();
		let tracks = Tracks::new(&broadcast);
		let mut recorder = tracks.track("video", Config::default()).unwrap();
		recorder.frame(0, ms(0));
		assert_eq!(tracks.hls("video"), None);
		recorder.frame(1, ms(5_000));
		assert_eq!(
			tracks.hls("video"),
			Some(hang::catalog::Hls::new("video.hls", Duration::from_secs(5)))
		);
	}

	#[tokio::test]
	async fn a_rejected_report_fails_the_record_track() {
		let broadcast = moq_net::broadcast::Info::new().produce();
		let tracks = Tracks::new(&broadcast);
		let mut recorder = tracks.track("video", hinted()).unwrap();
		let hls = tracks.hls("video").unwrap();
		let mut consumer = Consumer::subscribe(&broadcast.consume(), &hls).await.unwrap();
		recorder.frame(1, ms(0));
		recorder.frame(0, ms(1_000));
		let mut next = consumer.next().await;
		while let Ok(Some(_)) = next {
			next = consumer.next().await;
		}
		assert!(next.is_err(), "the track fails rather than ends");
	}

	#[tokio::test]
	async fn re_enrolling_continues_the_numbering() {
		let broadcast = moq_net::broadcast::Info::new().produce();
		let tracks = Tracks::new(&broadcast);
		let mut recorder = tracks.track("video", hinted()).unwrap();
		assert!(tracks.track("video", hinted()).is_err(), "its recorder is still alive");
		recorder.frame(0, ms(0));
		drop(recorder);
		let mut recorder = tracks.track("video", hinted()).unwrap();
		// The new producer restarts its group sequences.
		recorder.frame(0, ms(10_000));
		drop(recorder);
		tracks.finish();

		let hls = tracks.hls("video").unwrap();
		let mut consumer = Consumer::subscribe(&broadcast.consume(), &hls).await.unwrap();
		let mut records = Vec::new();
		while let Some(Event::Push { entry, .. }) = consumer.next().await.unwrap() {
			records.push((entry.sequence, entry.group, entry.is_stop()));
		}
		assert_eq!(records, [(0, 0, false), (1, 1, true), (2, 0, false), (3, 1, true)]);
	}

	#[test]
	fn a_taken_record_track_name_fails_enrollment() {
		let broadcast = moq_net::broadcast::Info::new().produce();
		let _taken = broadcast.create_track("video.hls", None).unwrap();
		let tracks = Tracks::new(&broadcast);
		assert!(tracks.track("video", Config::default()).is_err());
	}

	#[test]
	fn enrollment_after_finish_is_refused() {
		let broadcast = moq_net::broadcast::Info::new().produce();
		let tracks = Tracks::new(&broadcast);
		tracks.finish();
		assert!(tracks.track("video", Config::default()).is_err());
	}

	#[tokio::test]
	async fn a_record_out_of_place_is_refused() {
		let broadcast = moq_net::broadcast::Info::new().produce();
		let track = broadcast.create_track("video.hls", Producer::info()).unwrap();
		let mut sink = moq_json::window::Producer::<Record>::new(track, moq_json::window::ProducerConfig::default());
		sink.push(moq_net::Timed::from(&Record::new(1, 0, 0)).at(Timestamp::ZERO))
			.unwrap();
		let hls = hang::catalog::Hls::new("video.hls", Duration::from_secs(4));
		let mut consumer = Consumer::subscribe(&broadcast.consume(), &hls).await.unwrap();
		assert!(matches!(
			consumer.next().await,
			Err(crate::Error::HlsSequence { expected: 0, actual: 1 })
		));
	}
}
