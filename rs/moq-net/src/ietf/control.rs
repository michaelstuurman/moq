use std::collections::BTreeMap;

use crate::{Error, SessionError, ietf::RequestId};

/// The peer's spent request IDs: every ID of its parity below `next`, except the skipped
/// `start..end` runs in `missing`.
///
/// Independent streams can reorder, so a peer's IDs need not arrive in sequence. A jump
/// costs one run however far it skips, and only a request landing inside a run splits it,
/// so the runs never outnumber the requests received.
struct Spent {
	next: u64,
	missing: BTreeMap<u64, u64>,
}

impl Spent {
	fn new(first: u64) -> Self {
		Self {
			next: first,
			missing: BTreeMap::new(),
		}
	}

	/// The skipped run holding `id`, if any.
	fn run(&self, id: u64) -> Option<(u64, u64)> {
		let (&start, &end) = self.missing.range(..=id).next_back()?;
		(id < end).then_some((start, end))
	}

	/// Whether `id` has the peer's parity and was never spent.
	///
	/// No peer reaches 2^62 requests, and refusing the IDs past it keeps `next` from
	/// overflowing on a varint that can carry a full 64 bits.
	fn unused(&self, id: u64) -> bool {
		id < 1 << 62 && id % 2 == self.next % 2 && (id >= self.next || self.run(id).is_some())
	}

	/// Whether spending `id` would record one more run.
	fn splits(&self, id: u64) -> bool {
		match self.run(id) {
			Some((start, end)) => start < id && id + 2 < end,
			None => id > self.next,
		}
	}

	/// Spend an [unused](Self::unused) `id`.
	fn spend(&mut self, id: u64) {
		if let Some((start, end)) = self.run(id) {
			self.missing.remove(&start);
			if start < id {
				self.missing.insert(start, id);
			}
			if id + 2 < end {
				self.missing.insert(id + 2, end);
			}
		} else if id > self.next {
			self.missing.insert(self.next, id);
		}
		self.next = self.next.max(id + 2);
	}
}

/// The most runs of skipped IDs a peer may leave before the session closes.
///
/// Each request holds a stream, so stream credit bounds the live ones, but a skipped ID
/// stays remembered after the requests around it end. Forgetting one would make it usable
/// again, so a peer that keeps skipping loses the session instead. Matches `@moq/net`.
const HISTORY: u64 = 240_000;

struct ControlState {
	/// Our own role.
	client: bool,
	request_id_next: RequestId,
	/// The peer's spent IDs, which no window bounds.
	spent: Spent,
	/// The most runs `spent` may hold. Forgetting one would make its IDs usable again.
	history: u64,
	/// A GOAWAY has frozen admission of new peer requests.
	going_away: bool,
}

#[derive(Clone)]
pub(super) struct Control {
	state: kio::Shared<ControlState>,
}

impl Control {
	pub fn new(client: bool) -> Self {
		Self {
			state: kio::Shared::new(ControlState {
				client,
				request_id_next: if client { RequestId(0) } else { RequestId(1) },
				// The peer takes the other parity: a client's IDs are even, a server's odd.
				spent: Spent::new(u64::from(client)),
				history: HISTORY,
				going_away: false,
			}),
		}
	}

	/// Whether we opened the session.
	pub fn client(&self) -> bool {
		self.state.lock().client
	}

	/// Bound the runs of skipped IDs the peer may leave, closing the session with
	/// INTERNAL_ERROR past `limit` rather than forgetting any.
	#[cfg(test)]
	pub fn with_history(self, limit: u64) -> Self {
		self.state.lock().history = limit;
		self
	}

	/// Freeze admission and name the smallest peer ID not admitted: the first one a
	/// reordered stream skipped, or else the next.
	pub fn goaway(&self) -> Result<RequestId, Error> {
		let mut state = self.state.lock();
		let spent = &state.spent;
		let cutoff = spent.missing.keys().next().copied().unwrap_or(spent.next);
		if cutoff >= 1 << 62 {
			return Err(SessionError::Internal.into());
		}
		state.going_away = true;
		Ok(RequestId(cutoff))
	}

	/// Admit a request the peer opened.
	///
	/// Used IDs close with INVALID_REQUEST_ID even after retirement, and every ID once
	/// GOAWAY froze admission is refused with [`Error::GoingAway`].
	pub fn accept(&self, id: RequestId) -> Result<(), Error> {
		let mut state = self.state.lock();
		let state = &mut *state;
		let spent = &mut state.spent;
		if !spent.unused(id.0) {
			return Err(SessionError::InvalidRequestId.into());
		}
		if spent.splits(id.0) && spent.missing.len() as u64 >= state.history {
			tracing::warn!(limit = state.history, "request ID history exhausted");
			return Err(SessionError::Internal.into());
		}
		spent.spend(id.0);
		match state.going_away {
			true => Err(Error::GoingAway),
			false => Ok(()),
		}
	}

	/// Allocate the next request ID.
	pub fn next_request_id(&self) -> RequestId {
		self.state.lock().request_id_next.increment()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn goaway_freezes_request_admission_at_current_progress() {
		for client in [false, true] {
			let control = Control::new(client);
			let first = u64::from(client);
			assert_eq!(control.goaway().unwrap(), RequestId(first));
			assert!(matches!(control.accept(RequestId(first)), Err(Error::GoingAway)));

			let control = Control::new(client);
			control.accept(RequestId(first)).unwrap();
			control.accept(RequestId(first + 2)).unwrap();
			assert_eq!(control.goaway().unwrap(), RequestId(first + 4));

			// A reordered stream skipped first + 2, which is the smallest ID not processed.
			let control = Control::new(client);
			control.accept(RequestId(first)).unwrap();
			control.accept(RequestId(first + 4)).unwrap();
			assert_eq!(control.goaway().unwrap(), RequestId(first + 2));
			// The skipped ID may still arrive after GOAWAY, and is past the cutoff too.
			for id in [first + 2, first + 6, first + 8] {
				assert!(matches!(control.accept(RequestId(id)), Err(Error::GoingAway)));
			}
			assert!(matches!(
				control.accept(RequestId(first)),
				Err(Error::Session(SessionError::InvalidRequestId))
			));
		}
	}

	#[test]
	fn requests_reject_reuse_and_wrong_parity() {
		for client in [false, true] {
			let control = Control::new(client);
			let first = u64::from(client);
			for id in [first + 4, first, first + 2] {
				control.accept(RequestId(id)).unwrap();
			}
			for id in [first, first + 2, first + 4, first ^ 1] {
				assert!(
					matches!(
						control.accept(RequestId(id)),
						Err(Error::Session(SessionError::InvalidRequestId))
					),
					"accepted spent or wrong-parity ID {id}"
				);
			}
		}
	}

	/// Contiguous arrival in either order leaves no runs, and a full history refuses a new
	/// gap rather than forgetting an old one.
	#[test]
	fn history_coalesces_and_never_forgets() {
		for reversed in [false, true] {
			let control = Control::new(false).with_history(1);
			for n in 0..4096 {
				let id = if reversed { 4095 - n } else { n } * 2;
				control.accept(RequestId(id)).unwrap();
			}
			assert!(control.state.lock().spent.missing.is_empty());
			assert!(matches!(
				control.accept(RequestId(0)),
				Err(Error::Session(SessionError::InvalidRequestId))
			));
		}
		let control = Control::new(false).with_history(1);
		control.accept(RequestId(0)).unwrap();
		control.accept(RequestId(4)).unwrap();
		assert!(matches!(
			control.accept(RequestId(8)),
			Err(Error::Session(SessionError::Internal))
		));
		control.accept(RequestId(2)).unwrap();
		control.accept(RequestId(8)).unwrap();
		for id in [0, 2, 4, 8] {
			assert!(matches!(
				control.accept(RequestId(id)),
				Err(Error::Session(SessionError::InvalidRequestId))
			));
		}
	}

	#[test]
	fn sparse_ids_do_not_allocate_the_gap() {
		let control = Control::new(false).with_history(1);
		let top = (1 << 62) - 2;
		control.accept(RequestId(top)).unwrap();
		control.accept(RequestId(0)).unwrap();
		let runs: Vec<_> = control.state.lock().spent.missing.clone().into_iter().collect();
		assert_eq!(runs, [(2, top)]);
		for id in [top, 1 << 62, u64::MAX - 1] {
			assert!(matches!(
				control.accept(RequestId(id)),
				Err(Error::Session(SessionError::InvalidRequestId))
			));
		}
	}
}
