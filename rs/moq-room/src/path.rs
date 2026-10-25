//! Room path convention: identity is everything before the last segment, and
//! that last segment is the broadcast kind (`camera` or `screen`).
//!
//! `alice/camera`, `alice/camera.hang`, and `guest/uuid/screen` are all valid.
//! A `.hang` suffix on the kind is optional and equivalent.

use moq_net::{AsPath, Path, PathOwned};

/// The two broadcasts each participant may publish.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
	/// Camera plus microphone.
	Camera,
	/// Screenshare; its announce/unannounce is the share lifecycle.
	Screen,
}

impl Kind {
	/// The path segment this kind publishes as, without a catalog-format suffix.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Camera => "camera",
			Self::Screen => "screen",
		}
	}
}

/// An announced path split into identity and kind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Parsed {
	/// Path prefix identifying the participant; may be more than one segment.
	pub identity: PathOwned,
	/// `camera` (camera + mic) or `screen` (screenshare).
	pub kind: Kind,
}

/// Strip an optional `.hang` catalog-format suffix from a path segment.
pub fn kind_from_segment(segment: &str) -> Option<Kind> {
	let kind = segment.strip_suffix(".hang").unwrap_or(segment);
	match kind {
		"camera" => Some(Kind::Camera),
		"screen" => Some(Kind::Screen),
		_ => None,
	}
}

/// Split a room-relative broadcast path into identity and kind.
///
/// Returns `None` when there is no identity, or the last segment is not a kind.
pub fn parse(path: impl AsPath) -> Option<Parsed> {
	let path = path.as_path();
	let (identity, last) = path.as_str().rsplit_once('/')?;
	let kind = kind_from_segment(last)?;
	let identity = Path::new(identity).ok()?.to_owned();
	Some(Parsed { identity, kind })
}

/// The broadcast path a participant publishes for `kind`.
pub fn broadcast_path(identity: impl AsPath, kind: Kind) -> PathOwned {
	let segment = PathOwned::try_from(format!("{}.hang", kind.as_str())).expect("a kind is one segment");
	identity.as_path().join(segment)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn p(s: &str) -> PathOwned {
		Path::new(s).unwrap().to_owned()
	}

	#[test]
	fn parse_splits_identity_and_kind() {
		assert_eq!(
			parse(Path::new("alice/camera").unwrap()),
			Some(Parsed {
				identity: p("alice"),
				kind: Kind::Camera
			})
		);
		assert_eq!(
			parse(Path::new("alice/screen").unwrap()),
			Some(Parsed {
				identity: p("alice"),
				kind: Kind::Screen
			})
		);
	}

	#[test]
	fn parse_accepts_hang_suffix() {
		assert_eq!(
			parse(Path::new("alice/camera.hang").unwrap()).map(|p| p.kind),
			Some(Kind::Camera)
		);
		assert_eq!(
			parse(Path::new("alice/screen.hang").unwrap()).map(|p| p.kind),
			Some(Kind::Screen)
		);
	}

	#[test]
	fn parse_keeps_multi_segment_identity() {
		assert_eq!(
			parse(Path::new("guest/uuid/camera").unwrap()),
			Some(Parsed {
				identity: p("guest/uuid"),
				kind: Kind::Camera
			})
		);
	}

	#[test]
	fn parse_rejects_unknown() {
		assert_eq!(parse(Path::new("alice").unwrap()), None);
		assert_eq!(parse(Path::new("alice/chat").unwrap()), None);
		assert_eq!(parse(Path::new("camera").unwrap()), None);
		assert_eq!(parse(Path::empty()), None);
	}

	#[test]
	fn broadcast_path_joins_with_hang_suffix() {
		assert_eq!(broadcast_path(p("alice"), Kind::Camera).as_str(), "alice/camera.hang");
		assert_eq!(
			broadcast_path(p("guest/uuid"), Kind::Screen).as_str(),
			"guest/uuid/screen.hang"
		);
	}
}
