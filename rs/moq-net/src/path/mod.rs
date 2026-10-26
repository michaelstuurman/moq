//! Broadcast paths and the patterns that match them.
//!
//! [`Path`] is a literal coordinate: `/`-separated non-empty segments, with
//! segment-aware prefix operations. [`Pattern`] describes a set of paths with
//! wildcards, and [`Patterns`] is a union of them reduced by containment. The
//! grammar and algebra live in [`moq-pattern`](moq_pattern); this module
//! re-exports them beside [`Path`] so grants, origin scopes, and announce
//! interests can share one dialect.

pub use moq_pattern::{InvalidPattern, Pattern, Patterns, Segment, Specificity};

use std::borrow::Cow;
use std::fmt::{self, Display};
use std::sync::Arc;

use crate::Error;
use crate::coding::{Decode, DecodeError, Decoder, Encode, EncodeError, Encoder};

// An empty segment would give one path several spellings, so it is refused rather than collapsed.
fn validate(s: &str) -> Result<(), Error> {
	if !s.is_empty() && (s.starts_with('/') || s.ends_with('/') || s.contains("//")) {
		return Err(Error::EmptySegment);
	}
	Ok(())
}

/// An owned version of [`Path`] with a `'static` lifetime.
pub type PathOwned = Path<'static>;

/// A trait for types that can be borrowed as a [`Path`].
///
/// Only paths implement it, so a string goes through [`Path::new`] and its
/// validation before it can name a broadcast.
pub trait AsPath {
	/// Borrow `self` as a [`Path`] without copying.
	fn as_path(&self) -> Path<'_>;
}

impl<'a> AsPath for &'a Path<'a> {
	fn as_path(&self) -> Path<'a> {
		self.borrow()
	}
}

impl AsPath for Path<'_> {
	fn as_path(&self) -> Path<'_> {
		self.borrow()
	}
}

/// A borrowed slice of the path, or a suffix of a shared reference-counted buffer.
///
/// The `Shared` variant is what makes owned paths cheap: cloning bumps a refcount and
/// suffix operations (strip_prefix) only advance `start`, so one allocation
/// serves every copy of a path as it fans out to consumers.
#[derive(Clone)]
enum Repr<'a> {
	Borrowed(&'a str),
	Shared { buf: Arc<str>, start: usize },
}

/// A broadcast path that provides safe prefix matching operations.
///
/// This type wraps a string but provides path-aware operations that respect
/// delimiter boundaries, preventing issues like "foo" matching "foobar".
///
/// A path is a sequence of non-empty segments joined by `/`, so it has one spelling:
/// [`Path::new`] refuses a leading, trailing, or doubled `/` rather than collapsing it.
/// The empty path has no segments; it is the origin's root and prefixes every other
/// path. The same type names an exact broadcast and the prefix a route or announcement
/// covers, so a broadcast's own path is the prefix it announces, and a prefix needs no
/// trailing `/`. See [`Relative`] for `..`-style references.
///
/// Owned paths ([`PathOwned`]) share one reference-counted allocation: cloning, converting
/// a shared path with [`Path::to_owned`], and suffix operations like [`Path::strip_prefix`]
/// do not copy the underlying bytes.
///
/// # Examples
/// ```
/// use moq_net::{Path};
///
/// // An empty segment is refused, not collapsed.
/// assert!(Path::new("/foo/bar/").is_err());
/// assert!(Path::new("foo//bar").is_err());
///
/// let base = Path::new("api/v1")?;
/// assert!(base.has_prefix(Path::new("api")?));
/// assert!(base.has_prefix(Path::empty()));
///
/// let joined = base.join(Path::new("users")?);
/// assert_eq!(joined.as_str(), "api/v1/users");
/// # Ok::<(), moq_net::Error>(())
/// ```
#[derive(Clone)]
pub struct Path<'a>(Repr<'a>);

impl<'a> Path<'a> {
	/// Maximum number of slash-separated parts in a path.
	///
	/// Matches the IETF moq-transport limit of 32 fields in a namespace tuple.
	/// moq-lite enforces the same bound: encoding or decoding a deeper path fails,
	/// and publishing one to an origin is rejected.
	pub const MAX_PARTS: usize = 32;

	/// Borrow a string as a path, without copying.
	///
	/// The empty string is the root path. Anything else must be non-empty segments
	/// joined by `/`: a leading, trailing, or doubled `/` returns [`Error::EmptySegment`].
	pub fn new(s: &'a str) -> Result<Self, Error> {
		validate(s)?;
		Ok(Self(Repr::Borrowed(s)))
	}

	// A string the caller already knows is valid, such as a run of another path's segments.
	pub(crate) fn from_valid(s: &'a str) -> Self {
		debug_assert!(validate(s).is_ok(), "invalid path {s:?}");
		Self(Repr::Borrowed(s))
	}

	#[cfg_attr(not(feature = "async"), allow(dead_code, reason = "async-only"))]
	pub(crate) fn from_escaped(s: String) -> PathOwned {
		if s.is_empty() {
			Path::empty()
		} else {
			Path(Repr::Shared {
				buf: s.into(),
				start: 0,
			})
		}
	}

	// A copy of this path skipping the first `n` bytes, reusing the shared buffer when possible.
	pub(crate) fn slice_from(&'a self, n: usize) -> Path<'a> {
		match &self.0 {
			Repr::Borrowed(s) => Path(Repr::Borrowed(&s[n..])),
			Repr::Shared { buf, start } => Path(Repr::Shared {
				buf: buf.clone(),
				start: start + n,
			}),
		}
	}

	/// Check if this path has the given prefix, respecting path boundaries.
	///
	/// Unlike String::starts_with, this ensures that "foo" does not match "foobar".
	/// The prefix must either:
	/// - Be exactly equal to this path
	/// - Be followed by a '/' delimiter in the original path
	/// - Be empty (matches everything)
	///
	/// # Examples
	/// ```
	/// use moq_net::Path;
	///
	/// let path = Path::new("foo/bar")?;
	/// assert!(path.has_prefix(Path::new("foo")?));
	/// assert!(!path.has_prefix(Path::new("fo")?));
	///
	/// let path = Path::new("foobar")?;
	/// assert!(!path.has_prefix(Path::new("foo")?));
	/// # Ok::<(), moq_net::Error>(())
	/// ```
	pub fn has_prefix(&self, prefix: impl AsPath) -> bool {
		let prefix = prefix.as_path();

		if prefix.is_empty() {
			return true;
		}

		let s = self.as_str();
		if !s.starts_with(prefix.as_str()) {
			return false;
		}

		// Check if the prefix is the exact match
		if s.len() == prefix.len() {
			return true;
		}

		// Otherwise, ensure the character after the prefix is a delimiter
		s.as_bytes().get(prefix.len()) == Some(&b'/')
	}

	/// The remainder after removing `prefix`, or `None` if it isn't a prefix.
	///
	/// Only whole segments match: `a/bc` is not prefixed by `a/b`. An empty prefix
	/// returns the whole path.
	pub fn strip_prefix(&'a self, prefix: impl AsPath) -> Option<Path<'a>> {
		let prefix = prefix.as_path();

		if prefix.is_empty() {
			return Some(self.borrow());
		}

		let s = self.as_str();
		if !s.starts_with(prefix.as_str()) {
			return None;
		}

		// Check if the prefix is the exact match
		if s.len() == prefix.len() {
			return Some(Path::empty());
		}

		// Otherwise, ensure the character after the prefix is a delimiter
		if s.as_bytes().get(prefix.len()) != Some(&b'/') {
			return None;
		}

		Some(self.slice_from(prefix.len() + 1))
	}

	/// Iterate over the slash-separated parts of the path.
	///
	/// The empty path has no parts.
	///
	/// # Examples
	/// ```
	/// use moq_net::Path;
	///
	/// let path = Path::new("foo/bar/baz")?;
	/// assert_eq!(path.parts().collect::<Vec<_>>(), ["foo", "bar", "baz"]);
	/// assert_eq!(Path::empty().parts().count(), 0);
	/// # Ok::<(), moq_net::Error>(())
	/// ```
	pub fn parts(&self) -> impl Iterator<Item = &str> {
		// Segments are never empty, but splitting the empty path would yield one.
		let s = self.as_str();
		(!s.is_empty()).then(|| s.split('/')).into_iter().flatten()
	}

	/// Whether a segment starts with `.`, which hides the path from announce
	/// discovery below the requested prefix.
	pub(crate) fn is_hidden(&self) -> bool {
		self.parts().any(|part| part.starts_with('.'))
	}

	/// The path as a string, with no leading or trailing slash.
	pub fn as_str(&self) -> &str {
		match &self.0 {
			Repr::Borrowed(s) => s,
			Repr::Shared { buf, start } => &buf[*start..],
		}
	}

	/// The empty path, which prefixes every other path.
	pub fn empty() -> Path<'static> {
		Path(Repr::Borrowed(""))
	}

	/// Returns `true` if this is the empty path.
	pub fn is_empty(&self) -> bool {
		self.as_str().is_empty()
	}

	/// The length in bytes, not segments.
	pub fn len(&self) -> usize {
		self.as_str().len()
	}

	/// Clone into a `'static` path, sharing the existing buffer when there is one.
	pub fn to_owned(&self) -> PathOwned {
		match &self.0 {
			Repr::Borrowed("") => Path::empty(),
			Repr::Borrowed(s) => Path(Repr::Shared {
				buf: Arc::from(*s),
				start: 0,
			}),
			Repr::Shared { buf, start } => Path(Repr::Shared {
				buf: buf.clone(),
				start: *start,
			}),
		}
	}

	/// Consume into a `'static` path, reusing the existing buffer when there is one.
	pub fn into_owned(self) -> PathOwned {
		match self.0 {
			Repr::Borrowed("") => Path::empty(),
			Repr::Borrowed(s) => Path(Repr::Shared {
				buf: Arc::from(s),
				start: 0,
			}),
			Repr::Shared { buf, start } => Path(Repr::Shared { buf, start }),
		}
	}

	/// A copy of this path bound to `self`'s lifetime, without copying the underlying bytes.
	pub fn borrow(&'a self) -> Path<'a> {
		self.slice_from(0)
	}

	/// Join this path with another path component.
	///
	/// # Examples
	/// ```
	/// use moq_net::Path;
	///
	/// let base = Path::new("foo")?;
	/// assert_eq!(base.join(Path::new("bar")?).as_str(), "foo/bar");
	///
	/// // The empty path is the identity.
	/// assert_eq!(base.join(Path::empty()).as_str(), "foo");
	/// # Ok::<(), moq_net::Error>(())
	/// ```
	pub fn join(&self, other: impl AsPath) -> PathOwned {
		let other = other.as_path();

		if self.is_empty() {
			other.to_owned()
		} else if other.is_empty() {
			self.to_owned()
		} else {
			// Neither side has a leading or trailing slash, so exactly one joins them.
			Path(Repr::Shared {
				buf: format!("{}/{}", self.as_str(), other.as_str()).into(),
				start: 0,
			})
		}
	}

	/// Resolve a [`Relative`] against this path.
	///
	/// A non-empty reference replaces the last segment of the base, matching relative URL
	/// resolution. `..` segments then pop another segment; other segments are appended.
	/// Excess `..` is a no-op once the base is empty (subsequent named segments still append).
	/// An empty `rel` returns this path as an owned copy.
	///
	/// [`Relative::new`] strips empty and redundant `.` segments, but preserves a lone `.`
	/// so it can reference the base's parent.
	///
	/// # Examples
	/// ```
	/// use moq_net::{Path, path::Relative};
	///
	/// let base = Path::new("a/b/c")?;
	/// assert_eq!(base.resolve(&Relative::new("./d")).as_str(), "a/b/d");
	/// assert_eq!(base.resolve(&Relative::new(".")).as_str(), "a/b");
	/// assert_eq!(base.resolve(&Relative::new("../d")).as_str(), "a/d");
	/// # Ok::<(), moq_net::Error>(())
	/// ```
	pub fn resolve(&self, rel: &Relative<'_>) -> PathOwned {
		if rel.is_empty() {
			return self.to_owned();
		}

		let mut segments: Vec<&str> = self.parts().collect();
		segments.pop();

		for seg in rel.as_str().split('/') {
			if seg == "." {
				continue;
			} else if seg == ".." {
				segments.pop();
			} else {
				segments.push(seg);
			}
		}

		let path = segments.join("/");
		if path.is_empty() {
			Path::empty()
		} else {
			Path(Repr::Shared {
				buf: path.into(),
				start: 0,
			})
		}
	}

	/// Resolve a [`Relative`], returning `None` if it escapes above the root.
	///
	/// Unlike [`Path::resolve`], this distinguishes a valid reference to the empty root
	/// path from excess `..` segments. Use it when an untrusted relative reference must
	/// not be clamped to the root.
	pub fn try_resolve(&self, rel: &Relative<'_>) -> Option<PathOwned> {
		if rel.is_empty() {
			return Some(self.to_owned());
		}

		let mut segments: Vec<&str> = self.parts().collect();
		segments.pop();

		for seg in rel.as_str().split('/') {
			if seg == "." {
				continue;
			} else if seg == ".." {
				segments.pop()?;
			} else {
				segments.push(seg);
			}
		}

		let path = segments.join("/");
		if path.is_empty() {
			Some(Path::empty())
		} else {
			Some(Path(Repr::Shared {
				buf: path.into(),
				start: 0,
			}))
		}
	}

	/// Express this path relative to `base`: the inverse of [`Path::resolve`].
	///
	/// The result round-trips (`base.resolve(&rel) == self`) and never walks above the
	/// root, so [`Path::try_resolve`] accepts it too.
	///
	/// A relative reference replaces the last segment of the base, matching relative URL
	/// resolution, so a target nested under the base repeats the base's own last segment.
	///
	/// The empty reference names the base itself, so that is what a self-reference returns.
	///
	/// Returns `None` for a target no reference can name: a path segment may literally be
	/// `.` or `..`, which resolution reads as navigation instead of as a name. Only the
	/// segments past the shared prefix matter, since the rest are never emitted.
	///
	/// # Examples
	/// ```
	/// use moq_net::Path;
	///
	/// // The base names a broadcast, so its last segment is replaced, not descended into.
	/// let base = Path::new("a/b")?;
	/// assert_eq!(Path::new("a/b/c")?.relative(&base).unwrap().as_str(), "b/c");
	/// assert_eq!(Path::new("a/c")?.relative(&base).unwrap().as_str(), "c");
	/// assert_eq!(Path::new("c")?.relative(&base).unwrap().as_str(), "../c");
	///
	/// // The lone `.` names the base's parent, which the empty reference cannot.
	/// assert_eq!(Path::new("a")?.relative(&base).unwrap().as_str(), ".");
	///
	/// // The base itself.
	/// assert_eq!(Path::new("a/b")?.relative(&base).unwrap().as_str(), "");
	///
	/// // A segment named `..` is a legal path component but an unnameable target.
	/// assert!(Path::new("a/..")?.relative(&base).is_none());
	/// # Ok::<(), moq_net::Error>(())
	/// ```
	pub fn relative(&self, base: impl AsPath) -> Option<RelativeOwned> {
		let base = base.as_path();

		// Only the empty reference can name a base whose last segment is itself `.` or `..`,
		// since resolution replaces that segment rather than emitting it.
		if *self == base {
			return Some(Relative::empty());
		}

		// Resolution replaces the base's last segment, so walk from its parent.
		let mut dir: Vec<&str> = base.parts().collect();
		dir.pop();

		let target: Vec<&str> = self.parts().collect();
		let common = dir.iter().zip(&target).take_while(|(a, b)| a == b).count();

		let down = &target[common..];
		if down.iter().any(|part| *part == "." || *part == "..") {
			// Resolution would walk on these instead of naming them.
			return None;
		}

		let mut rel: Vec<&str> = vec![".."; dir.len() - common];
		rel.extend(down);

		if rel.is_empty() {
			// An empty reference resolves to the base itself, so name the parent explicitly.
			return Some(Relative::new("."));
		}

		Some(RelativeOwned::from(rel.join("/")))
	}
}

// Comparisons, ordering, and hashing all go through `as_str()` so a borrowed and a
// shared path with the same content behave identically (e.g. as map keys).
impl<'b> PartialEq<Path<'b>> for Path<'_> {
	fn eq(&self, other: &Path<'b>) -> bool {
		self.as_str() == other.as_str()
	}
}

impl Eq for Path<'_> {}

impl PartialOrd for Path<'_> {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Path<'_> {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.as_str().cmp(other.as_str())
	}
}

impl std::hash::Hash for Path<'_> {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.as_str().hash(state)
	}
}

impl fmt::Debug for Path<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_tuple("Path").field(&self.as_str()).finish()
	}
}

impl serde::Serialize for Path<'_> {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_str(self.as_str())
	}
}

impl<'a> TryFrom<&'a str> for Path<'a> {
	type Error = Error;

	fn try_from(s: &'a str) -> Result<Self, Error> {
		Self::new(s)
	}
}

impl<'a> TryFrom<&'a String> for Path<'a> {
	type Error = Error;

	fn try_from(s: &'a String) -> Result<Self, Error> {
		Self::new(s)
	}
}

impl TryFrom<String> for Path<'_> {
	type Error = Error;

	/// Take ownership of the string as a path, refusing it like [`Path::new`].
	fn try_from(s: String) -> Result<Self, Error> {
		validate(&s)?;
		Ok(if s.is_empty() {
			Path::empty()
		} else {
			Path(Repr::Shared {
				buf: s.into(),
				start: 0,
			})
		})
	}
}

impl std::str::FromStr for PathOwned {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Error> {
		Ok(Path::new(s)?.into_owned())
	}
}

impl Default for Path<'_> {
	fn default() -> Self {
		Path::empty()
	}
}

impl AsRef<str> for Path<'_> {
	fn as_ref(&self) -> &str {
		self.as_str()
	}
}

impl Display for Path<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.as_str())
	}
}

impl<V> Decode<V> for Path<'_> {
	fn decode(r: &mut Decoder<'_>, _: V) -> Result<Self, DecodeError> {
		// Borrow the wire bytes until the path is known good, then copy them once.
		let path = Path::new(r.str()?).map_err(|_| DecodeError::InvalidValue)?;
		if path.parts().count() > Path::MAX_PARTS {
			return Err(DecodeError::BoundsExceeded);
		}
		Ok(path.into_owned())
	}
}

impl<V> Encode<V> for Path<'_> {
	fn encode(&self, w: &mut Encoder<'_>, _: V) -> Result<(), EncodeError> {
		if self.parts().count() > Path::MAX_PARTS {
			return Err(EncodeError::BoundsExceeded);
		}
		w.string(self.as_str())
	}
}

/// A grant's patterns as a count and each one's canonical text, the one encoding both
/// wires' AUTH_OK share.
impl<V> Encode<V> for Patterns {
	fn encode(&self, w: &mut Encoder<'_>, _: V) -> Result<(), EncodeError> {
		w.varint(self.len() as u64)?;
		for pattern in self {
			w.string(pattern.as_str())?;
		}
		Ok(())
	}
}

impl<V> Decode<V> for Patterns {
	fn decode(r: &mut Decoder<'_>, _: V) -> Result<Self, DecodeError> {
		let count = r.varint()?;
		// No preallocation: the count is peer-controlled, and the message size limit
		// is what bounds how many patterns actually fit.
		let mut patterns = Vec::new();
		for _ in 0..count {
			let text = r.string()?;
			let pattern = Pattern::try_from(text.as_str()).map_err(|_| DecodeError::InvalidValue)?;
			// Only the canonical spelling is valid, so each pattern has one encoding.
			if pattern.as_str() != text {
				return Err(DecodeError::InvalidValue);
			}
			patterns.push(pattern);
		}
		// Reduce once, so a peer's count never turns into quadratic work.
		Ok(patterns.into_iter().collect())
	}
}

/// An owned version of [`Relative`] with a `'static` lifetime.
pub type RelativeOwned = Relative<'static>;

/// A relative broadcast path, used to reference one broadcast from another broadcast's content.
///
/// Unlike [`Path`] (which is a complete reference within the broadcast namespace),
/// `Relative` may contain `.` and `..` segments to walk the namespace and is meaningful
/// only when resolved against a base [`Path`] via [`Path::resolve`]. The hang catalog uses it
/// to point a rendition at a track published in a sibling broadcast (e.g. `./source`).
///
/// `Relative` has no `Encode`/`Decode` impl, so it never appears in announce/subscribe
/// frames. It does serialize via serde for off-wire use (e.g. as a field inside a catalog
/// JSON payload, which itself travels as a track).
///
/// Normalization on creation: leading/trailing slashes are trimmed, consecutive internal
/// slashes collapse to one, and redundant `.` segments are stripped. A reference made only
/// of `.` segments normalizes to `.` rather than empty because `.` resolves to the base's
/// parent while empty resolves to the base itself. `..` is preserved for resolve time.
///
/// # Examples
/// ```
/// use moq_net::{Path, path::Relative};
///
/// let rel = Relative::new("./source");
/// assert_eq!(Path::new("a/b")?.resolve(&rel).as_str(), "a/source");
///
/// // Redundant `.` segments are stripped on creation.
/// assert_eq!(Relative::new("./a/./b").as_str(), "a/b");
/// assert_eq!(Relative::new(".").as_str(), ".");
/// # Ok::<(), moq_net::Error>(())
/// ```
#[derive(Debug, PartialEq, Eq, Hash, Clone, serde::Serialize)]
pub struct Relative<'a>(Cow<'a, str>);

impl<'a> Relative<'a> {
	/// Create a new `Relative` from a string slice.
	///
	/// Leading and trailing slashes are trimmed, consecutive internal slashes collapse to one,
	/// and redundant `.` segments are stripped. See the type-level doc for the full rules.
	pub fn new(s: &'a str) -> Self {
		let trimmed = s.trim_start_matches('/').trim_end_matches('/');

		if needs_normalize_relative(trimmed) {
			Self(Cow::Owned(normalize_relative_segments(trimmed)))
		} else {
			Self(Cow::Borrowed(trimmed))
		}
	}

	/// The normalized path as a string slice.
	pub fn as_str(&self) -> &str {
		&self.0
	}

	/// The empty relative path, which resolves to the base path itself.
	pub fn empty() -> Relative<'static> {
		Relative(Cow::Borrowed(""))
	}

	/// True if the path is empty (resolves to the base path itself).
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	/// The length of the normalized path in bytes.
	pub fn len(&self) -> usize {
		self.0.len()
	}

	/// Copy into an owned version with a `'static` lifetime.
	pub fn to_owned(&self) -> RelativeOwned {
		Relative(Cow::Owned(self.0.to_string()))
	}

	/// Convert into an owned version with a `'static` lifetime.
	pub fn into_owned(self) -> RelativeOwned {
		Relative(Cow::Owned(self.0.into_owned()))
	}

	/// Reborrow without copying.
	pub fn borrow(&'a self) -> Relative<'a> {
		Relative(Cow::Borrowed(&self.0))
	}
}

impl<'a> From<&'a str> for Relative<'a> {
	fn from(s: &'a str) -> Self {
		Self::new(s)
	}
}

impl<'a> From<&'a String> for Relative<'a> {
	fn from(s: &'a String) -> Self {
		Self::new(s)
	}
}

impl From<String> for Relative<'_> {
	fn from(s: String) -> Self {
		let trimmed = s.trim_start_matches('/').trim_end_matches('/');

		if needs_normalize_relative(trimmed) {
			Self(Cow::Owned(normalize_relative_segments(trimmed)))
		} else if trimmed == s {
			Self(Cow::Owned(s))
		} else {
			Self(Cow::Owned(trimmed.to_string()))
		}
	}
}

fn needs_normalize_relative(trimmed: &str) -> bool {
	trimmed.split('/').any(|seg| seg.is_empty() || seg == ".")
}

fn normalize_relative_segments(trimmed: &str) -> String {
	let segments = trimmed
		.split('/')
		.filter(|seg| !seg.is_empty() && *seg != ".")
		.collect::<Vec<_>>()
		.join("/");

	if segments.is_empty() && trimmed.split('/').any(|seg| seg == ".") {
		".".to_string()
	} else {
		segments
	}
}

impl Default for Relative<'_> {
	fn default() -> Self {
		Self(Cow::Borrowed(""))
	}
}

impl AsRef<str> for Relative<'_> {
	fn as_ref(&self) -> &str {
		&self.0
	}
}

impl Display for Relative<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

// Owned-only deserialization. We use `String::deserialize` so that owned deserializers
// (e.g. `serde_json::from_slice`) work. The borrowed form `<&str>::deserialize` requires
// `'de: 'a`, which is unsatisfiable when `'a = 'static`.
impl<'de> serde::Deserialize<'de> for Relative<'static> {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let s = String::deserialize(deserializer)?;
		Ok(Relative::from(s))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn p(s: &str) -> Path<'_> {
		Path::new(s).unwrap()
	}

	#[test]
	fn test_refuses_empty_segments() {
		for s in ["/", "//", "/foo", "foo/", "/foo/", "foo//bar", "foo/bar/", "a///b"] {
			assert!(matches!(Path::new(s), Err(Error::EmptySegment)), "accepted {s:?}");
			assert!(Path::try_from(s.to_string()).is_err(), "accepted owned {s:?}");
			assert!(s.parse::<PathOwned>().is_err(), "parsed {s:?}");
		}

		// The root has no segments, so none of them is empty.
		assert!(Path::new("").unwrap().is_empty());
		assert_eq!(Path::try_from(String::new()).unwrap(), Path::empty());

		// Borrowed and owned construction agree on everything else.
		for s in ["foo", "foo/bar", ".hidden/x", "a/../b"] {
			assert_eq!(p(s), Path::try_from(s.to_string()).unwrap());
			assert_eq!(p(s), s.parse::<PathOwned>().unwrap());
		}
	}

	#[test]
	fn test_decode_refuses_empty_segments() {
		use crate::lite::Version;

		for s in ["/foo", "foo/", "foo//bar"] {
			let mut buf = Vec::new();
			Encoder::new(&mut buf, Version::Lite04.into()).string(s).unwrap();
			assert!(matches!(
				crate::coding::decode_buf(&mut bytes::Bytes::from(buf), Version::Lite04, Path::decode),
				Err(DecodeError::InvalidValue)
			));
		}
	}

	#[test]
	fn test_has_prefix() {
		let path = p("foo/bar/baz");

		assert!(path.has_prefix(Path::empty()));
		assert!(path.has_prefix(p("foo")));
		assert!(path.has_prefix(p("foo/bar")));
		assert!(path.has_prefix(p("foo/bar/baz")));

		// Only whole segments match.
		assert!(!path.has_prefix(p("f")));
		assert!(!path.has_prefix(p("fo")));
		assert!(!path.has_prefix(p("foo/b")));
		assert!(!path.has_prefix(p("foo/ba")));
		assert!(!path.has_prefix(p("foo/bar/ba")));
		assert!(!p("foobar").has_prefix(p("foo")));
		assert!(p("foobar").has_prefix(p("foobar")));

		// The root prefixes everything, and is prefixed only by itself.
		assert!(Path::empty().has_prefix(Path::empty()));
		assert!(!Path::empty().has_prefix(p("foo")));
	}

	#[test]
	fn test_strip_prefix() {
		let path = p("foo/bar/baz");

		assert_eq!(path.strip_prefix(Path::empty()).unwrap().as_str(), "foo/bar/baz");
		assert_eq!(path.strip_prefix(p("foo")).unwrap().as_str(), "bar/baz");
		assert_eq!(path.strip_prefix(p("foo/bar")).unwrap().as_str(), "baz");
		assert_eq!(path.strip_prefix(p("foo/bar/baz")).unwrap().as_str(), "");

		assert!(path.strip_prefix(p("fo")).is_none());
		assert!(path.strip_prefix(p("bar")).is_none());
		assert!(path.strip_prefix(p("foo/ba")).is_none());
		assert!(p("foobar").strip_prefix(p("foo")).is_none());

		assert_eq!(Path::empty().strip_prefix(Path::empty()).unwrap().as_str(), "");
		assert!(Path::empty().strip_prefix(p("foo")).is_none());
	}

	#[test]
	fn test_join() {
		assert_eq!(p("foo").join(p("bar")).as_str(), "foo/bar");
		assert_eq!(p("foo/bar").join(p("baz/qux")).as_str(), "foo/bar/baz/qux");

		// The root is the identity on either side.
		assert_eq!(p("foo").join(Path::empty()).as_str(), "foo");
		assert_eq!(Path::empty().join(p("bar")).as_str(), "bar");
		assert_eq!(Path::empty().join(Path::empty()).as_str(), "");
	}

	// Pointer-equality checks that owned paths share one allocation through the
	// clone / to_owned / strip_prefix flow used by origin announce fan-out.
	#[test]
	fn test_owned_paths_share_allocation() {
		let path = p("customer/room/broadcast").to_owned();

		// Cloning an owned path shares the buffer.
		let cloned = path.clone();
		assert_eq!(path.as_str().as_ptr(), cloned.as_str().as_ptr());

		// as_path + to_owned (how notify queues a path per consumer) shares too.
		let requeued = path.as_path().to_owned();
		assert_eq!(path.as_str().as_ptr(), requeued.as_str().as_ptr());

		// Stripping a prefix from an owned path is offset arithmetic, not a copy.
		let stripped = path.strip_prefix(p("customer")).unwrap().to_owned();
		assert_eq!(stripped.as_str(), "room/broadcast");
		assert_eq!(stripped.as_str().as_ptr(), path.as_str()["customer/".len()..].as_ptr());

		// join produces an owned path whose clones share.
		let joined = path.join(p("alice"));
		let joined2 = joined.clone();
		assert_eq!(joined.as_str(), "customer/room/broadcast/alice");
		assert_eq!(joined.as_str().as_ptr(), joined2.as_str().as_ptr());
	}

	#[test]
	fn test_parts() {
		assert_eq!(Path::empty().parts().count(), 0);
		assert_eq!(p("foo").parts().collect::<Vec<_>>(), ["foo"]);
	}

	#[test]
	fn test_wire_max_parts() {
		use crate::lite::Version;

		let ok = (0..Path::MAX_PARTS)
			.map(|i| i.to_string())
			.collect::<Vec<_>>()
			.join("/");
		let too_deep = format!("{ok}/extra");

		// Encode enforces the limit.
		let mut buf = Vec::new();
		p(&ok)
			.encode(&mut Encoder::new(&mut buf, Version::Lite04.into()), Version::Lite04)
			.unwrap();
		assert!(matches!(
			p(&too_deep).encode_bytes(Version::Lite04),
			Err(EncodeError::BoundsExceeded)
		));

		// Decode round-trips at the limit.
		let decoded = crate::coding::decode_buf(&mut bytes::Bytes::from(buf), Version::Lite04, Path::decode).unwrap();
		assert_eq!(decoded.as_str(), ok);

		// Decode enforces the limit on a raw string that encode would have refused.
		let mut buf = Vec::new();
		Encoder::new(&mut buf, Version::Lite04.into())
			.string(too_deep.as_str())
			.unwrap();
		assert!(matches!(
			crate::coding::decode_buf(&mut bytes::Bytes::from(buf), Version::Lite04, Path::decode),
			Err(DecodeError::BoundsExceeded)
		));
	}

	#[test]
	fn test_owned_empty_paths() {
		// Empty paths never allocate and stay well-behaved.
		let empty = p("").to_owned();
		assert!(empty.is_empty());
		assert_eq!(empty, Path::empty());

		let path = p("foo").to_owned();
		let rest = path.strip_prefix(p("foo")).unwrap().to_owned();
		assert!(rest.is_empty());
	}

	#[test]
	fn test_path_relative_normalize() {
		assert_eq!(Relative::new("foo").as_str(), "foo");
		assert_eq!(Relative::new("/foo/").as_str(), "foo");
		assert_eq!(Relative::new("foo//bar").as_str(), "foo/bar");
		assert_eq!(Relative::new("../foo").as_str(), "../foo");
		assert_eq!(Relative::new("../../a/b").as_str(), "../../a/b");
		assert!(Relative::new("").is_empty());
	}

	#[test]
	fn test_path_relative_normalizes_dot_segments() {
		assert_eq!(Relative::new(".").as_str(), ".");
		assert_eq!(Relative::new("././").as_str(), ".");
		assert_eq!(Relative::new("./foo").as_str(), "foo");
		assert_eq!(Relative::new("foo/./bar").as_str(), "foo/bar");
		assert_eq!(Relative::new("./../foo").as_str(), "../foo");
		// From<String> takes the same normalization.
		assert_eq!(Relative::from("./foo".to_string()).as_str(), "foo");
		assert_eq!(Relative::from(".".to_string()).as_str(), ".");
	}

	#[test]
	fn test_resolve_replaces_base_name() {
		let base = p("a/b");
		assert_eq!(base.resolve(&Relative::new("c")).as_str(), "a/c");
		assert_eq!(base.resolve(&Relative::new("c/d")).as_str(), "a/c/d");
		assert_eq!(
			p("foo.hang/catalog.pro")
				.resolve(&Relative::new("./transcode.pro"))
				.as_str(),
			"foo.hang/transcode.pro"
		);
	}

	#[test]
	fn test_resolve_empty_rel_returns_base() {
		let base = p("a/b");
		assert_eq!(base.resolve(&Relative::new("")).as_str(), "a/b");
	}

	#[test]
	fn test_resolve_single_dotdot() {
		let base = p("a/b/c");
		assert_eq!(base.resolve(&Relative::new("../d")).as_str(), "a/d");
		assert_eq!(base.resolve(&Relative::new("..")).as_str(), "a");
	}

	#[test]
	fn test_resolve_multiple_dotdot() {
		let base = p("a/b/c");
		assert_eq!(base.resolve(&Relative::new("../../x")).as_str(), "x");
		assert_eq!(base.resolve(&Relative::new("../../../x")).as_str(), "x");
	}

	#[test]
	fn test_resolve_dotdot_clamps_at_root() {
		let base = p("a");
		// Excess `..` clamps at the root instead of escaping it.
		assert_eq!(base.resolve(&Relative::new("../../../foo")).as_str(), "foo");
		assert_eq!(base.resolve(&Relative::new("..")).as_str(), "");
	}

	#[test]
	fn test_resolve_empty_base() {
		let base = Path::empty();
		assert_eq!(base.resolve(&Relative::new("foo")).as_str(), "foo");
		assert_eq!(base.resolve(&Relative::new("..")).as_str(), "");
	}

	#[test]
	fn test_resolve_dot_names_parent() {
		let base = p("a/b");
		assert_eq!(base.resolve(&Relative::new(".")).as_str(), "a");
		assert_eq!(base.resolve(&Relative::new("./c")).as_str(), "a/c");
		assert_eq!(base.resolve(&Relative::new("./../c")).as_str(), "c");
	}

	#[test]
	fn test_resolve_self_reference_via_sibling_name() {
		// Naming the base within its parent yields the base unchanged, which lets the
		// caller compare resolved == base to detect a self-reference.
		let base = p("a/b");
		assert_eq!(base.resolve(&Relative::new("./b")).as_str(), "a/b");
	}

	#[test]
	fn test_try_resolve_distinguishes_root_from_escape() {
		let base = p("top");
		assert_eq!(base.try_resolve(&Relative::new(".")).unwrap().as_str(), "");
		assert!(base.try_resolve(&Relative::new("..")).is_none());

		let nested = p("a/b");
		assert_eq!(nested.try_resolve(&Relative::new("..")).unwrap().as_str(), "");
		assert!(nested.try_resolve(&Relative::new("../..")).is_none());
	}

	#[test]
	fn test_relative() {
		let rel = |target: &str, base: &str| p(target).relative(p(base)).unwrap();

		// Nested under the base: the base's own last segment is replaced, so it repeats.
		assert_eq!(rel("foo/bar/baz", "foo/bar").as_str(), "bar/baz");
		// Sibling.
		assert_eq!(rel("foo/baz", "foo/bar").as_str(), "baz");
		// Different subtree.
		assert_eq!(rel("foo/baz/bar", "foo/bar/baz").as_str(), "../baz/bar");
		// The base's parent, which only `.` can name.
		assert_eq!(rel("a/b", "a/b/transcode.hang").as_str(), ".");
		assert_eq!(rel("a/b", "a/b/one/two/transcode.hang").as_str(), "../..");
		// Roots.
		assert_eq!(rel("foo/bar", "").as_str(), "foo/bar");
		assert_eq!(rel("", "foo").as_str(), ".");
		// The base itself, which only the empty reference names.
		assert_eq!(rel("a/b", "a/b").as_str(), "");
		assert_eq!(rel("", "").as_str(), "");
	}

	#[test]
	fn test_relative_rejects_unnameable_targets() {
		// A segment literally named `.` or `..` is a legal path component, but resolution
		// would walk on it instead of naming it.
		assert!(p("a/../b").relative(p("")).is_none());
		assert!(p("x/./y").relative(p("x/z")).is_none());
		assert!(p("a/..").relative(p("a/b")).is_none());

		// A base is always nameable by itself, however its last segment is spelled.
		assert_eq!(p("a/..").relative(p("a/..")).unwrap().as_str(), "");

		// Dot segments inside the shared prefix are never emitted, so they are fine.
		let rel = p("a/../b/x").relative(p("a/../b/c")).unwrap();
		assert_eq!(rel.as_str(), "x");
		assert_eq!(p("a/../b/c").resolve(&rel).as_str(), "a/../b/x");
	}

	#[test]
	fn test_relative_round_trips() {
		let paths = [
			"", "a", "b", "a/b", "a/c", "a/b/c", "a/b/c/d", "x/y/z", "a/../b", "a/./b", "a/..", "a/.",
		];

		for base in paths {
			for target in paths {
				let base = p(base);
				let target = p(target);
				let Some(rel) = target.relative(&base) else {
					// Only an unnameable target may be refused, and never the base itself.
					assert!(
						target != base && target.parts().any(|part| part == "." || part == ".."),
						"{base} -> {target} refused a nameable target"
					);
					continue;
				};

				assert_eq!(base.resolve(&rel), target, "{base} -> {target} via {rel}");
				// The reference is derived from a real target, so it never escapes the root.
				assert!(
					base.try_resolve(&rel).is_some(),
					"{base} -> {target} via {rel} escaped the root"
				);
			}
		}
	}
}
