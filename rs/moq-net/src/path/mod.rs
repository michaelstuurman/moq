//! Broadcast paths and the patterns that match them.
//!
//! [`Path`] is a literal coordinate: `/`-separated segments, normalized, with
//! segment-aware prefix operations. [`Pattern`] describes a set of paths with
//! wildcards, and [`Patterns`] is a union of them reduced by containment. The
//! grammar and algebra live in [`moq-pattern`](moq_pattern); this module
//! re-exports them beside [`Path`] so grants, origin scopes, and announce
//! interests can share one dialect. Literal path construction and wire
//! decoding retain their existing behavior.

pub use moq_pattern::{InvalidPattern, Pattern, Patterns, Segment, Specificity};

use std::fmt::{self, Display};
use std::sync::Arc;

use crate::coding::{Decode, DecodeError, Decoder, Encode, EncodeError, Encoder};

// Paths up to this many bytes live inline, which keeps `Path` at 24 bytes.
const INLINE: usize = 22;

/// Inline bytes, a static literal, or a suffix of a shared reference-counted buffer.
///
/// Short paths copy inline, so cloning one is a memcpy with no atomics. A long path shares
/// one allocation: cloning bumps a refcount and suffix operations like `strip_prefix` only
/// advance `start`, so one allocation serves every copy of a path as it fans out.
#[derive(Clone)]
enum Repr {
	Inline { len: u8, buf: [u8; INLINE] },
	Static(&'static str),
	Shared { start: u32, buf: Arc<str> },
}

const _: () = assert!(std::mem::size_of::<Repr>() == 24);

impl Repr {
	fn copy(s: &str) -> Self {
		if s.len() <= INLINE {
			let mut buf = [0; INLINE];
			buf[..s.len()].copy_from_slice(s.as_bytes());
			Repr::Inline {
				len: s.len() as u8,
				buf,
			}
		} else {
			Repr::Shared {
				start: 0,
				buf: s.into(),
			}
		}
	}

	fn string(s: String) -> Self {
		if s.len() <= INLINE {
			Self::copy(&s)
		} else {
			Repr::Shared {
				start: 0,
				buf: s.into(),
			}
		}
	}
}

/// A broadcast path that provides safe prefix matching operations.
///
/// This type wraps a string but provides path-aware operations that respect
/// delimiter boundaries, preventing issues like "foo" matching "foobar".
///
/// Paths are automatically trimmed of leading and trailing slashes on creation,
/// making all slashes implicit at boundaries. A path names a point in the origin's
/// tree from its root; a leading slash never escapes that root. The same type names
/// an exact broadcast and the prefix a route or announcement covers, so a broadcast's
/// own path is the prefix it announces. See [`Relative`] for `..`-style references.
///
/// Cloning is cheap: a short path is stored inline, and a long one shares a single
/// reference-counted allocation that suffix operations like [`Path::strip_prefix`] reuse.
///
/// # Examples
/// ```
/// use moq_net::Path;
///
/// // Creation automatically trims slashes
/// let path1 = Path::new("/foo/bar/");
/// let path2 = Path::new("foo/bar");
/// assert_eq!(path1, path2);
///
/// let base = Path::new("api/v1");
/// assert!(base.has_prefix(&Path::new("api")));
///
/// let joined = base.join(&Path::new("users"));
/// assert_eq!(joined.as_str(), "api/v1/users");
/// ```
#[derive(Clone)]
pub struct Path(Repr);

impl Path {
	/// Maximum number of slash-separated parts in a path.
	///
	/// Matches the IETF moq-transport limit of 32 fields in a namespace tuple.
	/// moq-lite enforces the same bound: encoding or decoding a deeper path fails,
	/// and publishing one to an origin is rejected.
	pub const MAX_PARTS: usize = 32;

	/// Create a new Path by copying a string slice.
	///
	/// Leading and trailing slashes are automatically trimmed.
	/// Multiple consecutive internal slashes are collapsed to single slashes.
	pub fn new(s: &str) -> Self {
		let trimmed = s.trim_start_matches('/').trim_end_matches('/');
		match normalize(trimmed) {
			Some(normalized) => Self(Repr::string(normalized)),
			None => Self(Repr::copy(trimmed)),
		}
	}

	/// Create a new Path from a string literal without copying it, normalized like [`Path::new`].
	pub fn from_static(s: &'static str) -> Self {
		let trimmed = s.trim_start_matches('/').trim_end_matches('/');
		match normalize(trimmed) {
			Some(normalized) => Self(Repr::string(normalized)),
			None => Self(Repr::Static(trimmed)),
		}
	}

	#[cfg_attr(not(feature = "async"), allow(dead_code, reason = "async-only"))]
	pub(crate) fn from_escaped(s: String) -> Path {
		Path(Repr::string(s))
	}

	// A copy of this path skipping the first `n` bytes, reusing the shared buffer when possible.
	pub(crate) fn slice_from(&self, n: usize) -> Path {
		let rest = &self.as_str()[n..];
		match &self.0 {
			Repr::Static(s) => Path(Repr::Static(&s[n..])),
			Repr::Shared { buf, start } if rest.len() > INLINE => match u32::try_from(*start as usize + n) {
				Ok(start) => Path(Repr::Shared {
					buf: buf.clone(),
					start,
				}),
				// An offset past 4 GiB doesn't fit, so copy the rest instead.
				Err(_) => Path(Repr::copy(rest)),
			},
			_ => Path(Repr::copy(rest)),
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
	/// let path = Path::new("foo/bar");
	/// assert!(path.has_prefix(&Path::new("foo")));
	/// assert!(path.has_prefix(&Path::new("foo/")));
	/// assert!(!path.has_prefix(&Path::new("fo")));
	///
	/// let path = Path::new("foobar");
	/// assert!(!path.has_prefix(&Path::new("foo")));
	/// ```
	pub fn has_prefix(&self, prefix: &Path) -> bool {
		self.prefix_len(prefix).is_some()
	}

	// The bytes to skip past `prefix` and its delimiter, if it is a whole-segment prefix.
	fn prefix_len(&self, prefix: &Path) -> Option<usize> {
		let prefix = prefix.as_str();
		if prefix.is_empty() {
			return Some(0);
		}

		let s = self.as_str();
		if !s.starts_with(prefix) {
			return None;
		}

		// Check if the prefix is the exact match
		if s.len() == prefix.len() {
			return Some(s.len());
		}

		// Otherwise, ensure the character after the prefix is a delimiter
		(s.as_bytes().get(prefix.len()) == Some(&b'/')).then_some(prefix.len() + 1)
	}

	/// The remainder after removing `prefix`, or `None` if it isn't a prefix.
	///
	/// Only whole segments match: `a/bc` is not prefixed by `a/b`. An empty prefix
	/// returns the whole path.
	pub fn strip_prefix(&self, prefix: &Path) -> Option<Path> {
		let n = self.prefix_len(prefix)?;
		Some(self.slice_from(n))
	}

	/// Iterate over the slash-separated parts of the path.
	///
	/// The empty path has no parts.
	///
	/// # Examples
	/// ```
	/// use moq_net::Path;
	///
	/// let path = Path::new("foo/bar/baz");
	/// assert_eq!(path.parts().collect::<Vec<_>>(), ["foo", "bar", "baz"]);
	/// assert_eq!(Path::empty().parts().count(), 0);
	/// ```
	pub fn parts(&self) -> impl Iterator<Item = &str> {
		// Paths are normalized on creation so there are no empty parts to filter,
		// except that splitting the empty path yields one empty item.
		self.as_str().split('/').filter(|part| !part.is_empty())
	}

	/// Whether a segment starts with `.`, which hides the path from announce
	/// discovery below the requested prefix.
	pub(crate) fn is_hidden(&self) -> bool {
		self.parts().any(|part| part.starts_with('.'))
	}

	/// The normalized path as a string, with no leading or trailing slash.
	pub fn as_str(&self) -> &str {
		match &self.0 {
			// SAFETY: `Repr::copy` and `concat` fill `buf[..len]` only from whole `&str`s and an
			// ASCII `/`, so those bytes are valid UTF-8.
			Repr::Inline { len, buf } => unsafe { std::str::from_utf8_unchecked(&buf[..*len as usize]) },
			Repr::Static(s) => s,
			Repr::Shared { buf, start } => &buf[*start as usize..],
		}
	}

	/// The empty path, which prefixes every other path.
	pub const fn empty() -> Path {
		Path(Repr::Static(""))
	}

	/// Returns `true` if this is the empty path.
	pub fn is_empty(&self) -> bool {
		self.as_str().is_empty()
	}

	/// The length in bytes, not segments.
	pub fn len(&self) -> usize {
		self.as_str().len()
	}

	/// Join this path with another path component.
	///
	/// # Examples
	/// ```
	/// use moq_net::Path;
	///
	/// let base = Path::new("foo");
	/// let joined = base.join(&Path::new("bar"));
	/// assert_eq!(joined.as_str(), "foo/bar");
	/// ```
	pub fn join(&self, other: &Path) -> Path {
		if self.is_empty() {
			other.clone()
		} else if other.is_empty() {
			self.clone()
		} else {
			Self::concat(self.as_str(), other.as_str())
		}
	}

	// `a/b` for two non-empty normalized paths, built in place.
	pub(crate) fn concat(a: &str, b: &str) -> Path {
		let len = a.len() + 1 + b.len();
		if len <= INLINE {
			let mut buf = [0; INLINE];
			buf[..a.len()].copy_from_slice(a.as_bytes());
			buf[a.len()] = b'/';
			buf[a.len() + 1..len].copy_from_slice(b.as_bytes());
			Path(Repr::Inline { len: len as u8, buf })
		} else {
			// Sized up front: `format!` guesses low and reallocates.
			let mut buf = String::with_capacity(len);
			buf.push_str(a);
			buf.push('/');
			buf.push_str(b);
			Path(Repr::Shared {
				buf: buf.into(),
				start: 0,
			})
		}
	}

	/// Resolve a [`Relative`] against this path.
	///
	/// A non-empty reference replaces the last segment of the base, matching relative URL
	/// resolution. `..` segments then pop another segment; other segments are appended.
	/// Excess `..` is a no-op once the base is empty (subsequent named segments still append).
	/// An empty `rel` returns this path unchanged.
	///
	/// [`Relative::new`] strips empty and redundant `.` segments, but preserves a lone `.`
	/// so it can reference the base's parent.
	///
	/// # Examples
	/// ```
	/// use moq_net::{Path, path::Relative};
	///
	/// let base = Path::new("a/b/c");
	/// assert_eq!(base.resolve(&Relative::new("./d")).as_str(), "a/b/d");
	/// assert_eq!(base.resolve(&Relative::new(".")).as_str(), "a/b");
	/// assert_eq!(base.resolve(&Relative::new("../d")).as_str(), "a/d");
	/// ```
	pub fn resolve(&self, rel: &Relative) -> Path {
		if rel.is_empty() {
			return self.clone();
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

		Path(Repr::string(segments.join("/")))
	}

	/// Resolve a [`Relative`], returning `None` if it escapes above the root.
	///
	/// Unlike [`Path::resolve`], this distinguishes a valid reference to the empty root
	/// path from excess `..` segments. Use it when an untrusted relative reference must
	/// not be clamped to the root.
	pub fn try_resolve(&self, rel: &Relative) -> Option<Path> {
		if rel.is_empty() {
			return Some(self.clone());
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

		Some(Path(Repr::string(segments.join("/"))))
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
	/// let base = Path::new("a/b");
	/// assert_eq!(Path::new("a/b/c").relative(&base).unwrap().as_str(), "b/c");
	/// assert_eq!(Path::new("a/c").relative(&base).unwrap().as_str(), "c");
	/// assert_eq!(Path::new("c").relative(&base).unwrap().as_str(), "../c");
	///
	/// // The lone `.` names the base's parent, which the empty reference cannot.
	/// assert_eq!(Path::new("a").relative(&base).unwrap().as_str(), ".");
	///
	/// // The base itself.
	/// assert_eq!(Path::new("a/b").relative(&base).unwrap().as_str(), "");
	///
	/// // A segment named `..` is a legal path component but an unnameable target.
	/// assert!(Path::new("a/..").relative(&base).is_none());
	/// ```
	pub fn relative(&self, base: &Path) -> Option<Relative> {
		// Only the empty reference can name a base whose last segment is itself `.` or `..`,
		// since resolution replaces that segment rather than emitting it.
		if self == base {
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

		Some(Relative::from(rel.join("/")))
	}
}

// The normalized copy when `trimmed` holds consecutive slashes, else `None`.
fn normalize(trimmed: &str) -> Option<String> {
	trimmed.contains("//").then(|| {
		trimmed
			.split('/')
			.filter(|s| !s.is_empty())
			.collect::<Vec<_>>()
			.join("/")
	})
}

// Comparisons, ordering, and hashing all go through `as_str()` so paths with the same
// content behave identically whatever their representation (e.g. as map keys).
impl PartialEq for Path {
	fn eq(&self, other: &Path) -> bool {
		self.as_str() == other.as_str()
	}
}

impl Eq for Path {}

impl PartialOrd for Path {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Path {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.as_str().cmp(other.as_str())
	}
}

impl std::hash::Hash for Path {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.as_str().hash(state)
	}
}

/// Look up a `Path`-keyed map by `&str` without building a `Path`. The key must be normalized.
impl std::borrow::Borrow<str> for Path {
	fn borrow(&self) -> &str {
		self.as_str()
	}
}

impl fmt::Debug for Path {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_tuple("Path").field(&self.as_str()).finish()
	}
}

impl serde::Serialize for Path {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_str(self.as_str())
	}
}

impl From<&str> for Path {
	fn from(s: &str) -> Self {
		Self::new(s)
	}
}

impl From<&String> for Path {
	fn from(s: &String) -> Self {
		Self::new(s)
	}
}

impl From<&Path> for Path {
	fn from(p: &Path) -> Self {
		p.clone()
	}
}

impl Default for Path {
	fn default() -> Self {
		Path::empty()
	}
}

impl From<String> for Path {
	fn from(s: String) -> Self {
		// `Arc<str>` can't adopt a `String`'s buffer, so this copies like `new`.
		Self::new(&s)
	}
}

impl AsRef<str> for Path {
	fn as_ref(&self) -> &str {
		self.as_str()
	}
}

impl Display for Path {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.as_str())
	}
}

impl<V> Decode<V> for Path {
	fn decode(r: &mut Decoder<'_>, _: V) -> Result<Self, DecodeError> {
		let path: Path = r.string()?.into();
		if path.parts().count() > Path::MAX_PARTS {
			return Err(DecodeError::BoundsExceeded);
		}
		Ok(path)
	}
}

impl<V> Encode<V> for Path {
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
/// assert_eq!(Path::new("a/b").resolve(&rel).as_str(), "a/source");
///
/// // Redundant `.` segments are stripped on creation.
/// assert_eq!(Relative::new("./a/./b").as_str(), "a/b");
/// assert_eq!(Relative::new(".").as_str(), ".");
/// ```
#[derive(Debug, PartialEq, Eq, Hash, Clone, Default)]
pub struct Relative(Path);

impl serde::Serialize for Relative {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		self.0.serialize(serializer)
	}
}

impl Relative {
	/// Create a new `Relative` from a string slice.
	///
	/// Leading and trailing slashes are trimmed, consecutive internal slashes collapse to one,
	/// and redundant `.` segments are stripped. See the type-level doc for the full rules.
	pub fn new(s: &str) -> Self {
		let trimmed = s.trim_start_matches('/').trim_end_matches('/');

		if needs_normalize_relative(trimmed) {
			Self(Path(Repr::string(normalize_relative_segments(trimmed))))
		} else {
			Self(Path(Repr::copy(trimmed)))
		}
	}

	/// The normalized path as a string slice.
	pub fn as_str(&self) -> &str {
		self.0.as_str()
	}

	/// The empty relative path, which resolves to the base path itself.
	pub const fn empty() -> Relative {
		Relative(Path::empty())
	}

	/// True if the path is empty (resolves to the base path itself).
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	/// The length of the normalized path in bytes.
	pub fn len(&self) -> usize {
		self.0.len()
	}
}

impl From<&str> for Relative {
	fn from(s: &str) -> Self {
		Self::new(s)
	}
}

impl From<&String> for Relative {
	fn from(s: &String) -> Self {
		Self::new(s)
	}
}

impl From<String> for Relative {
	fn from(s: String) -> Self {
		Self::new(&s)
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

impl AsRef<str> for Relative {
	fn as_ref(&self) -> &str {
		self.as_str()
	}
}

impl Display for Relative {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.as_str())
	}
}

impl<'de> serde::Deserialize<'de> for Relative {
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

	#[test]
	fn test_has_prefix() {
		let path = Path::new("foo/bar/baz");

		// Valid prefixes - test with both &str and &Path
		assert!(path.has_prefix(&Path::new("")));
		assert!(path.has_prefix(&Path::new("foo")));
		assert!(path.has_prefix(&Path::new("foo")));
		assert!(path.has_prefix(&Path::new("foo/")));
		assert!(path.has_prefix(&Path::new("foo/bar")));
		assert!(path.has_prefix(&Path::new("foo/bar/")));
		assert!(path.has_prefix(&Path::new("foo/bar/baz")));

		// Invalid prefixes - should not match partial components
		assert!(!path.has_prefix(&Path::new("f")));
		assert!(!path.has_prefix(&Path::new("fo")));
		assert!(!path.has_prefix(&Path::new("foo/b")));
		assert!(!path.has_prefix(&Path::new("foo/ba")));
		assert!(!path.has_prefix(&Path::new("foo/bar/ba")));

		// Edge case: "foobar" should not match "foo"
		let path = Path::new("foobar");
		assert!(!path.has_prefix(&Path::new("foo")));
		assert!(path.has_prefix(&Path::new("foobar")));
	}

	#[test]
	fn test_strip_prefix() {
		let path = Path::new("foo/bar/baz");

		// Test with both &str and &Path
		assert_eq!(path.strip_prefix(&Path::new("")).unwrap().as_str(), "foo/bar/baz");
		assert_eq!(path.strip_prefix(&Path::new("foo")).unwrap().as_str(), "bar/baz");
		assert_eq!(path.strip_prefix(&Path::new("foo/")).unwrap().as_str(), "bar/baz");
		assert_eq!(path.strip_prefix(&Path::new("foo/bar")).unwrap().as_str(), "baz");
		assert_eq!(path.strip_prefix(&Path::new("foo/bar/")).unwrap().as_str(), "baz");
		assert_eq!(path.strip_prefix(&Path::new("foo/bar/baz")).unwrap().as_str(), "");

		// Should fail for invalid prefixes
		assert!(path.strip_prefix(&Path::new("fo")).is_none());
		assert!(path.strip_prefix(&Path::new("bar")).is_none());
	}

	#[test]
	fn test_join() {
		// Test with both &str and &Path
		assert_eq!(Path::new("foo").join(&Path::new("bar")).as_str(), "foo/bar");
		assert_eq!(Path::new("foo/").join(&Path::new("bar")).as_str(), "foo/bar");
		assert_eq!(Path::new("").join(&Path::new("bar")).as_str(), "bar");
		assert_eq!(Path::new("foo/bar").join(&Path::new("baz")).as_str(), "foo/bar/baz");
	}

	#[test]
	fn test_from_conversions() {
		let path1 = Path::from("foo/bar");
		let path2 = Path::from("foo/bar");
		let s = String::from("foo/bar");
		let path3 = Path::from(&s);

		assert_eq!(path1.as_str(), "foo/bar");
		assert_eq!(path2.as_str(), "foo/bar");
		assert_eq!(path3.as_str(), "foo/bar");
	}

	#[test]
	fn test_path_prefix_join() {
		let prefix = Path::new("foo");
		let suffix = Path::new("bar/baz");
		let path = prefix.join(&suffix);
		assert_eq!(path.as_str(), "foo/bar/baz");

		let prefix = Path::new("foo/");
		let suffix = Path::new("bar/baz");
		let path = prefix.join(&suffix);
		assert_eq!(path.as_str(), "foo/bar/baz");

		let prefix = Path::new("foo");
		let suffix = Path::new("/bar/baz");
		let path = prefix.join(&suffix);
		assert_eq!(path.as_str(), "foo/bar/baz");

		let prefix = Path::new("");
		let suffix = Path::new("bar/baz");
		let path = prefix.join(&suffix);
		assert_eq!(path.as_str(), "bar/baz");
	}

	#[test]
	fn test_path_prefix_conversions() {
		let prefix1 = Path::from("foo/bar");
		let prefix2 = Path::from(String::from("foo/bar"));
		let s = String::from("foo/bar");
		let prefix3 = Path::from(&s);

		assert_eq!(prefix1.as_str(), "foo/bar");
		assert_eq!(prefix2.as_str(), "foo/bar");
		assert_eq!(prefix3.as_str(), "foo/bar");
	}

	#[test]
	fn test_path_suffix_conversions() {
		let suffix1 = Path::from("foo/bar");
		let suffix2 = Path::from(String::from("foo/bar"));
		let s = String::from("foo/bar");
		let suffix3 = Path::from(&s);

		assert_eq!(suffix1.as_str(), "foo/bar");
		assert_eq!(suffix2.as_str(), "foo/bar");
		assert_eq!(suffix3.as_str(), "foo/bar");
	}

	#[test]
	fn test_path_types_basic_operations() {
		let prefix = Path::new("foo/bar");
		assert_eq!(prefix.as_str(), "foo/bar");
		assert!(!prefix.is_empty());
		assert_eq!(prefix.len(), 7);

		let suffix = Path::new("baz/qux");
		assert_eq!(suffix.as_str(), "baz/qux");
		assert!(!suffix.is_empty());
		assert_eq!(suffix.len(), 7);

		let empty_prefix = Path::new("");
		assert!(empty_prefix.is_empty());
		assert_eq!(empty_prefix.len(), 0);

		let empty_suffix = Path::new("");
		assert!(empty_suffix.is_empty());
		assert_eq!(empty_suffix.len(), 0);
	}

	#[test]
	fn test_prefix_has_prefix() {
		// Test empty prefix (should match everything)
		let prefix = Path::new("foo/bar");
		assert!(prefix.has_prefix(&Path::new("")));

		// Test exact matches
		let prefix = Path::new("foo/bar");
		assert!(prefix.has_prefix(&Path::new("foo/bar")));

		// Test valid prefixes
		assert!(prefix.has_prefix(&Path::new("foo")));
		assert!(prefix.has_prefix(&Path::new("foo/")));

		// Test invalid prefixes - partial matches should fail
		assert!(!prefix.has_prefix(&Path::new("f")));
		assert!(!prefix.has_prefix(&Path::new("fo")));
		assert!(!prefix.has_prefix(&Path::new("foo/b")));
		assert!(!prefix.has_prefix(&Path::new("foo/ba")));

		// Test edge cases
		let prefix = Path::new("foobar");
		assert!(!prefix.has_prefix(&Path::new("foo")));
		assert!(prefix.has_prefix(&Path::new("foobar")));

		// Test trailing slash handling
		let prefix = Path::new("foo/bar/");
		assert!(prefix.has_prefix(&Path::new("foo")));
		assert!(prefix.has_prefix(&Path::new("foo/")));
		assert!(prefix.has_prefix(&Path::new("foo/bar")));
		assert!(prefix.has_prefix(&Path::new("foo/bar/")));

		// Test single component
		let prefix = Path::new("foo");
		assert!(prefix.has_prefix(&Path::new("")));
		assert!(prefix.has_prefix(&Path::new("foo")));
		assert!(prefix.has_prefix(&Path::new("foo/"))); // "foo/" becomes "foo" after trimming
		assert!(!prefix.has_prefix(&Path::new("f")));

		// Test empty prefix
		let prefix = Path::new("");
		assert!(prefix.has_prefix(&Path::new("")));
		assert!(!prefix.has_prefix(&Path::new("foo")));
	}

	#[test]
	fn test_prefix_join() {
		// Basic joining
		let prefix = Path::new("foo");
		let suffix = Path::new("bar");
		assert_eq!(prefix.join(&suffix).as_str(), "foo/bar");

		// Trailing slash on prefix
		let prefix = Path::new("foo/");
		let suffix = Path::new("bar");
		assert_eq!(prefix.join(&suffix).as_str(), "foo/bar");

		// Leading slash on suffix
		let prefix = Path::new("foo");
		let suffix = Path::new("/bar");
		assert_eq!(prefix.join(&suffix).as_str(), "foo/bar");

		// Trailing slash on suffix
		let prefix = Path::new("foo");
		let suffix = Path::new("bar/");
		assert_eq!(prefix.join(&suffix).as_str(), "foo/bar"); // trailing slash is trimmed

		// Both have slashes
		let prefix = Path::new("foo/");
		let suffix = Path::new("/bar");
		assert_eq!(prefix.join(&suffix).as_str(), "foo/bar");

		// Empty suffix
		let prefix = Path::new("foo");
		let suffix = Path::new("");
		assert_eq!(prefix.join(&suffix).as_str(), "foo");

		// Empty prefix
		let prefix = Path::new("");
		let suffix = Path::new("bar");
		assert_eq!(prefix.join(&suffix).as_str(), "bar");

		// Both empty
		let prefix = Path::new("");
		let suffix = Path::new("");
		assert_eq!(prefix.join(&suffix).as_str(), "");

		// Complex paths
		let prefix = Path::new("foo/bar");
		let suffix = Path::new("baz/qux");
		assert_eq!(prefix.join(&suffix).as_str(), "foo/bar/baz/qux");

		// Complex paths with slashes
		let prefix = Path::new("foo/bar/");
		let suffix = Path::new("/baz/qux/");
		assert_eq!(prefix.join(&suffix).as_str(), "foo/bar/baz/qux"); // all slashes are trimmed
	}

	#[test]
	fn test_path_ref() {
		// Test PathRef creation and normalization
		let ref1 = Path::new("/foo/bar/");
		assert_eq!(ref1.as_str(), "foo/bar");

		let ref2 = Path::from("///foo///");
		assert_eq!(ref2.as_str(), "foo");

		// Test PathRef normalizes multiple slashes
		let ref3 = Path::new("foo//bar///baz");
		assert_eq!(ref3.as_str(), "foo/bar/baz");

		// Test conversions
		let path = Path::new("foo/bar");
		let path_ref = path;
		assert_eq!(path_ref.as_str(), "foo/bar");

		// Test that Path methods work with PathRef
		let path2 = Path::new("foo/bar/baz");
		assert!(path2.has_prefix(&path_ref));
		assert_eq!(path2.strip_prefix(&path_ref).unwrap().as_str(), "baz");

		// Test empty PathRef
		let empty = Path::new("");
		assert!(empty.is_empty());
		assert_eq!(empty.len(), 0);
	}

	#[test]
	fn test_multiple_consecutive_slashes() {
		let path = Path::new("foo//bar///baz");
		// Multiple consecutive slashes are collapsed to single slashes
		assert_eq!(path.as_str(), "foo/bar/baz");

		// Test with leading and trailing slashes too
		let path2 = Path::new("//foo//bar///baz//");
		assert_eq!(path2.as_str(), "foo/bar/baz");

		// Test empty segments are handled correctly
		let path3 = Path::new("foo///bar");
		assert_eq!(path3.as_str(), "foo/bar");
	}

	#[test]
	fn test_removes_multiple_slashes_comprehensively() {
		// Test various multiple slash scenarios
		assert_eq!(Path::new("foo//bar").as_str(), "foo/bar");
		assert_eq!(Path::new("foo///bar").as_str(), "foo/bar");
		assert_eq!(Path::new("foo////bar").as_str(), "foo/bar");

		// Multiple occurrences of double slashes
		assert_eq!(Path::new("foo//bar//baz").as_str(), "foo/bar/baz");
		assert_eq!(Path::new("a//b//c//d").as_str(), "a/b/c/d");

		// Mixed slash counts
		assert_eq!(Path::new("foo//bar///baz////qux").as_str(), "foo/bar/baz/qux");

		// With leading and trailing slashes
		assert_eq!(Path::new("//foo//bar//").as_str(), "foo/bar");
		assert_eq!(Path::new("///foo///bar///").as_str(), "foo/bar");

		// Edge case: only slashes
		assert_eq!(Path::new("//").as_str(), "");
		assert_eq!(Path::new("////").as_str(), "");

		// Test that operations work correctly with normalized paths
		let path_with_slashes = Path::new("foo//bar///baz");
		assert!(path_with_slashes.has_prefix(&Path::new("foo/bar")));
		assert_eq!(
			path_with_slashes.strip_prefix(&Path::new("foo")).unwrap().as_str(),
			"bar/baz"
		);
		assert_eq!(path_with_slashes.join(&Path::new("qux")).as_str(), "foo/bar/baz/qux");
	}

	#[test]
	fn test_prefix_strip_prefix() {
		// Test basic stripping
		let prefix = Path::new("foo/bar/baz");
		assert_eq!(prefix.strip_prefix(&Path::new("")).unwrap().as_str(), "foo/bar/baz");
		assert_eq!(prefix.strip_prefix(&Path::new("foo")).unwrap().as_str(), "bar/baz");
		assert_eq!(prefix.strip_prefix(&Path::new("foo/")).unwrap().as_str(), "bar/baz");
		assert_eq!(prefix.strip_prefix(&Path::new("foo/bar")).unwrap().as_str(), "baz");
		assert_eq!(prefix.strip_prefix(&Path::new("foo/bar/")).unwrap().as_str(), "baz");
		assert_eq!(prefix.strip_prefix(&Path::new("foo/bar/baz")).unwrap().as_str(), "");

		// Test invalid prefixes
		assert!(prefix.strip_prefix(&Path::new("fo")).is_none());
		assert!(prefix.strip_prefix(&Path::new("bar")).is_none());
		assert!(prefix.strip_prefix(&Path::new("foo/ba")).is_none());

		// Test edge cases
		let prefix = Path::new("foobar");
		assert!(prefix.strip_prefix(&Path::new("foo")).is_none());
		assert_eq!(prefix.strip_prefix(&Path::new("foobar")).unwrap().as_str(), "");

		// Test empty prefix
		let prefix = Path::new("");
		assert_eq!(prefix.strip_prefix(&Path::new("")).unwrap().as_str(), "");
		assert!(prefix.strip_prefix(&Path::new("foo")).is_none());

		// Test single component
		let prefix = Path::new("foo");
		assert_eq!(prefix.strip_prefix(&Path::new("foo")).unwrap().as_str(), "");
		assert_eq!(prefix.strip_prefix(&Path::new("foo/")).unwrap().as_str(), ""); // "foo/" becomes "foo" after trimming

		// Test trailing slash handling
		let prefix = Path::new("foo/bar/");
		assert_eq!(prefix.strip_prefix(&Path::new("foo")).unwrap().as_str(), "bar");
		assert_eq!(prefix.strip_prefix(&Path::new("foo/")).unwrap().as_str(), "bar");
		assert_eq!(prefix.strip_prefix(&Path::new("foo/bar")).unwrap().as_str(), "");
		assert_eq!(prefix.strip_prefix(&Path::new("foo/bar/")).unwrap().as_str(), "");
	}

	// A long path shares one allocation through the clone and strip_prefix flow used by
	// announce fan-out. A short one is stored inline instead, so its copies share nothing.
	#[test]
	fn test_long_paths_share_allocation() {
		let path = Path::new("customer/room-0123456789/broadcast");

		// Cloning shares the buffer.
		let cloned = path.clone();
		assert_eq!(path.as_str().as_ptr(), cloned.as_str().as_ptr());

		// Stripping a prefix leaving a long rest is offset arithmetic, not a copy.
		let stripped = path.strip_prefix(&Path::new("customer")).unwrap();
		assert_eq!(stripped.as_str(), "room-0123456789/broadcast");
		assert_eq!(stripped.as_str().as_ptr(), path.as_str()["customer/".len()..].as_ptr());

		// A short rest moves inline rather than keeping the long buffer alive.
		let short = path.strip_prefix(&Path::new("customer/room-0123456789")).unwrap();
		assert_eq!(short.as_str(), "broadcast");
		assert!(matches!(short.0, Repr::Inline { .. }));

		// join produces a path whose clones share.
		let joined = path.join(&Path::new("alice"));
		let joined2 = joined.clone();
		assert_eq!(joined.as_str(), "customer/room-0123456789/broadcast/alice");
		assert_eq!(joined.as_str().as_ptr(), joined2.as_str().as_ptr());
	}

	#[test]
	fn test_inline_boundary() {
		let fits = "a".repeat(INLINE);
		let spills = "a".repeat(INLINE + 1);
		assert!(matches!(Path::new(&fits).0, Repr::Inline { .. }));
		assert!(matches!(Path::new(&spills).0, Repr::Shared { .. }));

		// A join exactly at the limit stays inline; one byte more spills.
		let half = Path::new(&"b".repeat(INLINE / 2 - 1));
		let joined = half.join(&Path::new(&"c".repeat(INLINE / 2)));
		assert_eq!(joined.len(), INLINE);
		assert!(matches!(joined.0, Repr::Inline { .. }));
		assert_eq!(
			joined.as_str(),
			format!("{}/{}", "b".repeat(INLINE / 2 - 1), "c".repeat(INLINE / 2))
		);
		assert!(matches!(joined.join(&Path::new("d")).0, Repr::Shared { .. }));

		// Multi-byte characters survive the inline copy.
		let wide = Path::new("caf\u{e9}/\u{1f3a5}");
		assert!(matches!(wide.0, Repr::Inline { .. }));
		assert_eq!(wide.as_str(), "caf\u{e9}/\u{1f3a5}");
		assert_eq!(
			wide.strip_prefix(&Path::new("caf\u{e9}")).unwrap().as_str(),
			"\u{1f3a5}"
		);
	}

	#[test]
	fn test_from_static_borrows() {
		const LITERAL: &str = "/static/path/that/is/longer/than/inline/";
		let path = Path::from_static(LITERAL);
		assert_eq!(path.as_str(), "static/path/that/is/longer/than/inline");
		assert_eq!(path.as_str().as_ptr(), LITERAL[1..].as_ptr());

		// A suffix of a static path borrows the literal too.
		let rest = path.strip_prefix(&Path::new("static")).unwrap();
		assert_eq!(rest.as_str().as_ptr(), LITERAL["/static/".len()..].as_ptr());

		// Normalizing has to copy.
		assert_eq!(Path::from_static("a//b").as_str(), "a/b");
	}

	#[test]
	fn test_parts() {
		assert_eq!(Path::empty().parts().count(), 0);
		assert_eq!(Path::new("foo").parts().collect::<Vec<_>>(), ["foo"]);
		assert_eq!(Path::new("/foo//bar/").parts().collect::<Vec<_>>(), ["foo", "bar"]);
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
		Path::new(&ok)
			.encode(&mut Encoder::new(&mut buf, Version::Lite04.into()), Version::Lite04)
			.unwrap();
		assert!(matches!(
			Path::new(&too_deep).encode_bytes(Version::Lite04),
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
		let empty = Path::new("");
		assert!(empty.is_empty());
		assert_eq!(empty, Path::empty());

		let path = Path::new("foo");
		let rest = path.strip_prefix(&Path::new("foo")).unwrap();
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
		let base = Path::new("a/b");
		assert_eq!(base.resolve(&Relative::new("c")).as_str(), "a/c");
		assert_eq!(base.resolve(&Relative::new("c/d")).as_str(), "a/c/d");
		assert_eq!(
			Path::new("foo.hang/catalog.pro")
				.resolve(&Relative::new("./transcode.pro"))
				.as_str(),
			"foo.hang/transcode.pro"
		);
	}

	#[test]
	fn test_resolve_empty_rel_returns_base() {
		let base = Path::new("a/b");
		assert_eq!(base.resolve(&Relative::new("")).as_str(), "a/b");
	}

	#[test]
	fn test_resolve_single_dotdot() {
		let base = Path::new("a/b/c");
		assert_eq!(base.resolve(&Relative::new("../d")).as_str(), "a/d");
		assert_eq!(base.resolve(&Relative::new("..")).as_str(), "a");
	}

	#[test]
	fn test_resolve_multiple_dotdot() {
		let base = Path::new("a/b/c");
		assert_eq!(base.resolve(&Relative::new("../../x")).as_str(), "x");
		assert_eq!(base.resolve(&Relative::new("../../../x")).as_str(), "x");
	}

	#[test]
	fn test_resolve_dotdot_clamps_at_root() {
		let base = Path::new("a");
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
		let base = Path::new("a/b");
		assert_eq!(base.resolve(&Relative::new(".")).as_str(), "a");
		assert_eq!(base.resolve(&Relative::new("./c")).as_str(), "a/c");
		assert_eq!(base.resolve(&Relative::new("./../c")).as_str(), "c");
	}

	#[test]
	fn test_resolve_self_reference_via_sibling_name() {
		// Naming the base within its parent yields the base unchanged, which lets the
		// caller compare resolved == base to detect a self-reference.
		let base = Path::new("a/b");
		assert_eq!(base.resolve(&Relative::new("./b")).as_str(), "a/b");
	}

	#[test]
	fn test_try_resolve_distinguishes_root_from_escape() {
		let base = Path::new("top");
		assert_eq!(base.try_resolve(&Relative::new(".")).unwrap().as_str(), "");
		assert!(base.try_resolve(&Relative::new("..")).is_none());

		let nested = Path::new("a/b");
		assert_eq!(nested.try_resolve(&Relative::new("..")).unwrap().as_str(), "");
		assert!(nested.try_resolve(&Relative::new("../..")).is_none());
	}

	#[test]
	fn test_relative() {
		let rel = |target: &str, base: &str| Path::new(target).relative(&Path::new(base)).unwrap();

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
		// Slashes are normalized first.
		assert_eq!(rel("/a//b/", "//a/b/dir//").as_str(), ".");
	}

	#[test]
	fn test_relative_rejects_unnameable_targets() {
		// A segment literally named `.` or `..` is a legal path component, but resolution
		// would walk on it instead of naming it.
		assert!(Path::new("a/../b").relative(&Path::new("")).is_none());
		assert!(Path::new("x/./y").relative(&Path::new("x/z")).is_none());
		assert!(Path::new("a/..").relative(&Path::new("a/b")).is_none());

		// A base is always nameable by itself, however its last segment is spelled.
		assert_eq!(Path::new("a/..").relative(&Path::new("a/..")).unwrap().as_str(), "");

		// Dot segments inside the shared prefix are never emitted, so they are fine.
		let rel = Path::new("a/../b/x").relative(&Path::new("a/../b/c")).unwrap();
		assert_eq!(rel.as_str(), "x");
		assert_eq!(Path::new("a/../b/c").resolve(&rel).as_str(), "a/../b/x");
	}

	#[test]
	fn test_relative_round_trips() {
		let paths = [
			"", "a", "b", "a/b", "a/c", "a/b/c", "a/b/c/d", "x/y/z", "a/../b", "a/./b", "a/..", "a/.",
		];

		for base in paths {
			for target in paths {
				let base = Path::new(base);
				let target = Path::new(target);
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
