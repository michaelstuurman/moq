//! ISO Common Encryption (CENC) in fragmented MP4: reading what a protected track declares.
//!
//! An encrypted track wraps its sample entry: `encv`/`enca` holds the original entry's fields
//! plus a `sinf` box naming the original format (`frma`), the scheme (`schm`) and the default
//! key (`tenc`). DRM systems put their initialization data in `pssh` boxes beside the tracks.
//! Nothing here decrypts; it lets the importer read the real codec and lets the catalog say how
//! the track is protected (draft-ietf-moq-cmsf section 4).

use bytes::Bytes;
use mp4_atom::{Decode, FourCC};

const ENCV: [u8; 4] = *b"encv";
const ENCA: [u8; 4] = *b"enca";

/// Bytes before a visual sample entry's child boxes: SampleEntry (8) and VisualSampleEntry (70).
const VISUAL_FIELDS: usize = 78;
/// Bytes before an audio sample entry's child boxes: SampleEntry (8) and AudioSampleEntry v0 (20).
const AUDIO_FIELDS: usize = 28;

/// How a protected track is encrypted, from its sample entry and the init segment's `pssh` boxes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Protection {
	/// The Common Encryption scheme, e.g. `cenc` or `cbcs`.
	pub scheme: String,
	/// The default key ID as a UUID string.
	pub default_kid: String,
	/// Each DRM system that has a `pssh` box: its system ID as a UUID string and the box, base64.
	pub systems: Vec<(String, String)>,
}

/// Iterate the boxes in `data` as (type, whole box, payload). Stops at the first malformed one.
fn boxes(data: &[u8]) -> impl Iterator<Item = ([u8; 4], &[u8], &[u8])> {
	let mut rest = data;
	std::iter::from_fn(move || {
		if rest.len() < 8 {
			return None;
		}
		let size32 = u32::from_be_bytes(rest[..4].try_into().ok()?) as usize;
		let kind: [u8; 4] = rest[4..8].try_into().ok()?;
		let (size, header) = match size32 {
			1 if rest.len() >= 16 => (
				usize::try_from(u64::from_be_bytes(rest[8..16].try_into().ok()?)).ok()?,
				16,
			),
			0 => (rest.len(), 8),
			size => (size, 8),
		};
		if size < header || size > rest.len() {
			return None;
		}
		let (whole, tail) = rest.split_at(size);
		rest = tail;
		Some((kind, whole, &whole[header..]))
	})
}

/// The payload of the first child of type `kind`.
fn child<'a>(data: &'a [u8], kind: &[u8; 4]) -> Option<&'a [u8]> {
	boxes(data).find(|(k, _, _)| k == kind).map(|(_, _, payload)| payload)
}

/// The `pssh` boxes directly inside a `moov` body, whole, so they can travel in a rebuilt init.
pub(super) fn pssh_boxes(moov_body: &[u8]) -> Vec<Bytes> {
	boxes(moov_body)
		.filter(|(kind, _, _)| kind == b"pssh")
		.map(|(_, whole, _)| Bytes::copy_from_slice(whole))
		.collect()
}

/// Insert `boxes` at the end of the `moov` in an encoded `ftyp`+`moov` init segment.
pub(super) fn append_to_moov(init: &mut Vec<u8>, extra: &[Bytes]) {
	if extra.is_empty() {
		return;
	}
	let mut offset = 0;
	loop {
		let next = boxes(&init[offset..])
			.next()
			.map(|(kind, whole, _)| (kind, whole.len()));
		let Some((kind, len)) = next else {
			return;
		};
		if &kind == b"moov" {
			let added: usize = extra.iter().map(Bytes::len).sum();
			let end = offset + len;
			let tail = init.split_off(end);
			for pssh in extra {
				init.extend_from_slice(pssh);
			}
			init.extend_from_slice(&tail);
			// A rebuilt moov is far below 4 GiB, so its size stays in the 32-bit field.
			let size = (len + added) as u32;
			init[offset..offset + 4].copy_from_slice(&size.to_be_bytes());
			return;
		}
		offset += len;
	}
}

/// Put an encrypted sample entry back into an init segment encoded with its original entry.
///
/// `mp4-atom` can't encode an `encv`/`enca` entry, so a protected track's init is encoded with
/// the unwrapped codec; this swaps the first `stsd` entry of the first track back to the
/// encrypted one, `sinf` included, and corrects the sizes of every box around it.
pub(super) fn restore_entry(init: &mut Vec<u8>, encrypted: &mp4_atom::Codec) -> Result<(), super::Error> {
	let mp4_atom::Codec::Unknown(kind, body) = encrypted else {
		return Ok(());
	};
	let invalid = || super::Error::InvalidProtection("rebuilt init has no sample entry");
	// The absolute offset of each box on the path to the stsd.
	let mut ancestors = Vec::new();
	let mut start = 0;
	for (depth, kind) in [b"moov", b"trak", b"mdia", b"minf", b"stbl", b"stsd"]
		.iter()
		.enumerate()
	{
		let scope = if depth == 0 {
			&init[..]
		} else {
			&init[start..ancestors.last().map(|(o, l)| o + l).unwrap()]
		};
		let mut offset = 0;
		let (len, header) = boxes(scope)
			.find_map(|(k, whole, payload)| {
				let found = (&k == *kind).then(|| (offset, whole.len(), whole.len() - payload.len()));
				offset += whole.len();
				found
			})
			.map(|(at, len, header)| {
				start += at;
				(len, header)
			})
			.ok_or_else(invalid)?;
		ancestors.push((start, len));
		start += header;
	}
	// stsd: version and flags (4), entry count (4), then the entries.
	let entry = start + 8;
	let old = boxes(init.get(entry..).ok_or_else(invalid)?)
		.next()
		.map(|(_, whole, _)| whole.len())
		.ok_or_else(invalid)?;

	let mut replacement = Vec::with_capacity(8 + body.len());
	let size = u32::try_from(8 + body.len()).map_err(|_| super::Error::InvalidProtection("sample entry too large"))?;
	replacement.extend_from_slice(&size.to_be_bytes());
	replacement.extend_from_slice(&<[u8; 4]>::from(*kind));
	replacement.extend_from_slice(body);
	init.splice(entry..entry + old, replacement);

	let delta = (8 + body.len()) as i64 - old as i64;
	for (offset, len) in ancestors {
		let size = u32::try_from(len as i64 + delta).map_err(|_| super::Error::InvalidProtection("box too large"))?;
		init[offset..offset + 4].copy_from_slice(&size.to_be_bytes());
	}
	Ok(())
}

/// Where the first sample's IV starts in an encoded single-`traf` `moof`, relative to the
/// `moof`: past the `senc` header (8), its version and flags (4) and its sample count (4).
///
/// A track fragment's `saio` addresses its auxiliary information from the `moof` (the
/// default-base-is-moof addressing passthrough writes), so a rebuilt fragment, whose boxes moved,
/// must point it here again.
pub(super) fn senc_iv_offset(moof: &[u8]) -> Option<u64> {
	let (kind, whole, payload) = boxes(moof).next()?;
	if &kind != b"moof" {
		return None;
	}
	let mut at = whole.len() - payload.len();
	for (kind, whole, payload) in boxes(payload) {
		if &kind == b"traf" {
			let mut inner = at + whole.len() - payload.len();
			for (kind, whole, _) in boxes(payload) {
				if &kind == b"senc" {
					return Some((inner + 16) as u64);
				}
				inner += whole.len();
			}
			return None;
		}
		at += whole.len();
	}
	None
}

/// Whether `saio` locates Common Encryption auxiliary information: an untyped one, as CMAF
/// writes, or one typed with a CENC scheme.
pub(super) fn is_cenc_saio(saio: &mp4_atom::Saio) -> bool {
	saio.aux_info.as_ref().is_none_or(|aux| {
		matches!(
			&<[u8; 4]>::from(aux.aux_info_type),
			b"cenc" | b"cbcs" | b"cens" | b"cbc1"
		)
	})
}

/// For an encrypted sample entry (`encv`/`enca`), the original entry it wraps: `frma`'s format
/// with the same fields and children, minus `sinf`. `None` when the entry isn't encrypted.
pub(super) fn unwrap(codec: &mp4_atom::Codec) -> Result<Option<mp4_atom::Codec>, super::Error> {
	let mp4_atom::Codec::Unknown(kind, body) = codec else {
		return Ok(None);
	};
	let fields = match <[u8; 4]>::from(*kind) {
		ENCV => VISUAL_FIELDS,
		ENCA => AUDIO_FIELDS,
		_ => return Ok(None),
	};
	let body = body.as_slice();
	if body.len() < fields {
		return Err(super::Error::InvalidProtection("short encrypted sample entry"));
	}
	let (head, children) = body.split_at(fields);
	let sinf = child(children, b"sinf").ok_or(super::Error::InvalidProtection("missing sinf"))?;
	let format = child(sinf, b"frma")
		.and_then(|frma| frma.get(..4))
		.ok_or(super::Error::InvalidProtection("missing frma"))?;

	let mut entry = Vec::with_capacity(8 + body.len());
	entry.extend_from_slice(&[0; 4]);
	entry.extend_from_slice(format);
	entry.extend_from_slice(head);
	for (kind, whole, _) in boxes(children) {
		if &kind != b"sinf" {
			entry.extend_from_slice(whole);
		}
	}
	let size = u32::try_from(entry.len()).map_err(|_| super::Error::InvalidProtection("sample entry too large"))?;
	entry[..4].copy_from_slice(&size.to_be_bytes());
	let original = mp4_atom::Codec::decode(&mut entry.as_slice())?;
	if matches!(original, mp4_atom::Codec::Unknown(..)) {
		return Err(super::Error::UnknownCodec(FourCC::new(&format.try_into().unwrap())));
	}
	Ok(Some(original))
}

/// How the single track of a one-track `ftyp`+`moov` init segment is protected, if it is.
pub(crate) fn protection(init: &[u8]) -> Option<Protection> {
	let moov = child(init, b"moov")?;
	let stsd = child(moov, b"trak")
		.and_then(|trak| child(trak, b"mdia"))
		.and_then(|mdia| child(mdia, b"minf"))
		.and_then(|minf| child(minf, b"stbl"))
		.and_then(|stbl| child(stbl, b"stsd"))?;
	// stsd: version and flags (4), entry count (4), then the entries.
	let (kind, _, entry) = boxes(stsd.get(8..)?).next()?;
	let fields = match kind {
		ENCV => VISUAL_FIELDS,
		ENCA => AUDIO_FIELDS,
		_ => return None,
	};
	let sinf = child(entry.get(fields..)?, b"sinf")?;
	// schm: version and flags (4), scheme type (4), scheme version (4).
	let scheme = child(sinf, b"schm")?.get(4..8)?;
	// tenc: version and flags (4), two reserved or pattern bytes, isProtected, IV size, KID (16).
	let kid = child(sinf, b"schi").and_then(|schi| child(schi, b"tenc"))?.get(8..24)?;

	let systems = boxes(moov)
		.filter(|(kind, _, _)| kind == b"pssh")
		.filter_map(|(_, whole, payload)| {
			// pssh: version and flags (4), system ID (16).
			let id = payload.get(4..20)?;
			Some((
				uuid(id),
				base64::Engine::encode(&base64::engine::general_purpose::STANDARD, whole),
			))
		})
		.collect();
	Some(Protection {
		scheme: String::from_utf8_lossy(scheme).into_owned(),
		default_kid: uuid(kid),
		systems,
	})
}

/// Sixteen bytes as a lowercase UUID string.
fn uuid(bytes: &[u8]) -> String {
	let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
	format!(
		"{}-{}-{}-{}-{}",
		&hex[0..8],
		&hex[8..12],
		&hex[12..16],
		&hex[16..20],
		&hex[20..32]
	)
}

#[cfg(test)]
mod tests {
	use super::*;

	/// One second of 320x180 H.264 and AAC, encrypted by ffmpeg with `cenc-aes-ctr` under the
	/// public test KID a1b2c3d4-e5f6-0718-293a-4b5c6d7e8f90, plus a ClearKey `pssh`.
	const CLEARKEY: &[u8] = include_bytes!("test_data/cenc-clearkey.mp4");

	fn moov_body() -> &'static [u8] {
		child(CLEARKEY, b"moov").unwrap()
	}

	#[test]
	fn finds_the_clearkey_pssh() {
		let pssh = pssh_boxes(moov_body());
		assert_eq!(pssh.len(), 1);
		assert_eq!(&pssh[0][4..8], b"pssh");
	}

	#[test]
	fn unwraps_encrypted_entries_to_their_original_codec() {
		let moov = mp4_atom::Moov::decode(&mut child_whole(CLEARKEY, b"moov")).unwrap();
		let codecs: Vec<_> = moov
			.trak
			.iter()
			.map(|trak| unwrap(&trak.mdia.minf.stbl.stsd.codecs[0]).unwrap().unwrap())
			.collect();
		assert!(matches!(codecs[0], mp4_atom::Codec::Avc1(_)), "{:?}", codecs[0]);
		assert!(matches!(codecs[1], mp4_atom::Codec::Mp4a(_)), "{:?}", codecs[1]);
	}

	#[test]
	fn reads_the_protection_of_the_first_track() {
		let protection = protection(CLEARKEY).unwrap();
		assert_eq!(protection.scheme, "cenc");
		assert_eq!(protection.default_kid, "a1b2c3d4-e5f6-0718-293a-4b5c6d7e8f90");
		assert_eq!(protection.systems.len(), 1);
		assert_eq!(protection.systems[0].0, "1077efec-c0b2-4d02-ace3-3c1e52e2fb4b");
	}

	#[test]
	fn rebuilds_a_protected_single_track_init() {
		// As the importer does: encode the track with its unwrapped codec, then restore the
		// encrypted entry and append the pssh.
		let moov = mp4_atom::Moov::decode(&mut child_whole(CLEARKEY, b"moov")).unwrap();
		let mut single = moov.clone();
		single.trak.truncate(1);
		let encrypted = single.trak[0].mdia.minf.stbl.stsd.codecs[0].clone();
		single.trak[0].mdia.minf.stbl.stsd.codecs[0] = unwrap(&encrypted).unwrap().unwrap();
		let mut init = Vec::new();
		mp4_atom::Encode::encode(&single, &mut init).unwrap();
		restore_entry(&mut init, &encrypted).unwrap();
		append_to_moov(&mut init, &pssh_boxes(moov_body()));

		let moov = mp4_atom::Moov::decode(&mut init.as_slice()).expect("still a valid moov");
		assert_eq!(moov.trak[0].mdia.minf.stbl.stsd.codecs[0], encrypted);
		assert_eq!(protection(&init), protection(CLEARKEY));
	}

	#[test]
	fn finds_the_ivs_in_a_reencoded_moof() {
		// mp4-atom writes senc after the other traf children, wherever the source had it.
		let moof = mp4_atom::Moof::decode(&mut child_whole(CLEARKEY, b"moof")).unwrap();
		let mut encoded = Vec::new();
		mp4_atom::Encode::encode(&moof, &mut encoded).unwrap();
		let offset = senc_iv_offset(&encoded).unwrap() as usize;
		// The senc header sits 16 bytes before the IVs: size, "senc", version and flags, count.
		assert_eq!(&encoded[offset - 12..offset - 8], b"senc");
		let senc = moof.traf[0].senc.as_ref().unwrap();
		assert_eq!(&encoded[offset..offset + 8], &senc.data[4..12], "the first IV");
	}

	#[test]
	fn a_clear_entry_is_not_unwrapped() {
		let avc1 = mp4_atom::Codec::Unknown(FourCC::new(b"abcd"), Vec::new());
		assert!(unwrap(&avc1).unwrap().is_none());
	}

	fn child_whole<'a>(data: &'a [u8], kind: &[u8; 4]) -> &'a [u8] {
		boxes(data)
			.find(|(k, _, _)| k == kind)
			.map(|(_, whole, _)| whole)
			.unwrap()
	}
}
