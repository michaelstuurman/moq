//! IETF moq-transport TRACK_STATUS and its answer.

use std::borrow::Cow;

use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::{
	Path,
	coding::*,
	ietf::{Filter, Location, Properties, RequestId, Subscribe},
};

use super::Message;
use super::namespace::encode_namespace;

use super::Version;

/// TrackStatus message (0x0d), with the same wire format as SUBSCRIBE. The response is
/// REQUEST_OK.
#[derive(Clone, Debug)]
pub struct TrackStatus<'a> {
	pub request_id: RequestId,
	pub track_namespace: Path<'a>,
	pub track_name: Cow<'a, str>,
	/// Whether the requester wants Track Properties on the answer (INCLUDE_PROPERTIES,
	/// draft-20).
	pub properties_wanted: bool,
	/// The publisher instance the requester expects (MoQ Epoch).
	pub epoch: Option<crate::Epoch>,
}

impl Message for TrackStatus<'_> {
	const ID: u64 = 0x0d;

	fn encode_msg(&self, w: &mut Encoder<'_>, version: Version) -> Result<(), EncodeError> {
		self.request_id.encode(w, version)?;
		encode_namespace(w, &self.track_namespace)?;
		w.string(&self.track_name)?;

		// Only the opt-out is worth bytes, and an older peer would read the
		// parameter as unknown, which is a protocol violation.
		let include_properties = (!self.properties_wanted && Filter::is_draft20(version)).then_some(false);
		let epoch = self.epoch.clone();
		encode_params!(w, version,
			0x35 => include_properties,
			super::epoch::EPOCH_PARAM => epoch,
		);
		Ok(())
	}

	/// Every draft defines TRACK_STATUS as identical to SUBSCRIBE, so it decodes as one and
	/// keeps what names the track and shapes the answer. The rest goes unread: there is no
	/// subscription for it to configure.
	fn decode_msg(r: &mut Decoder<'_>, version: Version) -> Result<Self, DecodeError> {
		let subscribe = Subscribe::decode_msg(r, version)?;
		Ok(Self {
			request_id: subscribe.request_id,
			track_namespace: subscribe.track_namespace,
			track_name: subscribe.track_name,
			properties_wanted: subscribe.properties_wanted,
			epoch: subscribe.epoch,
		})
	}
}

/// TRACK_STATUS_OK: what a SUBSCRIBE_OK would say about the track, with no subscription,
/// sent as a REQUEST_OK with Track Properties.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrackStatusOk {
	/// The largest Location in the track (LARGEST_OBJECT), absent while it has no content.
	pub largest: Option<Location>,

	/// The Track Properties.
	pub properties: Properties,
}

impl Message for TrackStatusOk {
	/// REQUEST_OK.
	const ID: u64 = 0x07;

	fn encode_msg(&self, w: &mut Encoder<'_>, version: Version) -> Result<(), EncodeError> {
		// The parameters SUBSCRIBE_OK carries.
		encode_params!(w, version,
			0x09 => self.largest,
		);

		// Track Properties are the final field, so nothing may follow.
		self.properties.encode(w, version)?;
		Ok(())
	}

	fn decode_msg(r: &mut Decoder<'_>, version: Version) -> Result<Self, DecodeError> {
		// Read as SUBSCRIBE_OK's are: EXPIRES is ignored.
		decode_params!(r, version,
			0x08 => _expires: Option<u64>,
			0x09 => largest: Option<Location>,
		);
		let properties = Properties::decode(r, version)?;

		Ok(Self { largest, properties })
	}
}

#[derive(Clone, Copy, Debug, TryFromPrimitive, IntoPrimitive)]
#[repr(u64)]
pub enum TrackStatusCode {
	InProgress = 0x00,
	NotFound = 0x01,
	NotAuthorized = 0x02,
	Ended = 0x03,
}

impl Encode<Version> for TrackStatusCode {
	fn encode(&self, w: &mut Encoder<'_>, _: Version) -> Result<(), EncodeError> {
		w.varint(u64::from(*self))?;
		Ok(())
	}
}

impl Decode<Version> for TrackStatusCode {
	fn decode(r: &mut Decoder<'_>, _: Version) -> Result<Self, DecodeError> {
		Self::try_from(r.varint()?).map_err(|_| DecodeError::InvalidValue)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn encode_message<M: Message>(msg: &M, version: Version) -> Vec<u8> {
		let mut buf = Vec::new();
		msg.encode_msg(&mut Encoder::new(&mut buf, version.into()), version)
			.unwrap();
		buf.to_vec()
	}

	/// GROUP_ORDER and MAX_CACHE_DURATION are TRACK_STATUS_OK parameters only on draft-15,
	/// as on SUBSCRIBE_OK. Draft-16 ignores a GROUP_ORDER it defines elsewhere, even with a
	/// value no field could hold, and rejects 0x04, which it does not define at all. Draft-18
	/// rejects a known parameter on a message that does not define it.
	#[test]
	fn track_status_ok_parameters_follow_the_draft() {
		// Request 1, then GROUP_ORDER as the varint 256.
		let ok = decode_message::<TrackStatusOk>(&[0x01, 0x01, 0x22, 0x41, 0x00], Version::Draft16)
			.expect("draft-16 ignores GROUP_ORDER");
		assert_eq!(ok.properties.group_order, None);
		// Request 1, then MAX_CACHE_DURATION of 5.
		assert!(decode_message::<TrackStatusOk>(&[0x01, 0x01, 0x04, 0x05], Version::Draft16).is_err());
		// GROUP_ORDER = Ascending, then an empty properties block.
		assert!(decode_message::<TrackStatusOk>(&[0x01, 0x22, 0x01], Version::Draft18).is_err());
	}

	fn decode_message<M: Message>(bytes: &[u8], version: Version) -> Result<M, DecodeError> {
		let mut buf = bytes::Bytes::from(bytes.to_vec());
		crate::coding::decode_buf(&mut buf, version, M::decode_msg)
	}

	#[test]
	fn test_track_status_v14_round_trip() {
		let msg = TrackStatus {
			request_id: RequestId(1),
			track_namespace: Path::new("test/ns"),
			track_name: "video".into(),
			properties_wanted: true,
			epoch: None,
		};

		let encoded = encode_message(&msg, Version::Draft14);
		let decoded: TrackStatus = decode_message(&encoded, Version::Draft14).unwrap();

		assert_eq!(decoded.request_id, RequestId(1));
		assert_eq!(decoded.track_namespace.as_str(), "test/ns");
		assert_eq!(decoded.track_name, "video");
	}

	#[test]
	fn test_track_status_v15_round_trip() {
		let msg = TrackStatus {
			request_id: RequestId(1),
			track_namespace: Path::new("test/ns"),
			track_name: "video".into(),
			properties_wanted: true,
			epoch: None,
		};

		let encoded = encode_message(&msg, Version::Draft15);
		let decoded: TrackStatus = decode_message(&encoded, Version::Draft15).unwrap();

		assert_eq!(decoded.request_id, RequestId(1));
		assert_eq!(decoded.track_namespace.as_str(), "test/ns");
		assert_eq!(decoded.track_name, "video");
	}

	#[test]
	fn test_track_status_v17_round_trip() {
		let msg = TrackStatus {
			request_id: RequestId(1),
			track_namespace: Path::new("test/ns"),
			track_name: "video".into(),
			properties_wanted: true,
			epoch: None,
		};

		let encoded = encode_message(&msg, Version::Draft17);
		let decoded: TrackStatus = decode_message(&encoded, Version::Draft17).unwrap();

		assert_eq!(decoded.request_id, RequestId(1));
		assert_eq!(decoded.track_namespace.as_str(), "test/ns");
		assert_eq!(decoded.track_name, "video");
	}

	#[test]
	fn test_track_status_v16_round_trip() {
		let msg = TrackStatus {
			request_id: RequestId(1),
			track_namespace: Path::new("test/ns"),
			track_name: "video".into(),
			properties_wanted: true,
			epoch: None,
		};

		let encoded = encode_message(&msg, Version::Draft16);
		let decoded: TrackStatus = decode_message(&encoded, Version::Draft16).unwrap();

		assert_eq!(decoded.request_id, RequestId(1));
		assert_eq!(decoded.track_namespace.as_str(), "test/ns");
		assert_eq!(decoded.track_name, "video");
	}

	#[test]
	fn test_track_status_v18_round_trip() {
		let msg = TrackStatus {
			request_id: RequestId(1),
			track_namespace: Path::new("test/ns"),
			track_name: "video".into(),
			properties_wanted: true,
			epoch: None,
		};

		let encoded = encode_message(&msg, Version::Draft18);
		let decoded: TrackStatus = decode_message(&encoded, Version::Draft18).unwrap();

		assert_eq!(decoded.request_id, RequestId(1));
		assert_eq!(decoded.track_namespace.as_str(), "test/ns");
		assert_eq!(decoded.track_name, "video");
	}

	const ALL: [Version; 9] = [
		Version::Draft18,
		Version::Draft19,
		Version::Draft20,
		Version::Draft21,
		Version::Draft22,
	];

	/// INCLUDE_PROPERTIES only exists from draft-20, so the opt-out is dropped before it,
	/// and the default (1) is never written.
	#[test]
	fn test_track_status_opts_out_of_properties_from_draft_20() {
		for version in ALL {
			for wanted in [true, false] {
				let msg = TrackStatus {
					request_id: RequestId(1),
					track_namespace: Path::new("ns"),
					track_name: "video".into(),
					properties_wanted: wanted,
					epoch: None,
				};
				let decoded: TrackStatus = decode_message(&encode_message(&msg, version), version)
					.unwrap_or_else(|e| panic!("{version}: {e}"));
				let expected = wanted || !Filter::is_draft20(version);
				assert_eq!(decoded.properties_wanted, expected, "{version} wanted={wanted}");
			}
		}
	}

	/// TRACK_STATUS_OK carries what SUBSCRIBE_OK would, as far as each draft's answer
	/// has room: draft-14 is a SUBSCRIBE_OK, drafts 15-17 a REQUEST_OK with only
	/// parameters, and from draft-18 the REQUEST_OK ends with the Track Properties.
	#[test]
	fn test_track_status_ok_round_trip() {
		let properties = Properties {
			unknown: Default::default(),
			dynamic_groups: false,
			compress: false,
			max_cache_duration: Some(std::time::Duration::from_secs(30)),
			timescale: Some(crate::Timescale::MILLI),
			priority: Some(7),
			group_order: Some(GroupOrder::Descending),
		};
		let largest = Some(Location { group: 5, object: 2 });

		for version in ALL {
			let msg = TrackStatusOk {
				largest,
				properties: properties.clone(),
			};
			let decoded: TrackStatusOk =
				decode_message(&encode_message(&msg, version), version).unwrap_or_else(|e| panic!("{version}: {e}"));

			let expected = properties.clone();
			assert_eq!(decoded.request_id, msg.request_id, "{version}");
			assert_eq!(decoded.largest, largest, "{version}");
			assert_eq!(decoded.properties, expected, "{version}");
		}
	}

	/// Draft-14's TRACK_STATUS_OK is a SUBSCRIBE_OK with Track Alias 0, under its own type.
	#[test]
	fn test_track_status_ok_draft_14_is_a_subscribe_ok() {
		let msg = TrackStatusOk {
			largest: None,
			properties: Properties::default(),
		};
		assert_eq!(TrackStatusOk::id(Version::Draft14), 0x0E);
		assert_eq!(TrackStatusOk::id(Version::Draft15), 0x07);
		let subscribe_ok = SubscribeOk {
			track_alias: 0,
			largest: None,
			properties: Properties::default(),
		};
		assert_eq!(
			encode_message(&msg, Version::Draft14),
			encode_message(&subscribe_ok, Version::Draft14)
		);
	}

	/// TRACK_STATUS is identical to SUBSCRIBE on every draft, so it carries whatever
	/// fields and parameters a SUBSCRIBE can. A peer that sends them must still be
	/// answered rather than having its session closed.
	#[test]
	fn test_track_status_carries_subscribe_fields() {
		// Request ID 1, Track Namespace ("live"), Track Name ("video").
		let head: &[u8] = &[
			0x01, 0x01, 0x04, b'l', b'i', b'v', b'e', 0x05, b'v', b'i', b'd', b'e', b'o',
		];

		#[rustfmt::skip]
		let cases: [(Version, &[u8]); 3] = [
			(Version::Draft14, &[
				0x80, // Subscriber Priority
				0x02, // Group Order
				0x00, // Forward
				0x03, 0x05, 0x01, // Filter Type AbsoluteStart, at {5, 1}
				0x00, // Number of Parameters
			]),
			(Version::Draft15, &[
				0x02, // Number of Parameters
				0x10, 0x00, // FORWARD = 0
				0x20, 0x01, // SUBSCRIBER_PRIORITY = 1
			]),
			(Version::Draft20, &[
				0x02, // Number of Parameters
				0x03, 0x03, 0x03, 0x00, 0xAA, // AUTHORIZATION TOKEN
				0x32, 0x00, // INCLUDE_PROPERTIES (0x35) = 0
			]),
		];

		for (version, rest) in cases {
			let body = [head, rest].concat();
			let wire = body;
			let mut buf = Decoder::new(&wire, version.into());
			let msg = TrackStatus::decode_msg(&mut buf, version).unwrap_or_else(|e| panic!("{version}: {e}"));
			assert!(buf.is_empty(), "{version}: trailing bytes");
			assert_eq!(msg.track_name, "video", "{version}");
			assert_eq!(msg.properties_wanted, version != Version::Draft20, "{version}");
		}
	}
}
