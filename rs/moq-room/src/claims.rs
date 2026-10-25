//! The LiveKit AccessToken analogue: minting a moq-auth token rooted at the room prefix.
//!
//! This crate has no service and no storage. Joining a room is signing a token
//! with these claims and dialing the relay at that root.

/// Token claims for a participant in `room`.
///
/// `root` is the room prefix, `subscribe` is `**` (everything under the room),
/// and `publish` is `<identity>/**` so a participant cannot publish at anyone
/// else's paths. An identity that is empty or contains `*` is
/// refused, since a wildcard would let the participant publish as someone else.
pub fn claims(room: impl moq_net::AsPath, identity: impl moq_net::AsPath) -> Result<moq_auth::Claims, crate::Error> {
	let room = room.as_path();
	let identity = identity.as_path();
	if identity.is_empty() {
		return Err(crate::Error::EmptyIdentity);
	}
	let publish = moq_auth::Pattern::subtree(identity.as_str())?;
	Ok(moq_auth::Claims::default()
		.with_root(room.as_str())
		.with_subscribe([moq_auth::Pattern::all()])
		.with_publish([publish]))
}

#[cfg(test)]
mod tests {
	use super::*;
	use moq_net::Path;

	fn patterns(texts: &[&str]) -> moq_auth::Patterns {
		texts.iter().map(|text| text.parse().unwrap()).collect()
	}

	#[test]
	fn rejects_empty_identity() {
		assert!(matches!(
			claims(Path::new("room").unwrap(), Path::empty()),
			Err(crate::Error::EmptyIdentity)
		));
	}

	#[test]
	fn rejects_wildcard_identity() {
		for identity in ["*", "alice*", "a/*/b", "**"] {
			assert!(matches!(
				claims(Path::new("room").unwrap(), Path::new(identity).unwrap()),
				Err(crate::Error::InvalidIdentity(_))
			));
		}
	}

	#[test]
	fn claims_root_the_token_and_scope_publish_to_identity() {
		let c = claims(Path::new("meet/demo").unwrap(), Path::new("alice").unwrap()).unwrap();
		assert_eq!(c.root, "meet/demo");
		assert_eq!(c.subscribe, patterns(&["**"]));
		assert_eq!(c.publish, patterns(&["alice/**"]));
	}

	#[test]
	fn claims_accepts_a_multi_segment_identity() {
		let c = claims(Path::new("hang/room").unwrap(), Path::new("guest/uuid").unwrap()).unwrap();
		assert_eq!(c.root, "hang/room");
		assert_eq!(c.publish, patterns(&["guest/uuid/**"]));
	}
}
