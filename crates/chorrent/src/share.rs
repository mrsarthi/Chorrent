use crate::error::Error;
use crate::manifest::ShareId;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use iroh::{EndpointAddr, EndpointId};
use iroh_gossip::proto::TopicId;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

const SHARE_CODE_PREFIX: &str = "chr2";

/// Everything a downloader needs to find a share's swarm and verify it.
///
/// Parse one with `str::parse` and print one with `Display`; the text form is
/// what you hand to other people. A private share's code contains its secret,
/// so anyone holding the code can join.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShareCode {
    pub(crate) id: ShareId,
    pub(crate) name: String,
    pub(crate) total_size: u64,
    /// Peers to try first, before gossip finds more.
    pub(crate) peers: Vec<EndpointAddr>,
    pub(crate) secret: Option<[u8; 32]>,
}

impl ShareCode {
    /// Identifies the share's content (the hash of its file list).
    pub fn id(&self) -> ShareId {
        self.id
    }

    /// Name of the shared file or top-level folder, as the seeder named it.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Combined size of all files, in bytes, as the share code states it.
    /// A download fails if the actual file list doesn't match.
    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    /// Whether only holders of this code can find or download the share.
    pub fn is_private(&self) -> bool {
        self.secret.is_some()
    }

    pub(crate) fn topic(&self) -> TopicId {
        topic_for(self.id, self.secret.as_ref())
    }

    pub(crate) fn bootstrap_ids(&self) -> Vec<EndpointId> {
        self.peers.iter().map(|a| a.id).collect()
    }
}

impl fmt::Display for ShareCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = postcard::to_allocvec(self).map_err(|_| fmt::Error)?;
        write!(f, "{SHARE_CODE_PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes))
    }
}

impl FromStr for ShareCode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let body = s.trim().strip_prefix(SHARE_CODE_PREFIX).ok_or_else(|| {
            Error::InvalidShareCode("not a chorrent v2 share code (should start with \"chr2\")".into())
        })?;
        let bytes = URL_SAFE_NO_PAD
            .decode(body)
            .map_err(|e| Error::InvalidShareCode(format!("not valid base64: {e}")))?;
        postcard::from_bytes(&bytes).map_err(|e| Error::InvalidShareCode(format!("corrupted: {e}")))
    }
}

/// The gossip topic for a share. Public shares use a topic anyone with the
/// share id can compute; private ones mix in the secret, so knowing the
/// content alone isn't enough to find the swarm.
pub(crate) fn topic_for(id: ShareId, secret: Option<&[u8; 32]>) -> TopicId {
    let bytes = match secret {
        None => blake3::keyed_hash(&blake3::derive_key("chorrent v2 public topic", &[]), &id.0),
        Some(secret) => blake3::keyed_hash(&blake3::derive_key("chorrent v2 private topic", secret), &id.0),
    };
    TopicId::from_bytes(*bytes.as_bytes())
}

/// Proof that the peer `requester` knows a private share's secret. It's bound
/// to the requester's endpoint id (authenticated by QUIC), so a token
/// overheard on the wire is useless to anyone else.
pub(crate) fn access_token(secret: &[u8; 32], requester: &EndpointId) -> [u8; 32] {
    *blake3::keyed_hash(&blake3::derive_key("chorrent v2 access", secret), requester.as_bytes()).as_bytes()
}

pub(crate) fn new_secret() -> [u8; 32] {
    rand::random()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_topics_differ_from_public_and_by_secret() {
        let id = ShareId([1; 32]);
        let public = topic_for(id, None);
        assert_ne!(public, topic_for(id, Some(&[2; 32])));
        assert_ne!(topic_for(id, Some(&[2; 32])), topic_for(id, Some(&[3; 32])));
    }

    #[test]
    fn share_code_round_trips_and_rejects_v1() {
        let code = ShareCode { id: ShareId([7; 32]), name: "x".into(), total_size: 5, peers: vec![], secret: Some([1; 32]) };
        let text = code.to_string();
        assert!(text.starts_with("chr2"));
        assert_eq!(text.parse::<ShareCode>().unwrap(), code);
        assert!("kAFlbmRwb2ludA".parse::<ShareCode>().is_err());
    }
}
