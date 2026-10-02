use crate::error::{Error, Result};
use crate::handler::Registry;
use crate::local::SharedShare;
use std::sync::Arc;

/// A share listed in the node's registry, so peers can fetch from it.
/// Unlisted again when dropped.
pub(crate) struct Registration {
    registry: Registry,
    pub share: SharedShare,
}

impl Registration {
    pub fn new(registry: &Registry, share: SharedShare) -> Result<Self> {
        let mut shares = registry.write().unwrap();
        if shares.contains_key(&share.id) {
            return Err(Error::AlreadySharing);
        }
        shares.insert(share.id, Arc::clone(&share));
        Ok(Self { registry: Arc::clone(registry), share })
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut shares = self.registry.write().unwrap();
        if shares.get(&self.share.id).is_some_and(|s| Arc::ptr_eq(s, &self.share)) {
            shares.remove(&self.share.id);
        }
    }
}
