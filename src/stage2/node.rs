use crate::error::NodeError;
use iroh::endpoint::{presets, Connection};
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr};
use iroh_gossip::Gossip;
use std::sync::Arc;

pub const ALPN: &[u8] = b"chorrent/0.1";

pub struct ChorrentNode {
    endpoint: Endpoint,
    gossip: Gossip,
    _router: Router,
}

impl ChorrentNode {
    /// Bind a node that can dial out, and accepts incoming connections
    /// for both our own protocol and gossip.
    pub async fn bind<H: iroh::protocol::ProtocolHandler>(handler: H) -> Result<Self, NodeError> {
        let endpoint = Endpoint::builder(presets::N0)
            .bind()
            .await
            .map_err(|e| NodeError::Bind { message: e.to_string() })?;

        let gossip = Gossip::builder().spawn(endpoint.clone());

        let router = Router::builder(endpoint.clone())
            .accept(ALPN.to_vec(), Arc::new(handler))
            .accept(iroh_gossip::ALPN, gossip.clone())
            .spawn();

        Ok(Self { endpoint, gossip, _router: router })
    }

    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    pub async fn connect(&self, addr: EndpointAddr) -> Result<Connection, NodeError> {
        self.endpoint
            .connect(addr, ALPN)
            .await
            .map_err(|e| NodeError::Connect { message: e.to_string() })
    }

    pub fn gossip(&self) -> &Gossip {
        &self.gossip
    }
}