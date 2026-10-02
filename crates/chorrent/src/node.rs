use crate::error::NodeError;
use crate::protocol::ALPN;
use iroh::endpoint::{presets, Connection};
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use iroh_gossip::Gossip;
use std::sync::Arc;

pub(crate) struct ChorrentNode {
    endpoint: Endpoint,
    gossip: Gossip,
    router: Router,
}

impl ChorrentNode {
    /// Bind a node that can dial out, and accepts incoming connections
    /// for both our own protocol and gossip.
    pub async fn bind<H: iroh::protocol::ProtocolHandler>(
        handler: H,
        secret_key: SecretKey,
        mainline_dht: bool,
    ) -> Result<Self, NodeError> {
        let builder = Endpoint::builder(presets::N0).secret_key(secret_key);
        #[cfg(feature = "mainline")]
        let builder = match mainline_dht {
            true => builder.address_lookup(iroh_mainline_address_lookup::DhtAddressLookup::builder()),
            false => builder,
        };
        #[cfg(not(feature = "mainline"))]
        let _ = mainline_dht;
        let endpoint = builder
            .bind()
            .await
            .map_err(|e| NodeError::Bind { message: e.to_string() })?;

        let gossip = Gossip::builder().spawn(endpoint.clone());

        let router = Router::builder(endpoint.clone())
            .accept(ALPN, Arc::new(handler))
            .accept(iroh_gossip::ALPN, gossip.clone())
            .spawn();

        Ok(Self { endpoint, gossip, router })
    }

    pub fn id(&self) -> EndpointId {
        self.endpoint.id()
    }

    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    /// Our address once we're reachable through a relay (or after a short
    /// wait if that's not possible), so share codes carry a usable address.
    pub async fn reachable_addr(&self) -> EndpointAddr {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), self.endpoint.online()).await;
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

    /// Stop accepting connections and close the endpoint.
    pub async fn shutdown(&self) {
        let _ = self.router.shutdown().await;
    }
}
