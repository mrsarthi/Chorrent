use crate::error::NodeError;
use crate::handler::ChorrentProtocol;
use crate::protocol::ALPN;
use iroh::endpoint::{presets, Connection};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use iroh_gossip::Gossip;
use std::sync::Arc;

/// The network side of a client: an iroh endpoint, either our own or one
/// the embedding app already runs.
pub(crate) struct ChorrentNode {
    endpoint: Endpoint,
    handler: ChorrentProtocol,
    gossip: Option<Gossip>,
    /// Only when we own the endpoint; an embedding app routes connections itself.
    router: Option<Router>,
}

impl ChorrentNode {
    /// Bind our own endpoint, accepting our protocol (and gossip, if enabled).
    pub async fn bind(
        handler: ChorrentProtocol,
        secret_key: SecretKey,
        gossip: bool,
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

        let gossip = gossip.then(|| Gossip::builder().spawn(endpoint.clone()));
        let mut router = Router::builder(endpoint.clone()).accept(ALPN, Arc::new(handler.clone()));
        if let Some(gossip) = &gossip {
            router = router.accept(iroh_gossip::ALPN, gossip.clone());
        }
        Ok(Self { endpoint, handler, gossip, router: Some(router.spawn()) })
    }

    /// Use an endpoint the embedding app owns. It must list our ALPNs and
    /// pass matching incoming connections to [`ChorrentNode::handle`].
    pub fn attach(endpoint: Endpoint, handler: ChorrentProtocol, gossip: bool) -> Self {
        let gossip = gossip.then(|| Gossip::builder().spawn(endpoint.clone()));
        Self { endpoint, handler, gossip, router: None }
    }

    pub fn alpns(&self) -> Vec<Vec<u8>> {
        let mut alpns = vec![ALPN.to_vec()];
        if self.gossip.is_some() {
            alpns.push(iroh_gossip::ALPN.to_vec());
        }
        alpns
    }

    /// Serve an incoming connection the embedding app accepted for us.
    pub async fn handle(&self, connection: Connection) -> Result<(), AcceptError> {
        if connection.alpn() == ALPN {
            self.handler.accept(connection).await
        } else if let Some(gossip) = self.gossip.as_ref().filter(|_| connection.alpn() == iroh_gossip::ALPN) {
            gossip.accept(connection).await
        } else {
            connection.close(0u32.into(), b"unknown protocol");
            Ok(())
        }
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

    /// Our relay server, if we are connected to one right now.
    pub fn relay(&self) -> Option<String> {
        use iroh::Watcher;
        self.endpoint
            .home_relay_status()
            .get()
            .into_iter()
            .find(|s| s.is_connected())
            .map(|s| s.url().to_string())
    }

    pub fn gossip(&self) -> Option<&Gossip> {
        self.gossip.as_ref()
    }

    /// Stop. Closes the endpoint only if it's ours.
    pub async fn shutdown(&self) {
        if let Some(router) = &self.router {
            let _ = router.shutdown().await;
        }
        if let Some(gossip) = &self.gossip {
            let _ = gossip.shutdown().await;
        }
    }
}
