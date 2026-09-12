use crate::error::NodeError;
use iroh::endpoint::{presets, Connection};
use iroh::{Endpoint, EndpointAddr};

pub const ALPN: &[u8] = b"chorrent/0.1";

pub struct ChorrentNode {
    endpoint: Endpoint,
    // endpoint variable: of type Endpoint
}

// The BEHAVIOR: what a ChorrentNode can do
impl ChorrentNode {
    pub async fn bind() -> Result<Self, NodeError> {
        let endpoint = Endpoint::builder(presets::N0)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .map_err(|e| NodeError::Bind { message: e.to_string() })?;

        Ok(Self { endpoint })
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

    pub async fn accept(&self) -> Result<Connection, NodeError> {
        let incoming = self
            .endpoint
            .accept()
            .await
            .ok_or_else(|| NodeError::Accept { message: "endpoint closed".into() })?;

        incoming
            .await
            .map_err(|e| NodeError::Accept { message: e.to_string() })
    }
}