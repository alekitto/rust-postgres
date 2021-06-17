use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::Error;
use bytes::{Bytes, BytesMut};
use postgres_protocol::Oid;
use std::sync::{Arc, Weak};
use tokio::sync::RwLock;

struct StatementInner {
    client: Weak<InnerClient>,
    name: String,
    param_types: Vec<Oid>,
    buf: RwLock<BytesMut>,
}

/// A prepared statement.
///
/// Prepared statements can only be used with the connection that created them.
#[derive(Clone)]
pub struct Statement(Arc<StatementInner>);

impl Statement {
    pub(crate) fn new(inner: &Arc<InnerClient>, name: String, param_types: Vec<Oid>) -> Statement {
        Statement(Arc::new(StatementInner {
            client: Arc::downgrade(inner),
            name,
            param_types,
            buf: RwLock::new(BytesMut::new()),
        }))
    }

    /// Gets the name of the current statement.
    pub fn name(&self) -> &str {
        &self.0.name
    }

    /// Returns the expected types of the statement's parameters.
    pub fn param_types(&self) -> &[Oid] {
        &self.0.param_types
    }

    pub(crate) async fn append_buf(&self, bytes: Bytes) {
        let mut buf = self.0.buf.write().await;
        buf.extend(bytes);
    }

    pub(crate) async fn send(&self) -> Result<Responses, Error> {
        let mut buf = self.0.buf.write().await;
        let bytes = buf.clone().freeze();

        let client = self.0.client.upgrade().ok_or_else(Error::closed)?;
        let result = client.send(RequestMessages::Single(FrontendMessage::Raw(bytes)));
        buf.clear();

        result
    }
}
