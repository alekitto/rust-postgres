use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use postgres_protocol::message::frontend;
use postgres_protocol::Oid;
use std::sync::{Arc, Weak};

struct StatementInner {
    client: Weak<InnerClient>,
    name: String,
    params: Vec<Oid>,
}

impl Drop for StatementInner {
    fn drop(&mut self) {
        if let Some(client) = self.client.upgrade() {
            let buf = client.with_buf(|buf| {
                frontend::close(b'S', &self.name, buf).unwrap();
                frontend::sync(buf);
                buf.split().freeze()
            });
            let _ = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)));
        }
    }
}

/// A prepared statement.
///
/// Prepared statements can only be used with the connection that created them.
#[derive(Clone)]
pub struct Statement(Arc<StatementInner>);

impl Statement {
    pub(crate) fn new(inner: &Arc<InnerClient>, name: String, params: Vec<Oid>) -> Statement {
        Statement(Arc::new(StatementInner {
            client: Arc::downgrade(inner),
            name,
            params,
        }))
    }

    pub(crate) fn name(&self) -> &str {
        &self.0.name
    }

    /// Returns the expected types of the statement's parameters.
    pub fn params(&self) -> &[Oid] {
        &self.0.params
    }
}
