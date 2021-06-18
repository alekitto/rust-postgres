use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use postgres_protocol::message::frontend;
use postgres_protocol::Oid;
use std::sync::{Arc, Weak};

struct StatementInner {
    client: Weak<InnerClient>,
    name: String,
    param_types: Vec<Oid>,
}

impl Drop for StatementInner {
    fn drop(&mut self) {
        if let Some(client) = self.client.upgrade() {
            if client.raw_buf(|buf| buf.is_empty()) {
                let buf = client.with_buf(|buf| {
                    frontend::close(b'S', &self.name, buf).unwrap();
                    frontend::sync(buf);
                    buf.split().freeze()
                });

                let _ = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)));
            } else {
                client.raw_buf(|buf| {
                    frontend::close(b'S', &self.name, buf).unwrap();
                });
            }
        }
    }
}

/// A prepared statement.
///
/// Prepared statements can only be used with the connection that created them.
#[derive(Clone)]
pub struct Statement(Arc<StatementInner>);

impl Statement {
    pub(crate) fn new(client: &Arc<InnerClient>, name: String, param_types: Vec<Oid>) -> Statement {
        Statement(Arc::new(StatementInner {
            client: Arc::downgrade(client),
            name,
            param_types,
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
}
