use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::raw::Statement;
use postgres_protocol::message::frontend;
use std::sync::{Arc, Weak};

struct Inner {
    client: Weak<InnerClient>,
    _statement: Statement,
    name: String,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(client) = self.client.upgrade() {
            let buf = client.with_buf(|buf| {
                frontend::close(b'P', &self.name, buf).unwrap();
                frontend::sync(buf);
                buf.split().freeze()
            });
            let _ = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)));
        }
    }
}

/// A portal.
///
/// Portals can only be used with the connection that created them, and only exist for the duration of the transaction
/// in which they were created.
#[derive(Clone)]
pub struct Portal(Arc<Inner>);

impl Portal {
    pub(crate) fn new<S: ToString>(
        client: &Arc<InnerClient>,
        statement: Statement,
        name: S,
    ) -> Portal {
        Portal(Arc::new(Inner {
            client: Arc::downgrade(client),
            _statement: statement,
            name: name.to_string(),
        }))
    }

    /// Gets the name of the current portal.
    pub fn name(&self) -> &str {
        &self.0.name
    }
}
