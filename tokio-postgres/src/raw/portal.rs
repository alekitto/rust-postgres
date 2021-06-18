use crate::client::InnerClient;
use std::sync::{Arc, Weak};

struct Inner {
    _client: Weak<InnerClient>,
    name: String,
}

/// A portal.
///
/// Portals can only be used with the connection that created them, and only exist for the duration of the transaction
/// in which they were created.
#[derive(Clone)]
pub struct Portal(Arc<Inner>);

impl Portal {
    pub(crate) fn new<S: ToString>(client: &Arc<InnerClient>, name: S) -> Portal {
        Portal(Arc::new(Inner {
            _client: Arc::downgrade(client),
            name: name.to_string(),
        }))
    }

    /// Gets the name of the current portal.
    pub fn name(&self) -> &str {
        &self.0.name
    }
}
