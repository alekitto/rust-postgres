use postgres_protocol::Oid;
use std::sync::Arc;

struct StatementInner {
    name: String,
    param_types: Vec<Oid>,
}

/// A prepared statement.
///
/// Prepared statements can only be used with the connection that created them.
#[derive(Clone)]
pub struct Statement(Arc<StatementInner>);

impl Statement {
    pub(crate) fn new(name: String, param_types: Vec<Oid>) -> Statement {
        Statement(Arc::new(StatementInner { name, param_types }))
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
