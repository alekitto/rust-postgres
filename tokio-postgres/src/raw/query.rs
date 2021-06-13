use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::{Client, Error};
use bytes::Bytes;
use log::debug;
use postgres_protocol::message::backend::Message;
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

    /// Returns the expected types of the statement's parameters.
    pub fn params(&self) -> &[Oid] {
        &self.0.params
    }
}

/// Creates a new prepared statement.
///
/// Prepared statements can be executed repeatedly, and may contain query parameters (indicated by `$1`, `$2`, etc),
/// which are set when executed. Prepared statements can only be used with the connection that created them.
pub async fn prepare<E>(
    client: &Client,
    query: &str,
    name: &str,
    types_oid: &[Oid],
) -> Result<Statement, Error> {
    internal_prepare(client.inner(), query, name, types_oid).await
}

pub async fn internal_prepare(
    client: &Arc<InnerClient>,
    query: &str,
    name: &str,
    types_oid: &[Oid],
) -> Result<Statement, Error> {
    let buf = encode(client, name, query, types_oid)?;
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    match responses.next().await? {
        Message::ParseComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(Statement::new(
        &client,
        name.to_string(),
        types_oid.to_vec(),
    ))
}

fn encode(
    client: &InnerClient,
    name: &str,
    query: &str,
    types_oid: &[Oid],
) -> Result<Bytes, Error> {
    debug!(
        "preparing query {} with types {:?}: {}",
        name, types_oid, query
    );

    client.with_buf(|buf| {
        frontend::parse(name, query, types_oid.iter().copied(), buf).map_err(Error::encode)?;
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })
}
