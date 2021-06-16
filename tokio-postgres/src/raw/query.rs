use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::raw::portal::Portal;
use crate::raw::statement::Statement;
use crate::{Client, Error};
use bytes::{BufMut, BytesMut};
use log::debug;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use postgres_protocol::Oid;
use std::sync::Arc;

/// Creates a new prepared statement.
///
/// Prepared statements can be executed repeatedly, and may contain query parameters (indicated by `$1`, `$2`, etc),
/// which are set when executed. Prepared statements can only be used with the connection that created them.
pub async fn prepare<E>(
    client: &Client,
    query: &str,
    name: &str,
    types_oid: &[Oid],
) -> Result<Statement, E>
where
    E: std::convert::From<crate::error::Error>,
{
    Ok(internal_prepare(client.inner(), query, name, types_oid).await?)
}

pub async fn internal_prepare(
    client: &Arc<InnerClient>,
    query: &str,
    name: &str,
    types_oid: &[Oid],
) -> Result<Statement, Error> {
    debug!(
        "preparing query {} with types {:?}: {}",
        name, types_oid, query
    );

    let buf = client.with_buf(|buf| {
        frontend::parse(name, query, types_oid.iter().copied(), buf).map_err(Error::encode)?;
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })?;
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

/// Binds some parameters to a prepared statement, thus creating a portal
/// Portals could be then executed or dropped when no more needed.
pub async fn bind<'a, I, E>(
    client: &Client,
    statement: &Statement,
    name: &str,
    params: I,
) -> Result<Portal, E>
where
    I: IntoIterator<Item = &'a Option<BytesMut>>,
    I::IntoIter: ExactSizeIterator,
    E: std::convert::From<crate::error::Error>,
{
    let inner = client.inner();
    let buf = inner.with_buf(|buf| {
        encode_bind(&statement, params, &name, buf)?;
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })?;

    let mut responses = inner.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;
    match responses.next().await? {
        Message::BindComplete => {}
        _ => return Err(E::from(Error::unexpected_message())),
    }

    Ok(Portal::new(inner, name))
}

pub fn encode_bind<'a, I>(
    statement: &Statement,
    params: I,
    portal: &str,
    buf: &mut BytesMut,
) -> Result<(), Error>
where
    I: IntoIterator<Item = &'a Option<BytesMut>>,
    I::IntoIter: ExactSizeIterator,
{
    let params = params.into_iter();
    let r = frontend::bind(
        portal,
        statement.name(),
        Some(1),
        params,
        |param, buf| match param {
            Some(bytes) => {
                buf.put(bytes.clone());
                Ok(postgres_protocol::IsNull::No)
            }
            None => Ok(postgres_protocol::IsNull::Yes),
        },
        Some(1),
        buf,
    );

    match r {
        Ok(()) => Ok(()),
        Err(frontend::BindError::Serialization(e)) => Err(Error::encode(e)),
        Err(_) => Err(Error::unexpected_message()),
    }
}
