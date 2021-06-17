use crate::client::{InnerClient, Responses};
use crate::raw::portal::Portal;
use crate::raw::statement::Statement;
use crate::{Client, Error};
use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use futures::task::{Context, Poll};
use futures::{ready, Stream};
use log::debug;
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::{DataRowBody, Message};
use postgres_protocol::message::frontend;
use postgres_protocol::Oid;
use std::fmt;
use std::marker::{PhantomData, PhantomPinned};
use std::ops::Range;
use std::pin::Pin;
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
        Ok(buf.split().freeze())
    })?;

    let stmt = Statement::new(&client, name.to_string(), types_oid.to_vec());

    stmt.append_buf(buf).await;
    Ok(stmt)
}

/// Binds some parameters to a prepared statement, thus creating a portal
/// Portals could be then executed or dropped when no more needed.
pub async fn bind<'a, I, E>(
    client: &Client,
    statement: Statement,
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
        Ok(buf.split().freeze())
    })?;

    statement.append_buf(buf).await;
    Ok(Portal::new(inner, statement, name))
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

pin_project! {
    /// A stream of table rows.
    pub struct QueryStream<E> {
        responses: Responses,
        #[pin]
        _p: PhantomPinned,
        _e: PhantomData<E>
    }
}

impl<E> Stream for QueryStream<E>
where
    E: std::convert::From<crate::error::Error>,
{
    type Item = Result<Message, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let message = ready!(this.responses.poll_next(cx));
        match message {
            Ok(Message::DataRow(_))
            | Ok(Message::ParseComplete)
            | Ok(Message::BindComplete)
            | Ok(Message::EmptyQueryResponse)
            | Ok(Message::CommandComplete(_))
            | Ok(Message::PortalSuspended)
            | Ok(Message::ReadyForQuery(_))
            | Ok(Message::ErrorResponse(_)) => Poll::Ready(Some(Ok(message.unwrap()))),
            Err(e) => {
                if e.is_closed() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Err(e.into())))
                }
            }
            _ => Poll::Ready(Some(Err(Error::unexpected_message().into()))),
        }
    }
}

/// Executes a bound statement (portal).
/// "max_rows" could be set to 0 to not apply any limit to the query.
pub async fn execute<E>(
    client: &Client,
    portal: &Portal,
    max_rows: i32,
) -> Result<QueryStream<E>, E>
where
    E: std::convert::From<crate::error::Error>,
{
    let inner = client.inner();
    let buf = inner.with_buf(|buf| {
        frontend::execute(portal.name(), max_rows, buf).map_err(Error::encode)?;
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })?;

    let statement = portal.statement();
    statement.append_buf(buf).await;
    let responses = statement.send().await?;

    Ok(QueryStream {
        responses,
        _p: PhantomPinned,
        _e: PhantomData::default(),
    })
}

/// A row of data returned from the database by a query.
pub struct Row {
    body: DataRowBody,
    ranges: Vec<Option<Range<usize>>>,
}

impl fmt::Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Row").finish()
    }
}

impl Row {
    /// Creates a new row object from the raw data body.
    pub fn new(body: DataRowBody) -> Result<Row, Error> {
        let ranges = body.ranges().collect().map_err(Error::parse)?;
        Ok(Row { body, ranges })
    }

    /// Determines if the row contains no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of values in the row.
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    /// Get the raw bytes for the column at the given index.
    pub fn get(&self, idx: usize) -> Option<&[u8]> {
        let range = self.ranges[idx].to_owned()?;
        Some(&self.body.buffer()[range])
    }
}
