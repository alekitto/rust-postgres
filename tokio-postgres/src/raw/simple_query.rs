use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
#[cfg(feature = "raw")]
use crate::types::Type;
#[cfg(feature = "raw")]
use crate::Client;
use crate::Error;
use bytes::Bytes;
#[cfg(feature = "raw")]
use fallible_iterator::FallibleIterator;
#[cfg(feature = "raw")]
use futures::{ready, Stream};
use log::debug;
#[cfg(feature = "raw")]
use pin_project_lite::pin_project;
#[cfg(feature = "raw")]
use postgres_protocol::message::backend::{DataRowBody, Message, RowDescriptionBody};
use postgres_protocol::message::frontend;
#[cfg(feature = "raw")]
use postgres_types::FromSql;
#[cfg(feature = "raw")]
use std::marker::PhantomPinned;
#[cfg(feature = "raw")]
use std::ops::Range;
#[cfg(feature = "raw")]
use std::pin::Pin;
#[cfg(feature = "raw")]
use std::sync::Arc;
#[cfg(feature = "raw")]
use std::task::{Context, Poll};

/// Executes a query via the simple query protocol and return the response stream.
#[cfg(feature = "raw")]
pub fn simple_query(client: &Client, query: &str) -> Result<SimpleQueryStream, Error> {
    let inner = client.inner().as_ref();
    let responses = internal_simple_query(inner, query)?;

    Ok(SimpleQueryStream {
        responses,
        _p: PhantomPinned,
    })
}

pub(crate) fn internal_simple_query(client: &InnerClient, query: &str) -> Result<Responses, Error> {
    debug!("executing simple query: {}", query);

    let buf = encode(client, query)?;
    client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))
}

pub(crate) fn encode(client: &InnerClient, query: &str) -> Result<Bytes, Error> {
    client.with_buf(|buf| {
        frontend::query(query, buf).map_err(Error::encode)?;
        Ok(buf.split().freeze())
    })
}

#[cfg(feature = "raw")]
pin_project! {
    /// A stream of simple query results.
    pub struct SimpleQueryStream {
        responses: Responses,
        #[pin]
        _p: PhantomPinned,
    }
}

#[cfg(feature = "raw")]
impl Stream for SimpleQueryStream {
    type Item = Result<Message, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let message = ready!(this.responses.poll_next(cx));
        match message {
            Ok(Message::CommandComplete(_))
            | Ok(Message::RowDescription(_))
            | Ok(Message::DataRow(_))
            | Ok(Message::ReadyForQuery(_))
            | Ok(Message::EmptyQueryResponse) => Poll::Ready(Some(message)),
            Err(e) => {
                if e.is_closed() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Err(Error::unexpected_message())))
                }
            }
            _ => Poll::Ready(Some(Err(Error::unexpected_message()))),
        }
    }
}

/// A row of data returned from the database by a simple query.
///
/// This struct can be used while processing a DataRow message to get the row data
/// in a more convenient way.
///
/// Compared to the standard SimpleQueryRow, this has a simpler structure,
/// no column data (meaning you can't get data by column name) and only a try_get method.
#[cfg(feature = "raw")]
pub struct SimpleQueryRow {
    body: DataRowBody,
    ranges: Vec<Option<Range<usize>>>,
}

#[cfg(feature = "raw")]
impl SimpleQueryRow {
    /// Create a new row from a simple query data row body
    #[allow(clippy::new_ret_no_self)]
    pub fn new(body: DataRowBody) -> Result<SimpleQueryRow, Error> {
        let ranges = body.ranges().collect().map_err(Error::parse)?;
        Ok(SimpleQueryRow { body, ranges })
    }

    /// Determines if the row contains no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of values in the row.
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    /// Returns a value from the row.
    /// The value can be specified only by its numeric index in the row.
    pub fn try_get(&self, idx: usize) -> Result<Option<&str>, Error> {
        let buf = self.ranges[idx].clone().map(|r| &self.body.buffer()[r]);
        FromSql::from_sql_nullable(&Type::TEXT, buf).map_err(|e| Error::from_sql(e, idx))
    }
}

/// Information about a column of a single query row.
#[cfg(feature = "raw")]
pub struct SimpleColumn {
    name: String,
    type_: Option<Type>,
}

#[cfg(feature = "raw")]
impl SimpleColumn {
    pub(crate) fn new(name: String, type_: Option<Type>) -> SimpleColumn {
        SimpleColumn { name, type_ }
    }

    /// Returns the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the type of the column.
    pub fn type_(&self) -> &Option<Type> {
        &self.type_
    }

    /// Converts a row description body into a set of simple columns
    pub fn from_row_description_body(body: RowDescriptionBody) -> Result<Arc<[Self]>, Error> {
        Ok(body
            .fields()
            .map(|f| {
                Ok(SimpleColumn::new(
                    f.name().to_string(),
                    Type::from_oid(f.type_oid()),
                ))
            })
            .collect::<Vec<_>>()
            .map_err(Error::parse)?
            .into())
    }
}
