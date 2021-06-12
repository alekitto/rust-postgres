use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::raw::simple_query::{encode, internal_simple_query};
use crate::query::extract_row_affected;
use crate::{Error, SimpleQueryMessage, SimpleQueryRow};
use fallible_iterator::FallibleIterator;
use futures_util::{ready, Stream};
use log::debug;
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::Message;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Information about a column of a single query row.
#[derive(Debug)]
pub struct SimpleColumn {
    name: String,
}

impl SimpleColumn {
    pub(crate) fn new(name: String) -> SimpleColumn {
        SimpleColumn { name }
    }

    /// Returns the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }
}

pub async fn simple_query(client: &InnerClient, query: &str) -> Result<SimpleQueryStream, Error> {
    Ok(SimpleQueryStream {
        responses: internal_simple_query(client, query)?,
        columns: None,
        _p: PhantomPinned,
    })
}

pub async fn batch_execute(client: &InnerClient, query: &str) -> Result<(), Error> {
    debug!("executing statement batch: {}", query);

    let buf = encode(client, query)?;
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    loop {
        match responses.next().await? {
            Message::ReadyForQuery(_) => return Ok(()),
            Message::CommandComplete(_)
            | Message::EmptyQueryResponse
            | Message::RowDescription(_)
            | Message::DataRow(_) => {}
            _ => return Err(Error::unexpected_message()),
        }
    }
}

pin_project! {
    /// A stream of simple query results.
    pub struct SimpleQueryStream {
        responses: Responses,
        columns: Option<Arc<[SimpleColumn]>>,
        #[pin]
        _p: PhantomPinned,
    }
}

impl Stream for SimpleQueryStream {
    type Item = Result<SimpleQueryMessage, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        loop {
            match ready!(this.responses.poll_next(cx)?) {
                Message::CommandComplete(body) => {
                    let rows = extract_row_affected(&body)?;
                    return Poll::Ready(Some(Ok(SimpleQueryMessage::CommandComplete(rows))));
                }
                Message::EmptyQueryResponse => {
                    return Poll::Ready(Some(Ok(SimpleQueryMessage::CommandComplete(0))));
                }
                Message::RowDescription(body) => {
                    let columns = body
                        .fields()
                        .map(|f| Ok(SimpleColumn::new(f.name().to_string())))
                        .collect::<Vec<_>>()
                        .map_err(Error::parse)?
                        .into();

                    *this.columns = Some(columns);
                }
                Message::DataRow(body) => {
                    let row = match &this.columns {
                        Some(columns) => SimpleQueryRow::new(columns.clone(), body)?,
                        None => return Poll::Ready(Some(Err(Error::unexpected_message()))),
                    };
                    return Poll::Ready(Some(Ok(SimpleQueryMessage::Row(row))));
                }
                Message::ReadyForQuery(_) => return Poll::Ready(None),
                _ => return Poll::Ready(Some(Err(Error::unexpected_message()))),
            }
        }
    }
}
