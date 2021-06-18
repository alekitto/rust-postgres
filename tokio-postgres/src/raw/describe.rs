use crate::{Client, Error};
use postgres_protocol::message::frontend;

/// Enumerate the targets of a describe command
#[derive(Debug)]
pub enum DescribeTarget {
    /// Describe a statement (parameters + rows)
    Statement(String),
    /// Describe a portal (rows only)
    Portal(String),
}

/// Issue a describe command. Will be fired on next sync.
pub fn describe<E>(client: &Client, what: DescribeTarget) -> Result<(), E>
where
    E: std::convert::From<crate::error::Error>,
{
    let inner = client.inner();
    inner.raw_buf(|buf| {
        match what {
            DescribeTarget::Statement(name) => {
                frontend::describe(b'S', &name, buf).map_err(Error::encode)?
            }
            DescribeTarget::Portal(name) => {
                frontend::describe(b'P', &name, buf).map_err(Error::encode)?
            }
        };

        Ok(())
    })?;

    Ok(())
}
