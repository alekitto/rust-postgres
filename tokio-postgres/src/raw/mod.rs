//! Raw (low-level) interface.

#[cfg(feature = "raw")]
pub(crate) mod portal;
#[cfg(feature = "raw")]
pub(crate) mod query;
pub(crate) mod simple_query;
#[cfg(feature = "raw")]
pub(crate) mod statement;

#[cfg(feature = "raw")]
pub use portal::Portal;
#[cfg(feature = "raw")]
pub use query::{bind, execute, prepare, sync, QueryStream, Row};
#[cfg(feature = "raw")]
pub use simple_query::{simple_query, SimpleColumn, SimpleQueryRow, SimpleQueryStream};
#[cfg(feature = "raw")]
pub use statement::Statement;
