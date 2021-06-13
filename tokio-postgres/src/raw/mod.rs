//! Raw (low-level) interface.

#[cfg(feature = "raw")]
pub(crate) mod query;
pub(crate) mod simple_query;

#[cfg(feature = "raw")]
pub use query::{prepare, Statement};
#[cfg(feature = "raw")]
pub use simple_query::{simple_query, SimpleColumn, SimpleQueryRow, SimpleQueryStream};
