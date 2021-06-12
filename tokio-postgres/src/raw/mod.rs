//! Raw (low-level) interface.

pub(crate) mod simple_query;

#[cfg(feature = "raw")]
pub use simple_query::{simple_query, SimpleColumn, SimpleQueryRow};
