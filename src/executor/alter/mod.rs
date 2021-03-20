#[cfg(feature = "alter-table")]
mod alter_table;
mod create_table;
mod drop;
mod error;

#[cfg(feature = "alter-table")]
pub use alter_table::alter_table;
pub use create_table::create_table;
pub use drop::drop;
pub use error::AlterError;
