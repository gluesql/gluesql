mod alter_table;
mod error;
mod index;
mod table;
mod validate;

use validate::validate;

#[cfg(feature = "alter-table")]
pub use alter_table::alter_table;
pub use error::AlterError;
#[cfg(feature = "index")]
pub use index::{create_index, drop_index};
pub use table::{create_table, drop_table};
