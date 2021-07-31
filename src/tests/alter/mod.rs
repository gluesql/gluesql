mod alter_table;
mod create_table;
mod drop_indexed;
mod drop_table;

#[cfg(feature = "alter-table")]
pub use alter_table::{alter_table_add_drop, alter_table_rename};
pub use create_table::create_table;
#[cfg(all(feature = "alter-table", feature = "index"))]
pub use drop_indexed::{drop_indexed_column, drop_indexed_table};
pub use drop_table::drop_table;
