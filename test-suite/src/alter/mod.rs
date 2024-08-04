mod alter_table;
mod create_table;
mod drop_indexed;
mod drop_table;

pub use {
    alter_table::{alter_table_add_drop, alter_table_rename},
    create_table::create_table,
    drop_indexed::{drop_indexed_column, drop_indexed_table},
    drop_table::drop_table,
};
