pub mod aggregate;
pub mod arithmetic;
pub mod blend;
pub mod create_table;
pub mod default;
pub mod drop_table;
pub mod error;
pub mod filter;
pub mod join;
pub mod migrate;
pub mod nested_select;
pub mod nullable;
pub mod ordering;
pub mod sql_types;
pub mod synthesize;

pub mod function;
pub mod generic;
pub mod validate;

#[cfg(feature = "alter-table")]
pub mod alter_table;

pub mod macros;
mod tester;
pub use tester::*;

#[macro_export]
macro_rules! generate_tests {
    ($test: meta, $storage: ident) => {
        generate_tests!($test, $storage,
            aggregate, aggregate::aggregate,
            aggregate_group_by, aggregate::group_by,
            arithmetic, arithmetic::arithmetic,
            arithmetic_blend, arithmetic::blend,
            blend, blend::blend,
            create_table, create_table::create_table,
            default, default::default,
            drop_table, drop_table::drop_table,
            error, error::error,
            filter, filter::filter,
            join, join::join,
            join_blend, join::blend,
            migrate, migrate::migrate,
            nested_select, nested_select::nested_select,
            nullable, nullable::nullable,
            nullable_text, nullable::nullable_text,
            ordering, ordering::ordering,
            sql_types, sql_types::sql_types,
            synthesize, synthesize::synthesize
        );

        #[cfg(feature = "alter-table")]
        generate_tests!($test, $storage, alter_table);

        generate_tests!($test, $storage,
            generic,
            function,
            validate
        );
    };
    ($test: meta, $storage: ident, $($name: ident, $func: expr),+) => {
        $(
            #[$test]
            async fn $name() {
                let path = stringify!($name);
                let storage = $storage::new(path);

                $func(storage).await;
            }
        )+
    };
    ($test: meta, $storage: ident, $($macro: ident),+) => {
        $(
            gluesql::$macro!($test, $storage);
        )+
    };
}
