pub mod aggregate;
#[cfg(feature = "alter-table")]
pub mod alter_table;
pub mod arithmetic;
pub mod basic;
pub mod blend;
pub mod create_table;
pub mod default;
pub mod drop_table;
pub mod error;
pub mod filter;
pub mod function;
pub mod join;
pub mod migrate;
pub mod nested_select;
pub mod nullable;
pub mod ordering;
pub mod sql_types;
pub mod synthesize;
pub mod validate;

mod tester;

pub mod macros;

pub use tester::*;

#[cfg(feature = "alter-table")]
#[macro_export]
macro_rules! generate_alter_table_tests {
    () => {
        glue!(alter_table_rename, alter_table::rename);
        glue!(alter_table_add_drop, alter_table::add_drop);
    };
}

#[cfg(not(feature = "alter-table"))]
#[macro_export]
macro_rules! generate_alter_table_tests {
    () => {};
}

#[macro_export]
macro_rules! generate_tests {
    ($test: meta, $storage: ident) => {
        macro_rules! glue {
            ($title: ident, $func: path) => {
                #[$test]
                async fn $title() {
                    let path = stringify!($title);
                    let storage = $storage::new(path);

                    $func(storage).await;
                }
            };
        }

        glue!(basic, basic::basic);
        glue!(aggregate, aggregate::aggregate);
        glue!(aggregate_group_by, aggregate::group_by);
        glue!(arithmetic, arithmetic::arithmetic);
        glue!(arithmetic_blend, arithmetic::blend);
        glue!(blend, blend::blend);
        glue!(create_table, create_table::create_table);
        glue!(default, default::default);
        glue!(drop_table, drop_table::drop_table);
        glue!(error, error::error);
        glue!(filter, filter::filter);
        glue!(function, function::function);
        glue!(join, join::join);
        glue!(join_blend, join::blend);
        glue!(migrate, migrate::migrate);
        glue!(nested_select, nested_select::nested_select);
        glue!(nullable, nullable::nullable);
        glue!(nullable_text, nullable::nullable_text);
        glue!(ordering, ordering::ordering);
        glue!(sql_types, sql_types::sql_types);
        glue!(synthesize, synthesize::synthesize);
        glue!(validate_unique, validate::unique::unique);
        glue!(validate_types, validate::types::types);

        generate_alter_table_tests!();
    };
}
