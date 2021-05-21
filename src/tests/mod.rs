pub mod aggregate;
#[cfg(feature = "alter-table")]
pub mod alter_table;
pub mod arithmetic;
pub mod basic;
pub mod blend;
pub mod concat;
pub mod create_table;
pub mod data_type;
pub mod default;
pub mod drop_table;
pub mod error;
pub mod filter;
pub mod function;
#[cfg(feature = "index")]
pub mod index;
pub mod join;
pub mod migrate;
pub mod nested_select;
pub mod nullable;
pub mod ordering;
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
        glue!(concat, concat::concat);
        glue!(blend, blend::blend);
        glue!(create_table, create_table::create_table);
        glue!(default, default::default);
        glue!(drop_table, drop_table::drop_table);
        glue!(error, error::error);
        glue!(filter, filter::filter);
        glue!(function_upper_lower, function::upper_lower::upper_lower);
        glue!(function_left_right, function::left_right::left_right);
        glue!(function_cast_literal, function::cast::cast_literal);
        glue!(function_cast_value, function::cast::cast_value);
        glue!(join, join::join);
        glue!(join_blend, join::blend);
        glue!(migrate, migrate::migrate);
        glue!(nested_select, nested_select::nested_select);
        glue!(nullable, nullable::nullable);
        glue!(nullable_text, nullable::nullable_text);
        glue!(ordering, ordering::ordering);
        glue!(sql_types, data_type::sql_types::sql_types);
        glue!(date, data_type::date::date);
        glue!(timestamp, data_type::timestamp::timestamp);
        glue!(time, data_type::time::time);
        glue!(interval, data_type::interval::interval);
        glue!(synthesize, synthesize::synthesize);
        glue!(validate_unique, validate::unique::unique);
        glue!(validate_types, validate::types::types);

        #[cfg(feature = "index")]
        glue!(index, index::index);

        generate_alter_table_tests!();
    };
}
