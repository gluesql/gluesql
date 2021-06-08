pub mod aggregate;
pub mod alter;
pub mod arithmetic;
pub mod basic;
pub mod blend;
pub mod concat;
pub mod data_type;
pub mod default;
pub mod error;
pub mod filter;
pub mod function;
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
        glue!(create_table, alter::create_table);
        glue!(drop_table, alter::drop_table);
        glue!(default, default::default);
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
        macro_rules! glue_index {
            () => {
                glue!(index_basic, index::basic);
                glue!(index_and, index::and);
                glue!(index_null, index::null);
                glue!(index_expr, index::expr);
                glue!(index_value, index::value);
            };
        }

        #[cfg(feature = "alter-table")]
        macro_rules! glue_alter_table {
            () => {
                glue!(alter_table_rename, alter::alter_table_rename);
                glue!(alter_table_add_drop, alter::alter_table_add_drop);
            };
        }

        #[cfg(all(feature = "alter-table", feature = "index"))]
        glue!(alter_table_drop_indexed_column, alter::drop_indexed_column);

        #[cfg(feature = "index")]
        glue_index!();
        #[cfg(feature = "alter-table")]
        glue_alter_table!();
    };
}
