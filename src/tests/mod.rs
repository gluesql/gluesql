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
pub mod limit;
pub mod migrate;
pub mod nested_select;
pub mod nullable;
pub mod order_by;
pub mod ordering;
pub mod synthesize;
pub mod transaction;
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
        glue!(limit, limit::limit);
        glue!(error, error::error);
        glue!(filter, filter::filter);
        glue!(function_upper_lower, function::upper_lower::upper_lower);
        glue!(function_left_right, function::left_right::left_right);
        glue!(function_trim, function::trim::trim);
        glue!(function_div_mod, function::div_mod::div_mod);
        glue!(function_cast_literal, function::cast::cast_literal);
        glue!(function_cast_value, function::cast::cast_value);
        glue!(function_ceil, function::ceil::ceil);
        glue!(function_round, function::round::round);
        glue!(function_floor, function::floor::floor);
        glue!(function_ln, function::exp_log::ln);
        glue!(function_log2, function::exp_log::log2);
        glue!(function_log10, function::exp_log::log10);
        glue!(function_exp, function::exp_log::exp);
        glue!(join, join::join);
        glue!(join_blend, join::blend);
        glue!(migrate, migrate::migrate);
        glue!(nested_select, nested_select::nested_select);
        glue!(nullable, nullable::nullable);
        glue!(nullable_text, nullable::nullable_text);
        glue!(ordering, ordering::ordering);
        glue!(order_by, order_by::order_by);
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
                glue!(index_nested, index::nested);
                glue!(index_null, index::null);
                glue!(index_expr, index::expr);
                glue!(index_value, index::value);
                glue!(index_order_by, index::order_by);
                #[cfg(feature = "sorter")]
                glue!(index_order_by_multi, index::order_by_multi);
            };
        }
        #[cfg(feature = "index")]
        glue_index!();

        #[cfg(feature = "alter-table")]
        macro_rules! glue_alter_table {
            () => {
                glue!(alter_table_rename, alter::alter_table_rename);
                glue!(alter_table_add_drop, alter::alter_table_add_drop);
            };
        }
        #[cfg(feature = "alter-table")]
        glue_alter_table!();

        #[cfg(all(feature = "alter-table", feature = "index"))]
        macro_rules! glue_alter_table_index {
            () => {
                glue!(alter_table_drop_indexed_table, alter::drop_indexed_table);
                glue!(alter_table_drop_indexed_column, alter::drop_indexed_column);
            };
        }
        #[cfg(all(feature = "alter-table", feature = "index"))]
        glue_alter_table_index!();

        #[cfg(feature = "transaction")]
        macro_rules! glue_transaction {
            () => {
                glue!(transaction_basic, transaction::basic);
                glue!(
                    transaction_create_drop_table,
                    transaction::create_drop_table
                );
            };
        }
        #[cfg(feature = "transaction")]
        glue_transaction!();

        #[cfg(all(feature = "transaction", feature = "alter-table"))]
        macro_rules! glue_transaction_alter_table {
            () => {
                glue!(
                    transaction_alter_table_rename_column,
                    transaction::alter_table_rename_column
                );
                glue!(
                    transaction_alter_table_add_column,
                    transaction::alter_table_add_column
                );
                glue!(
                    transaction_alter_table_drop_column,
                    transaction::alter_table_drop_column
                );
            };
        }
        #[cfg(all(feature = "transaction", feature = "alter-table"))]
        glue_transaction_alter_table!();

        #[cfg(all(feature = "transaction", feature = "index"))]
        macro_rules! glue_transaction_index {
            () => {
                glue!(transaction_index_create, transaction::index_create);
                glue!(transaction_index_drop, transaction::index_drop);
            };
        }
        #[cfg(all(feature = "transaction", feature = "index"))]
        glue_transaction_index!();
    };
}
