pub mod aggregate;
pub mod alter;
pub mod arithmetic;
pub mod basic;
pub mod blend;
pub mod case;
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
macro_rules! declare_test_fn {
    ($test: meta, $storage: ident, $title: ident, $func: path) => {
        #[$test]
        async fn $title() {
            let path = stringify!($title);
            let storage = $storage::new(path);

            $func(storage).await;
        }
    };
}

#[macro_export]
macro_rules! generate_store_tests {
    ($test: meta, $storage: ident) => {
        macro_rules! glue {
            ($title: ident, $func: path) => {
                declare_test_fn!($test, $storage, $title, $func);
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
        glue!(function_gcd_lcm, function::gcd_lcm::gcd_lcm);
        glue!(function_left_right, function::left_right::left_right);
        glue!(function_sqrt, function::sqrt_power::sqrt);
        glue!(function_power, function::sqrt_power::power);
        glue!(function_lpad_rpad, function::lpad_rpad::lpad_rpad);
        glue!(function_trim, function::trim::trim);
        glue!(function_div_mod, function::div_mod::div_mod);
        glue!(function_ltrim_rtrim, function::ltrim_rtrim::ltrim_rtrim);
        glue!(function_cast_literal, function::cast::cast_literal);
        glue!(function_cast_value, function::cast::cast_value);
        glue!(function_math_function_asin, function::math_function::asin);
        glue!(function_math_function_acos, function::math_function::acos);
        glue!(function_math_function_atan, function::math_function::atan);
        glue!(function_math_function_sin, function::math_function::sin);
        glue!(function_math_function_cos, function::math_function::cos);
        glue!(function_math_function_tan, function::math_function::tan);
        glue!(function_ceil, function::ceil::ceil);
        glue!(function_round, function::round::round);
        glue!(function_floor, function::floor::floor);
        glue!(function_ln, function::exp_log::ln);
        glue!(function_log, function::exp_log::log);
        glue!(function_log2, function::exp_log::log2);
        glue!(function_log10, function::exp_log::log10);
        glue!(function_exp, function::exp_log::exp);
        glue!(join, join::join);
        glue!(join_blend, join::blend);
        glue!(migrate, migrate::migrate);
        glue!(nested_select, nested_select::nested_select);
        glue!(nullable, nullable::nullable);
        glue!(nullable_text, nullable::nullable_text);
        glue!(nullable_implicit_insert, nullable::nullable_implicit_insert);
        glue!(ordering, ordering::ordering);
        glue!(order_by, order_by::order_by);
        glue!(sql_types, data_type::sql_types::sql_types);
        glue!(date, data_type::date::date);
        glue!(timestamp, data_type::timestamp::timestamp);
        glue!(time, data_type::time::time);
        glue!(interval, data_type::interval::interval);
        glue!(list, data_type::list::list);
        glue!(map, data_type::map::map);
        glue!(synthesize, synthesize::synthesize);
        glue!(validate_unique, validate::unique::unique);
        glue!(validate_types, validate::types::types);
        glue!(function_radians, function::radians::radians);
        glue!(function_degrees, function::degrees::degrees);
        glue!(function_pi, function::pi::pi);
        glue!(function_reverse, function::reverse::reverse);
        glue!(case, case::case);
        glue!(function_substr, function::substr::substr);
        glue!(uuid, data_type::uuid::uuid);
    };
}

#[cfg(feature = "alter-table")]
#[macro_export]
macro_rules! generate_alter_table_tests {
    ($test: meta, $storage: ident) => {
        macro_rules! glue {
            ($title: ident, $func: path) => {
                declare_test_fn!($test, $storage, $title, $func);
            };
        }

        glue!(alter_table_rename, alter::alter_table_rename);
        glue!(alter_table_add_drop, alter::alter_table_add_drop);
    };
}

#[cfg(feature = "index")]
#[macro_export]
macro_rules! generate_index_tests {
    ($test: meta, $storage: ident) => {
        macro_rules! glue {
            ($title: ident, $func: path) => {
                declare_test_fn!($test, $storage, $title, $func);
            };
        }

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

#[cfg(feature = "transaction")]
#[macro_export]
macro_rules! generate_transaction_tests {
    ($test: meta, $storage: ident) => {
        macro_rules! glue {
            ($title: ident, $func: path) => {
                declare_test_fn!($test, $storage, $title, $func);
            };
        }

        glue!(transaction_basic, transaction::basic);
        glue!(
            transaction_create_drop_table,
            transaction::create_drop_table
        );
    };
}

#[cfg(all(feature = "alter-table", feature = "index"))]
#[macro_export]
macro_rules! generate_alter_table_index_tests {
    ($test: meta, $storage: ident) => {
        macro_rules! glue {
            ($title: ident, $func: path) => {
                declare_test_fn!($test, $storage, $title, $func);
            };
        }

        glue!(alter_table_drop_indexed_table, alter::drop_indexed_table);
        glue!(alter_table_drop_indexed_column, alter::drop_indexed_column);
    };
}

#[cfg(all(feature = "transaction", feature = "alter-table"))]
#[macro_export]
macro_rules! generate_transaction_alter_table_tests {
    ($test: meta, $storage: ident) => {
        macro_rules! glue {
            ($title: ident, $func: path) => {
                declare_test_fn!($test, $storage, $title, $func);
            };
        }

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

#[cfg(all(feature = "transaction", feature = "index"))]
#[macro_export]
macro_rules! generate_transaction_index_tests {
    ($test: meta, $storage: ident) => {
        macro_rules! glue {
            ($title: ident, $func: path) => {
                declare_test_fn!($test, $storage, $title, $func);
            };
        }

        glue!(transaction_index_create, transaction::index_create);
        glue!(transaction_index_drop, transaction::index_drop);
    };
}
