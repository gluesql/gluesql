#![deny(clippy::str_to_string)]

pub mod aggregate;
pub mod alter;
pub mod arithmetic;
pub mod ast_builder;
pub mod basic;
pub mod bitwise_and;
pub mod bitwise_shift;
pub mod case;
pub mod column_alias;
pub mod concat;
pub mod custom_function;
pub mod data_type;
pub mod default;
pub mod delete;
pub mod dictionary;
pub mod dictionary_index;
pub mod filter;
pub mod function;
pub mod index;
pub mod inline_view;
pub mod insert;
pub mod join;
pub mod like_ilike;
pub mod limit;
pub mod metadata;
pub mod migrate;
pub mod nested_select;
pub mod nullable;
pub mod order_by;
pub mod ordering;
pub mod primary_key;
pub mod project;
pub mod schemaless;
pub mod series;
pub mod show_columns;
pub mod synthesize;
pub mod transaction;
pub mod type_match;
pub mod unary_operator;
pub mod update;
pub mod validate;
pub mod values;

pub mod tester;

pub use tester::*;

#[macro_export]
macro_rules! declare_test_fn {
    ($test: meta, $storage: ident, $title: ident, $func: path) => {
        #[$test]
        async fn $title() {
            let path = stringify!($title);
            let storage = $storage::new(path).await;

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
        glue!(update, update::update);
        glue!(insert, insert::insert);
        glue!(delete, delete::delete);
        glue!(basic, basic::basic);
        glue!(bitwise_and, bitwise_and::bitwise_and);
        glue!(aggregate_avg, aggregate::avg::avg);
        glue!(aggregate_count, aggregate::count::count);
        glue!(aggregate_group_by, aggregate::group_by::group_by);
        glue!(aggregate_max, aggregate::max::max);
        glue!(aggregate_min, aggregate::min::min);
        glue!(aggregate_stdev, aggregate::stdev::stdev);
        glue!(aggregate_sum, aggregate::sum::sum);
        glue!(aggregate_variance, aggregate::variance::variance);
        glue!(aggregate_error, aggregate::error::error);
        glue!(arithmetic_error, arithmetic::error::error);
        glue!(arithmetic_project, arithmetic::project::project);
        glue!(arithmetic_on_where, arithmetic::on_where::on_where);
        glue!(concat, concat::concat);
        glue!(project, project::project);
        glue!(bitwise_shift, bitwise_shift::bitwise_shift_left);
        glue!(create_table, alter::create_table);
        glue!(drop_table, alter::drop_table);
        glue!(default, default::default);
        glue!(limit, limit::limit);
        glue!(like_ilike, like_ilike::like_ilike);
        glue!(filter, filter::filter);
        glue!(inline_view, inline_view::inline_view);
        glue!(values, values::values);
        glue!(unary_operator, unary_operator::unary_operator);
        glue!(function_values, function::values::values);
        glue!(function_upper_lower, function::upper_lower::upper_lower);
        glue!(function_initcap, function::initcap::initcap);
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
        glue!(function_coalesce, function::coalesce::coalesce);
        glue!(function_concat, function::concat::concat);
        glue!(function_concat_ws, function::concat_ws::concat_ws);
        glue!(function_ifnull, function::ifnull::ifnull);
        glue!(function_is_empty, function::is_empty::is_empty);
        glue!(function_math_function_asin, function::math_function::asin);
        glue!(function_math_function_acos, function::math_function::acos);
        glue!(function_math_function_atan, function::math_function::atan);
        glue!(function_math_function_sin, function::math_function::sin);
        glue!(function_math_function_cos, function::math_function::cos);
        glue!(function_math_function_tan, function::math_function::tan);
        glue!(function_abs, function::abs::abs);
        glue!(function_ceil, function::ceil::ceil);
        glue!(function_round, function::round::round);
        glue!(function_rand, function::rand::rand);
        glue!(function_floor, function::floor::floor);
        glue!(function_format, function::format::format);
        glue!(function_last_day, function::last_day::last_day);
        glue!(function_ln, function::exp_log::ln);
        glue!(function_log, function::exp_log::log);
        glue!(function_log2, function::exp_log::log2);
        glue!(function_log10, function::exp_log::log10);
        glue!(function_exp, function::exp_log::exp);
        glue!(function_now, function::now::now);
        glue!(function_sign, function::sign::sign);
        glue!(function_skip, function::skip::skip);
        glue!(function_to_date, function::to_date::to_date);
        glue!(function_ascii, function::ascii::ascii);
        glue!(function_chr, function::chr::chr);
        glue!(function_mod, function::md5::md5);
        glue!(function_replace, function::replace::replace);
        glue!(function_length, function::length::length);
        glue!(function_position, function::position::position);
        glue!(function_find_idx, function::find_idx::find_idx);
        glue!(function_geometry_get_x, function::geometry::get_x);
        glue!(function_geometry_get_y, function::geometry::get_y);
        glue!(
            function_geometry_calc_distance,
            function::geometry::calc_distance
        );
        glue!(function_add_month, function::add_month::add_month);
        glue!(function_slice, function::slice::slice);
        glue!(function_entries, function::entries::entries);
        glue!(join, join::join);
        glue!(join_project, join::project);
        glue!(migrate, migrate::migrate);
        glue!(nested_select, nested_select::nested_select);
        glue!(primary_key, primary_key::primary_key);
        glue!(series, series::series);
        glue!(nullable, nullable::nullable);
        glue!(nullable_text, nullable::nullable_text);
        glue!(nullable_implicit_insert, nullable::nullable_implicit_insert);
        glue!(ordering, ordering::ordering);
        glue!(order_by, order_by::order_by);
        glue!(sql_types, data_type::sql_types::sql_types);
        glue!(show_columns, show_columns::show_columns);
        glue!(int8, data_type::int8::int8);
        glue!(int16, data_type::int16::int16);
        glue!(int32, data_type::int32::int32);
        glue!(int64, data_type::int64::int64);
        glue!(int128, data_type::int128::int128);
        glue!(float32, data_type::float32::float32);
        glue!(uint16, data_type::uint16::uint16);
        glue!(uint8, data_type::uint8::uint8);
        glue!(uint64, data_type::uint64::uint64);
        glue!(uint32, data_type::uint32::uint32);
        glue!(uint128, data_type::uint128::uint128);
        glue!(date, data_type::date::date);
        glue!(timestamp, data_type::timestamp::timestamp);
        glue!(time, data_type::time::time);
        glue!(interval, data_type::interval::interval);
        glue!(list, data_type::list::list);
        glue!(map, data_type::map::map);
        glue!(bytea, data_type::bytea::bytea);
        glue!(inet, data_type::inet::inet);
        glue!(point, data_type::point::point);
        glue!(synthesize, synthesize::synthesize);
        glue!(validate_unique, validate::unique::unique);
        glue!(validate_types, validate::types::types);
        glue!(function_extract, function::extract::extract);
        glue!(function_radians, function::radians::radians);
        glue!(function_degrees, function::degrees::degrees);
        glue!(function_pi, function::pi::pi);
        glue!(function_reverse, function::reverse::reverse);
        glue!(function_repeat, function::repeat::repeat);
        glue!(case, case::case);
        glue!(function_substr, function::substr::substr);
        glue!(uuid, data_type::uuid::uuid);
        glue!(decimal, data_type::decimal::decimal);
        glue!(
            function_generate_uuid,
            function::generate_uuid::generate_uuid
        );
        glue!(function_greatest, function::greatest::greatest);
        glue!(type_match, type_match::type_match);
        glue!(dictionary, dictionary::dictionary);
        glue!(function_append, function::append::append);
        glue!(function_prepend, function::prepend::prepend);
        glue!(function_sort, function::sort::sort);
        glue!(function_take, function::take::take);
        glue!(column_alias, column_alias::column_alias);
        glue!(function_splice, function::splice::splice);

        // ast-builder
        glue!(ast_builder_basic, ast_builder::basic::basic);
        glue!(
            ast_builder_statements_queryinng_data_aggregation,
            ast_builder::statements::querying::data_aggregation
        );
        glue!(
            ast_builder_function_math_rounding,
            ast_builder::function::math::rounding
        );
        glue!(
            ast_builder_expr_pattern_matching,
            ast_builder::expr::pattern_matching::pattern_matching
        );
        glue!(ast_builder_select, ast_builder::select::select);
        glue!(ast_builder_values, ast_builder::values::values);
        glue!(ast_builder_insert, ast_builder::insert::insert);
        glue!(ast_builder_update, ast_builder::update::update);
        glue!(ast_builder_delete, ast_builder::delete::delete);
        glue!(ast_builder_alias_as, ast_builder::alias_as::alias_as);
        glue!(
            ast_builder_function_text_case_conversion,
            ast_builder::function::text::case_conversion
        );
        glue!(
            ast_builder_function_other_coalesce,
            ast_builder::function::other::coalesce::coalesce
        );
        glue!(
            ast_builder_function_other_ifnull,
            ast_builder::function::other::ifnull::ifnull
        );
        glue!(
            ast_builder_function_datetime_conversion,
            ast_builder::function::datetime::conversion
        );
        glue!(
            ast_builder_function_math_basic_arithmetic,
            ast_builder::function::math::basic_arithmetic
        );
        glue!(
            ast_builder_function_math_conversion,
            ast_builder::function::math::conversion
        );
        glue!(
            ast_builder_function_datetime_formatting,
            ast_builder::function::datetime::formatting
        );
        glue!(
            ast_builder_function_text_trimming,
            ast_builder::function::text::trimming
        );
        glue!(
            ast_builder_function_datetime_current_date_and_time,
            ast_builder::function::datetime::current_date_and_time
        );
        glue!(
            ast_builder_function_text_position_and_indexing,
            ast_builder::function::text::position_and_indexing
        );

        // schemaless data support
        glue!(schemaless_basic, schemaless::basic);
        glue!(schemaless_error, schemaless::error);
    };
}

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

#[macro_export]
macro_rules! generate_custom_function_tests {
    ($test: meta, $storage: ident) => {
        macro_rules! glue {
            ($title: ident, $func: path) => {
                declare_test_fn!($test, $storage, $title, $func);
            };
        }

        glue!(function_custom, custom_function::custom);
    };
}

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
        glue!(index_order_by_multi, index::order_by_multi);
        glue!(showindexes, index::showindexes);
        glue!(dictionary_index, dictionary_index::ditionary_index);
    };
}

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
        glue!(transaction_dictionary, transaction::dictionary);
    };
}

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

#[macro_export]
macro_rules! generate_transaction_alter_table_tests {
    ($test: meta, $storage: ident) => {
        macro_rules! glue {
            ($title: ident, $func: path) => {
                declare_test_fn!($test, $storage, $title, $func);
            };
        }

        glue!(
            transaction_alter_table_rename_table,
            transaction::alter_table_rename_table
        );
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

#[macro_export]
macro_rules! generate_metadata_table_tests {
    ($test: meta, $storage: ident) => {
        macro_rules! glue {
            ($title: ident, $func: path) => {
                declare_test_fn!($test, $storage, $title, $func);
            };
        }

        glue!(metadata_table, metadata::table::table);
    };
}

#[macro_export]
macro_rules! generate_metadata_index_tests {
    ($test: meta, $storage: ident) => {
        macro_rules! glue {
            ($title: ident, $func: path) => {
                declare_test_fn!($test, $storage, $title, $func);
            };
        }

        glue!(metadata_index, metadata::index::index);
    };
}
