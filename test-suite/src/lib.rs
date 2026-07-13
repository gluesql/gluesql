#![deny(clippy::str_to_string)]

pub mod aggregate;
pub mod alter;
pub mod array;
pub mod basic;
pub mod column_alias;
pub mod custom_function;
pub mod data_type;
pub mod default;
pub mod delete;
pub mod dictionary;
pub mod dictionary_index;
pub mod distinct;
pub mod expr;
pub mod filter;
pub mod fixture;
pub mod foreign_key;
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
pub mod query_builder;
pub mod schemaless;
pub mod series;
pub mod show_columns;
pub mod store;
pub mod synthesize;
pub mod transaction;
pub mod type_match;
pub mod update;
pub mod validate;
pub mod values;

pub mod tester;

#[doc(hidden)]
pub use paste::paste;
pub use tester::*;

#[macro_export]
macro_rules! declare_test_fn {
    ($test: meta, $storage: ident, $title: ident, $func: path) => {
        #[$test]
        fn $title() {
            let path = stringify!($title);
            let storage = $storage::new(path);

            $func(storage);
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! declare_rust_case {
    ($test: meta, $storage: ident, $case: ident) => {
        $crate::declare_test_fn!($test, $storage, $case, $case::$case);
    };
    ($test: meta, $storage: ident, $module: ident::$case: ident) => {
        $crate::paste! {
            $crate::declare_test_fn!(
                $test,
                $storage,
                [<$module _ $case>],
                $module::$case::$case
            );
        }
    };
    ($test: meta, $storage: ident, $module1: ident::$module2: ident::$case: ident) => {
        $crate::paste! {
            $crate::declare_test_fn!(
                $test,
                $storage,
                [<$module1 _ $module2 _ $case>],
                $module1::$module2::$case::$case
            );
        }
    };
    ($test: meta, $storage: ident, $module1: ident::$module2: ident::$module3: ident::$case: ident) => {
        $crate::paste! {
            $crate::declare_test_fn!(
                $test,
                $storage,
                [<$module1 _ $module2 _ $module3 _ $case>],
                $module1::$module2::$module3::$case::$case
            );
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! declare_sql_test_fn {
    ($test: meta, $storage: ident, $title: ident, $fixture_path: expr) => {
        #[$test]
        fn $title() {
            let path = stringify!($title);
            let storage = $storage::new(path);
            let source = $crate::fixture::source($fixture_path);

            $crate::fixture::run_fixture(storage, $fixture_path, source);
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! declare_sql_case {
    ($test: meta, $storage: ident, $case: ident) => {
        $crate::paste! {
            $crate::declare_sql_test_fn!(
                $test,
                $storage,
                [<sql_ $case>],
                concat!(stringify!($case), ".sql")
            );
        }
    };
    ($test: meta, $storage: ident, $module: ident::$case: ident) => {
        $crate::paste! {
            $crate::declare_sql_test_fn!(
                $test,
                $storage,
                [<sql_ $module _ $case>],
                concat!(stringify!($module), "/", stringify!($case), ".sql")
            );
        }
    };
    ($test: meta, $storage: ident, $module1: ident::$module2: ident::$case: ident) => {
        $crate::paste! {
            $crate::declare_sql_test_fn!(
                $test,
                $storage,
                [<sql_ $module1 _ $module2 _ $case>],
                concat!(
                    stringify!($module1),
                    "/",
                    stringify!($module2),
                    "/",
                    stringify!($case),
                    ".sql"
                )
            );
        }
    };
    ($test: meta, $storage: ident, $module1: ident::$module2: ident::$module3: ident::$case: ident) => {
        $crate::paste! {
            $crate::declare_sql_test_fn!(
                $test,
                $storage,
                [<sql_ $module1 _ $module2 _ $module3 _ $case>],
                concat!(
                    stringify!($module1),
                    "/",
                    stringify!($module2),
                    "/",
                    stringify!($module3),
                    "/",
                    stringify!($case),
                    ".sql"
                )
            );
        }
    };
}

#[macro_export]
macro_rules! generate_store_tests {
    ($test: meta, $storage: ident) => {
        $crate::generate_store_tests!(@rust_case $test, $storage, $);
    };
    (@rust_case $test: meta, $storage: ident, $d: tt) => {
        macro_rules! rust_case {
            ($d($d module:ident)::+) => {
                $crate::declare_rust_case!($test, $storage, $d($d module)::+);
            };
        }
        macro_rules! sql_case {
            ($d($d module:ident)::+) => {
                $crate::declare_sql_case!($test, $storage, $d($d module)::+);
            };
        }
        rust_case!(update);
        sql_case!(update);
        rust_case!(insert);
        sql_case!(insert);
        rust_case!(delete);
        sql_case!(delete);
        rust_case!(basic);
        sql_case!(basic);
        rust_case!(array);
        sql_case!(array);
        rust_case!(aggregate::avg);
        sql_case!(aggregate::avg);
        rust_case!(aggregate::count);
        sql_case!(aggregate::count);
        rust_case!(aggregate::group_by);
        sql_case!(aggregate::group_by);
        rust_case!(aggregate::max);
        sql_case!(aggregate::max);
        rust_case!(aggregate::min);
        sql_case!(aggregate::min);
        rust_case!(aggregate::stdev);
        sql_case!(aggregate::stdev);
        rust_case!(aggregate::sum);
        sql_case!(aggregate::sum);
        rust_case!(aggregate::variance);
        sql_case!(aggregate::variance);
        rust_case!(aggregate::error);
        sql_case!(aggregate::error);
        rust_case!(aggregate::expr);
        sql_case!(aggregate::expr);
        rust_case!(project);
        sql_case!(project);

        // expression tests
        rust_case!(expr::arithmetic::error);
        sql_case!(expr::arithmetic::error);
        rust_case!(expr::arithmetic::project);
        sql_case!(expr::arithmetic::project);
        rust_case!(expr::arithmetic::on_where);
        sql_case!(expr::arithmetic::on_where);
        rust_case!(expr::bitwise_and);
        sql_case!(expr::bitwise_and);
        rust_case!(expr::bitwise_shift_left);
        sql_case!(expr::bitwise_shift_left);
        rust_case!(expr::bitwise_shift_right);
        sql_case!(expr::bitwise_shift_right);
        rust_case!(expr::case);
        sql_case!(expr::case);
        rust_case!(expr::concat);
        sql_case!(expr::concat);
        rust_case!(expr::between);
        sql_case!(expr::between);
        rust_case!(expr::in_list);
        sql_case!(expr::in_list);
        rust_case!(expr::arrow);
        sql_case!(expr::arrow);
        rust_case!(expr::unary_operator);
        sql_case!(expr::unary_operator);

        rust_case!(alter::create_table);
        sql_case!(alter::create_table);
        rust_case!(alter::drop_table);
        sql_case!(alter::drop_table);
        rust_case!(default);
        sql_case!(default);
        rust_case!(limit);
        sql_case!(limit);
        rust_case!(like_ilike);
        sql_case!(like_ilike);
        rust_case!(filter);
        sql_case!(filter);
        rust_case!(inline_view);
        sql_case!(inline_view);
        rust_case!(values);
        sql_case!(values);
        rust_case!(function::upper_lower);
        sql_case!(function::upper_lower);
        rust_case!(function::initcap);
        sql_case!(function::initcap);
        rust_case!(function::gcd_lcm);
        sql_case!(function::gcd_lcm);
        rust_case!(function::left_right);
        sql_case!(function::left_right);
        rust_case!(function::sqrt_power::sqrt);
        sql_case!(function::sqrt_power::sqrt);
        rust_case!(function::sqrt_power::power);
        sql_case!(function::sqrt_power::power);
        rust_case!(function::lpad_rpad);
        sql_case!(function::lpad_rpad);
        rust_case!(function::trim);
        sql_case!(function::trim);
        rust_case!(function::div_mod);
        sql_case!(function::div_mod);
        rust_case!(function::ltrim_rtrim);
        sql_case!(function::ltrim_rtrim);
        rust_case!(function::cast::literal);
        sql_case!(function::cast::literal);
        rust_case!(function::cast::value);
        sql_case!(function::cast::value);
        rust_case!(function::coalesce);
        sql_case!(function::coalesce);
        rust_case!(function::concat);
        sql_case!(function::concat);
        rust_case!(function::concat_ws);
        sql_case!(function::concat_ws);
        rust_case!(function::ifnull);
        sql_case!(function::ifnull);
        rust_case!(function::is_empty);
        sql_case!(function::is_empty);
        rust_case!(function::math_function::asin);
        sql_case!(function::math_function::asin);
        rust_case!(function::math_function::acos);
        sql_case!(function::math_function::acos);
        rust_case!(function::math_function::atan);
        sql_case!(function::math_function::atan);
        rust_case!(function::math_function::sin);
        sql_case!(function::math_function::sin);
        rust_case!(function::math_function::cos);
        sql_case!(function::math_function::cos);
        rust_case!(function::math_function::tan);
        sql_case!(function::math_function::tan);
        rust_case!(function::abs);
        sql_case!(function::abs);
        rust_case!(function::ceil);
        sql_case!(function::ceil);
        rust_case!(function::round);
        sql_case!(function::round);
        rust_case!(function::trunc);
        sql_case!(function::trunc);
        rust_case!(function::rand);
        sql_case!(function::rand);
        rust_case!(function::floor);
        sql_case!(function::floor);
        rust_case!(function::format);
        sql_case!(function::format);
        rust_case!(function::last_day);
        sql_case!(function::last_day);
        rust_case!(function::exp_log::ln);
        sql_case!(function::exp_log::ln);
        rust_case!(function::exp_log::log);
        sql_case!(function::exp_log::log);
        rust_case!(function::exp_log::log2);
        sql_case!(function::exp_log::log2);
        rust_case!(function::exp_log::log10);
        sql_case!(function::exp_log::log10);
        rust_case!(function::exp_log::exp);
        sql_case!(function::exp_log::exp);
        rust_case!(function::now);
        sql_case!(function::now);
        rust_case!(function::current_date);
        sql_case!(function::current_date);
        rust_case!(function::current_time);
        sql_case!(function::current_time);
        rust_case!(function::current_timestamp);
        sql_case!(function::current_timestamp);
        rust_case!(function::sign);
        sql_case!(function::sign);
        rust_case!(function::skip);
        sql_case!(function::skip);
        rust_case!(function::to_date);
        sql_case!(function::to_date);
        rust_case!(function::ascii);
        sql_case!(function::ascii);
        rust_case!(function::chr);
        sql_case!(function::chr);
        rust_case!(function::md5);
        sql_case!(function::md5);
        rust_case!(function::replace);
        sql_case!(function::replace);
        rust_case!(function::length);
        sql_case!(function::length);
        rust_case!(function::position);
        sql_case!(function::position);
        rust_case!(function::find_idx);
        sql_case!(function::find_idx);
        rust_case!(function::geometry::get_x);
        sql_case!(function::geometry::get_x);
        rust_case!(function::geometry::get_y);
        sql_case!(function::geometry::get_y);
        rust_case!(function::geometry::calc_distance);
        sql_case!(function::geometry::calc_distance);
        rust_case!(function::add_month);
        sql_case!(function::add_month);
        rust_case!(function::slice);
        sql_case!(function::slice);
        rust_case!(function::entries);
        sql_case!(function::entries);
        rust_case!(function::keys);
        sql_case!(function::keys);
        rust_case!(function::values);
        sql_case!(function::values);
        rust_case!(function::nullif);
        sql_case!(function::nullif);
        rust_case!(function::hex);
        sql_case!(function::hex);
        rust_case!(join);
        sql_case!(join);
        rust_case!(join::project);
        sql_case!(join::project);
        rust_case!(migrate);
        sql_case!(migrate);
        rust_case!(nested_select);
        sql_case!(nested_select);
        rust_case!(primary_key);
        sql_case!(primary_key);
        rust_case!(foreign_key);
        sql_case!(foreign_key);
        rust_case!(series);
        sql_case!(series);
        rust_case!(nullable);
        sql_case!(nullable);
        rust_case!(nullable::text);
        sql_case!(nullable::text);
        rust_case!(nullable::implicit_insert);
        sql_case!(nullable::implicit_insert);
        rust_case!(ordering);
        sql_case!(ordering);
        rust_case!(order_by);
        sql_case!(order_by);
        rust_case!(data_type::sql_types);
        sql_case!(data_type::sql_types);
        rust_case!(show_columns);
        sql_case!(show_columns);
        rust_case!(distinct);
        sql_case!(distinct);
        rust_case!(data_type::int8);
        sql_case!(data_type::int8);
        rust_case!(data_type::int16);
        sql_case!(data_type::int16);
        rust_case!(data_type::int32);
        sql_case!(data_type::int32);
        rust_case!(data_type::int64);
        sql_case!(data_type::int64);
        rust_case!(data_type::int128);
        sql_case!(data_type::int128);
        rust_case!(data_type::float32);
        sql_case!(data_type::float32);
        rust_case!(data_type::uint16);
        sql_case!(data_type::uint16);
        rust_case!(data_type::uint8);
        sql_case!(data_type::uint8);
        rust_case!(data_type::uint64);
        sql_case!(data_type::uint64);
        rust_case!(data_type::uint32);
        sql_case!(data_type::uint32);
        rust_case!(data_type::uint128);
        sql_case!(data_type::uint128);
        rust_case!(data_type::date);
        sql_case!(data_type::date);
        rust_case!(data_type::timestamp);
        sql_case!(data_type::timestamp);
        rust_case!(data_type::time);
        sql_case!(data_type::time);
        rust_case!(data_type::interval);
        sql_case!(data_type::interval);
        rust_case!(data_type::list);
        sql_case!(data_type::list);
        rust_case!(data_type::map);
        sql_case!(data_type::map);
        rust_case!(data_type::bytea);
        sql_case!(data_type::bytea);
        rust_case!(data_type::inet);
        sql_case!(data_type::inet);
        rust_case!(data_type::point);
        sql_case!(data_type::point);
        rust_case!(data_type::null);
        sql_case!(data_type::null);
        rust_case!(synthesize);
        sql_case!(synthesize);
        rust_case!(validate::unique);
        sql_case!(validate::unique);
        rust_case!(validate::types);
        sql_case!(validate::types);
        rust_case!(function::extract);
        sql_case!(function::extract);
        rust_case!(function::radians);
        sql_case!(function::radians);
        rust_case!(function::degrees);
        sql_case!(function::degrees);
        rust_case!(function::pi);
        sql_case!(function::pi);
        rust_case!(function::reverse);
        sql_case!(function::reverse);
        rust_case!(function::repeat);
        sql_case!(function::repeat);
        rust_case!(function::substr);
        sql_case!(function::substr);
        rust_case!(data_type::uuid);
        sql_case!(data_type::uuid);
        rust_case!(data_type::decimal);
        sql_case!(data_type::decimal);
        rust_case!(function::generate_uuid);
        sql_case!(function::generate_uuid);
        rust_case!(function::greatest);
        sql_case!(function::greatest);
        rust_case!(type_match);
        sql_case!(type_match);
        rust_case!(dictionary);
        sql_case!(dictionary);
        rust_case!(function::append);
        sql_case!(function::append);
        rust_case!(function::prepend);
        sql_case!(function::prepend);
        rust_case!(function::sort);
        sql_case!(function::sort);
        rust_case!(function::take);
        sql_case!(function::take);
        rust_case!(column_alias);
        sql_case!(column_alias);
        rust_case!(function::splice);
        sql_case!(function::splice);
        rust_case!(function::dedup);
        sql_case!(function::dedup);

        // query-builder
        rust_case!(query_builder::basic);
        rust_case!(query_builder::statements::querying::data_aggregation);
        rust_case!(query_builder::statements::querying::data_selection_and_projection);
        rust_case!(query_builder::function::math::rounding);
        rust_case!(query_builder::expr::pattern_matching);
        rust_case!(query_builder::select);
        rust_case!(query_builder::values);
        rust_case!(query_builder::insert);
        rust_case!(query_builder::update);
        rust_case!(query_builder::delete);
        rust_case!(query_builder::alias_as);
        rust_case!(query_builder::function::text::case_conversion);
        rust_case!(query_builder::function::text::character_conversion);
        rust_case!(query_builder::function::text::padding);
        rust_case!(query_builder::function::reference::coalesce);
        rust_case!(query_builder::function::reference::ifnull);
        rust_case!(query_builder::function::reference::nullif);
        rust_case!(query_builder::function::datetime::conversion);
        rust_case!(query_builder::function::math::basic_arithmetic);
        rust_case!(query_builder::function::math::conversion);
        rust_case!(query_builder::function::datetime::formatting);
        rust_case!(query_builder::function::text::trimming);
        rust_case!(query_builder::function::datetime::current_date_and_time);
        rust_case!(query_builder::function::reference::current_date);
        rust_case!(query_builder::function::reference::current_time);
        rust_case!(query_builder::function::reference::current_timestamp);
        rust_case!(query_builder::function::reference::generate_uuid);
        rust_case!(query_builder::function::text::position_and_indexing);
        rust_case!(query_builder::index_by);
        rust_case!(query_builder::schemaless::basic);

        // schemaless data support
        rust_case!(schemaless::basic);
        sql_case!(schemaless::basic);
        rust_case!(schemaless::error);
        sql_case!(schemaless::error);
        rust_case!(schemaless::project);
        sql_case!(schemaless::project);

        rust_case!(store::insert_schema);
    };
}

#[macro_export]
macro_rules! generate_alter_table_tests {
    ($test: meta, $storage: ident) => {
        $crate::generate_alter_table_tests!(@rust_case $test, $storage, $);
    };
    (@rust_case $test: meta, $storage: ident, $d: tt) => {
        macro_rules! rust_case {
            ($d($d module:ident)::+) => {
                $crate::declare_rust_case!($test, $storage, $d($d module)::+);
            };
        }
        macro_rules! sql_case {
            ($d($d module:ident)::+) => {
                $crate::declare_sql_case!($test, $storage, $d($d module)::+);
            };
        }

        rust_case!(alter::alter_table::rename);
        sql_case!(alter::alter_table::rename);
        rust_case!(alter::alter_table::add_drop);
        sql_case!(alter::alter_table::add_drop);
    };
}

#[macro_export]
macro_rules! generate_custom_function_tests {
    ($test: meta, $storage: ident) => {
        $crate::generate_custom_function_tests!(@rust_case $test, $storage, $);
    };
    (@rust_case $test: meta, $storage: ident, $d: tt) => {
        macro_rules! rust_case {
            ($d($d module:ident)::+) => {
                $crate::declare_rust_case!($test, $storage, $d($d module)::+);
            };
        }
        macro_rules! sql_case {
            ($d($d module:ident)::+) => {
                $crate::declare_sql_case!($test, $storage, $d($d module)::+);
            };
        }

        rust_case!(custom_function);
        sql_case!(custom_function);
    };
}

#[macro_export]
macro_rules! generate_index_tests {
    ($test: meta, $storage: ident) => {
        $crate::generate_index_tests!(@rust_case $test, $storage, $);
    };
    (@rust_case $test: meta, $storage: ident, $d: tt) => {
        macro_rules! rust_case {
            ($d($d module:ident)::+) => {
                $crate::declare_rust_case!($test, $storage, $d($d module)::+);
            };
        }

        rust_case!(index::basic);
        rust_case!(index::and);
        rust_case!(index::nested);
        rust_case!(index::null);
        rust_case!(index::expr);
        rust_case!(index::value);
        rust_case!(index::order_by);
        rust_case!(index::order_by::multi);
        rust_case!(index::showindexes);
        rust_case!(dictionary_index);
    };
}

#[macro_export]
macro_rules! generate_transaction_tests {
    ($test: meta, $storage: ident) => {
        $crate::generate_transaction_tests!(@rust_case $test, $storage, $);
    };
    (@rust_case $test: meta, $storage: ident, $d: tt) => {
        macro_rules! rust_case {
            ($d($d module:ident)::+) => {
                $crate::declare_rust_case!($test, $storage, $d($d module)::+);
            };
        }
        macro_rules! sql_case {
            ($d($d module:ident)::+) => {
                $crate::declare_sql_case!($test, $storage, $d($d module)::+);
            };
        }

        rust_case!(transaction::basic);
        sql_case!(transaction::basic);
        rust_case!(transaction::table);
        sql_case!(transaction::table);
        rust_case!(transaction::dictionary);
        sql_case!(transaction::dictionary);
        rust_case!(transaction::query_builder);
    };
}

#[macro_export]
macro_rules! generate_alter_table_index_tests {
    ($test: meta, $storage: ident) => {
        $crate::generate_alter_table_index_tests!(@rust_case $test, $storage, $);
    };
    (@rust_case $test: meta, $storage: ident, $d: tt) => {
        macro_rules! rust_case {
            ($d($d module:ident)::+) => {
                $crate::declare_rust_case!($test, $storage, $d($d module)::+);
            };
        }

        rust_case!(alter::drop_indexed::table);
        rust_case!(alter::drop_indexed::column);
    };
}

#[macro_export]
macro_rules! generate_transaction_alter_table_tests {
    ($test: meta, $storage: ident) => {
        $crate::generate_transaction_alter_table_tests!(@rust_case $test, $storage, $);
    };
    (@rust_case $test: meta, $storage: ident, $d: tt) => {
        macro_rules! rust_case {
            ($d($d module:ident)::+) => {
                $crate::declare_rust_case!($test, $storage, $d($d module)::+);
            };
        }
        macro_rules! sql_case {
            ($d($d module:ident)::+) => {
                $crate::declare_sql_case!($test, $storage, $d($d module)::+);
            };
        }

        rust_case!(transaction::alter_table::rename_table);
        sql_case!(transaction::alter_table::rename_table);
        rust_case!(transaction::alter_table::rename_column);
        sql_case!(transaction::alter_table::rename_column);
        rust_case!(transaction::alter_table::add_column);
        sql_case!(transaction::alter_table::add_column);
        rust_case!(transaction::alter_table::drop_column);
        sql_case!(transaction::alter_table::drop_column);
    };
}

#[macro_export]
macro_rules! generate_transaction_index_tests {
    ($test: meta, $storage: ident) => {
        $crate::generate_transaction_index_tests!(@rust_case $test, $storage, $);
    };
    (@rust_case $test: meta, $storage: ident, $d: tt) => {
        macro_rules! rust_case {
            ($d($d module:ident)::+) => {
                $crate::declare_rust_case!($test, $storage, $d($d module)::+);
            };
        }

        rust_case!(transaction::index::create);
        rust_case!(transaction::index::drop);
    };
}

#[macro_export]
macro_rules! generate_metadata_table_tests {
    ($test: meta, $storage: ident) => {
        $crate::generate_metadata_table_tests!(@rust_case $test, $storage, $);
    };
    (@rust_case $test: meta, $storage: ident, $d: tt) => {
        macro_rules! rust_case {
            ($d($d module:ident)::+) => {
                $crate::declare_rust_case!($test, $storage, $d($d module)::+);
            };
        }
        macro_rules! sql_case {
            ($d($d module:ident)::+) => {
                $crate::declare_sql_case!($test, $storage, $d($d module)::+);
            };
        }

        rust_case!(metadata::table);
        sql_case!(metadata::table);
    };
}

#[macro_export]
macro_rules! generate_metadata_index_tests {
    ($test: meta, $storage: ident) => {
        $crate::generate_metadata_index_tests!(@rust_case $test, $storage, $);
    };
    (@rust_case $test: meta, $storage: ident, $d: tt) => {
        macro_rules! rust_case {
            ($d($d module:ident)::+) => {
                $crate::declare_rust_case!($test, $storage, $d($d module)::+);
            };
        }

        rust_case!(metadata::index);
    };
}
