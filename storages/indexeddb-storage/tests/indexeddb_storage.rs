#![allow(clippy::future_not_send)]

use gluesql_core::prelude::Glue;
use serde::ser::Serialize;
use serde_wasm_bindgen::Serializer;
use test_suite::*;
use test_suite::{generate_store_tests, Tester};
use wasm_bindgen_test::{console_log, wasm_bindgen_test, wasm_bindgen_test_configure};

use glueseql_indexeddb_storage::IndexeddbStorage;

wasm_bindgen_test_configure!(run_in_browser);

struct IndexeddbTester {
    glue: Glue<IndexeddbStorage>,
}

impl Tester<IndexeddbStorage> for IndexeddbTester {
    fn new(_: &str) -> Self {
        panic!("oh no")
    }

    fn get_glue(&mut self) -> &mut Glue<IndexeddbStorage> {
        &mut self.glue
    }
}

impl IndexeddbTester {
    async fn new(namespace: &str) -> Self {
        let factory = idb::Factory::new().unwrap();
        factory.delete(namespace).await.ok();

        let storage = IndexeddbStorage::new(namespace).await.unwrap();

        let glue = Glue::new(storage);

        IndexeddbTester { glue }
    }
}

macro_rules! declare_test_fn {
    ($test: meta, $storage: ident, $title: ident, $func: path) => {
        #[wasm_bindgen_test]
        async fn $title() {
            let path = stringify!($title);
            let storage = $storage::new(path).await;

            $func(storage).await;
        }
    };
}

macro_rules! generate_store_tests {
    ($test: meta, $storage: ident) => {
        macro_rules! glue {
            ($title: ident, $func: path) => {
                declare_test_fn!($test, $storage, $title, $func);
            };
        }
        glue!(update, update::update);
        glue!(insert, insert::insert);
        glue!(basic, basic::basic);
        glue!(aggregate_avg, aggregate::avg::avg);
        glue!(aggregate_count, aggregate::count::count);
        glue!(aggregate_group_by, aggregate::group_by::group_by);
        glue!(aggregate_max, aggregate::max::max);
        glue!(aggregate_min, aggregate::min::min);
        glue!(aggregate_stdev, aggregate::stdev::stdev);
        glue!(aggregate_sum, aggregate::sum::sum);
        glue!(aggregate_variance, aggregate::variance::variance);
        glue!(aggregate_error, aggregate::error::error);
        glue!(aggregate_error_group_by, aggregate::error::error_group_by);
        glue!(arithmetic_error, arithmetic::error::error);
        glue!(arithmetic_blend, arithmetic::blend::blend);
        glue!(arithmetic_on_where, arithmetic::on_where::on_where);
        glue!(concat, concat::concat);
        glue!(blend, blend::blend);
        glue!(create_table, alter::create_table);
        glue!(drop_table, alter::drop_table);
        glue!(default, default::default);
        glue!(limit, limit::limit);
        glue!(like_ilike, like_ilike::like_ilike);
        glue!(filter, filter::filter);
        glue!(inline_view, inline_view::inline_view);
        glue!(values, values::values);
        glue!(unary_operator, unary_operator::unary_operator);
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
        glue!(function_concat, function::concat::concat);
        glue!(function_concat_ws, function::concat_ws::concat_ws);
        glue!(function_ifnull, function::ifnull::ifnull);
        glue!(function_math_function_asin, function::math_function::asin);
        glue!(function_math_function_acos, function::math_function::acos);
        glue!(function_math_function_atan, function::math_function::atan);
        glue!(function_math_function_sin, function::math_function::sin);
        glue!(function_math_function_cos, function::math_function::cos);
        glue!(function_math_function_tan, function::math_function::tan);
        glue!(function_abs, function::abs::abs);
        glue!(function_ceil, function::ceil::ceil);
        glue!(function_round, function::round::round);
        glue!(function_floor, function::floor::floor);
        glue!(function_format, function::format::format);
        glue!(function_ln, function::exp_log::ln);
        glue!(function_log, function::exp_log::log);
        glue!(function_log2, function::exp_log::log2);
        glue!(function_log10, function::exp_log::log10);
        glue!(function_exp, function::exp_log::exp);
        glue!(function_now, function::now::now);
        glue!(function_sign, function::sign::sign);
        glue!(function_to_date, function::to_date::to_date);
        glue!(function_ascii, function::ascii::ascii);
        glue!(function_chr, function::chr::chr);
        glue!(function_position, function::position::position);
        glue!(join, join::join);
        glue!(join_blend, join::blend);
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
        glue!(uint16, data_type::uint16::uint16);
        glue!(uint8, data_type::uint8::uint8);
        glue!(date, data_type::date::date);
        glue!(timestamp, data_type::timestamp::timestamp);
        glue!(time, data_type::time::time);
        glue!(interval, data_type::interval::interval);
        glue!(list, data_type::list::list);
        glue!(map, data_type::map::map);
        glue!(bytea, data_type::bytea::bytea);
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
        glue!(type_match, type_match::type_match);
        // glue!(dictionary, dictionary::dictionary);

        // ast-builder
        glue!(ast_builder_basic, ast_builder::basic::basic);
        glue!(ast_builder_select, ast_builder::select::select);
        glue!(ast_builder_insert, ast_builder::insert::insert);
        glue!(ast_builder_update, ast_builder::update::update);
        glue!(ast_builder_delete, ast_builder::delete::delete);
    };
}

generate_store_tests!(tokio::test, IndexeddbTester);

#[wasm_bindgen_test]
async fn first_test() {
    // use futures::executor::block_on;
    use gluesql_core::prelude::Glue;

    let serializer = Serializer::new().serialize_large_number_types_as_bigints(true);

    let x = 9223372036854775807_i64;
    let x = x.serialize(&serializer);
    console_log!("Result: {:?}", x);

    let storage = IndexeddbStorage::new("test").await.unwrap();

    let mut glue = Glue::new(storage);

    let sqls = vec![
        "DROP TABLE IF EXISTS Glue;",
        "CREATE TABLE Glue (id INTEGER);",
        "INSERT INTO Glue VALUES (100);",
        "INSERT INTO Glue VALUES (200);",
        "SELECT * FROM Glue WHERE id > 10;",
    ];

    // let sqls = vec![
    //     "DROP TABLE IF EXISTS Glue;",
    //     "CREATE TABLE Glue (id INTEGER);",
    //     // "INSERT INTO Glue VALUES (100);",
    //     "DELETE FROM Glue;",
    // ];

    for sql in sqls {
        let output = glue.execute_async(sql).await.unwrap();
        console_log!("{:?}", output);
    }
}

// #[wasm_bindgen_test]
// fn memory_storage_transaction() {
//     use gluesql_core::{prelude::Glue, result::Error};

//     let storage = IndexeddbStorage::default();
//     let mut glue = Glue::new(storage);

//     exec!(glue "CREATE TABLE TxTest (id INTEGER);");
//     test!(glue "BEGIN", Err(Error::StorageMsg("[IndexeddbStorage] transaction is not supported".to_owned())));
//     test!(glue "COMMIT", Err(Error::StorageMsg("[IndexeddbStorage] transaction is not supported".to_owned())));
//     test!(glue "ROLLBACK", Err(Error::StorageMsg("[IndexeddbStorage] transaction is not supported".to_owned())));
// }
