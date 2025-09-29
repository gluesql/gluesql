use {
    crate::*,
    gluesql_core::{
        error::TranslateError,
        executor::EvaluateError,
        prelude::Value::{self, Bool, F64, I64, Map, Str},
    },
};

test_case!(arrow, {
    let g = get_tester!();

    g.run("CREATE TABLE ArrowSample (object MAP, array LIST);")
        .await;

    g.run(
        r#"
        INSERT INTO ArrowSample VALUES (
            '{"id":1,"b":2,"name":"Han","price":4.25,"active":true,"nested":{"role":"admin"},"1":"first"}',
            '[1,"two",true,4.25,null]'
        );
        "#,
    )
    .await;

    let nested_map = match Value::parse_json_map(r#"{"role":"admin"}"#).unwrap() {
        Value::Map(map) => map,
        _ => unreachable!(),
    };

    g.test(
        "SELECT object->'b' AS result FROM ArrowSample;",
        Ok(select!(result I64; 2)),
    )
    .await;

    g.test(
        "SELECT object->'name' AS result FROM ArrowSample;",
        Ok(select!(result Str; "Han".to_owned())),
    )
    .await;

    g.test(
        "SELECT object->'price' AS result FROM ArrowSample;",
        Ok(select!(result F64; 4.25_f64)),
    )
    .await;

    g.test(
        "SELECT object->'active' AS result FROM ArrowSample;",
        Ok(select!(result Bool; true)),
    )
    .await;

    g.test(
        "SELECT object->'nested' AS result FROM ArrowSample;",
        Ok(select!(result Map; nested_map.clone())),
    )
    .await;

    g.test(
        "SELECT object->1 AS result FROM ArrowSample;",
        Ok(select!(result Str; "first".to_owned())),
    )
    .await;

    g.test(
        "SELECT object->CAST(1 AS INT16) AS result FROM ArrowSample;",
        Ok(select!(result Str; "first".to_owned())),
    )
    .await;

    g.test(
        "SELECT object->'missing' AS result FROM ArrowSample;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    g.test(
        "SELECT object->NULL AS result FROM ArrowSample;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    g.test(
        "SELECT array->0 AS result FROM ArrowSample;",
        Ok(select!(result I64; 1)),
    )
    .await;

    g.test(
        "SELECT array->1 AS result FROM ArrowSample;",
        Ok(select!(result Str; "two".to_owned())),
    )
    .await;

    g.test(
        "SELECT array->2 AS result FROM ArrowSample;",
        Ok(select!(result Bool; true)),
    )
    .await;

    g.test(
        "SELECT array->3 AS result FROM ArrowSample;",
        Ok(select!(result F64; 4.25_f64)),
    )
    .await;

    g.test(
        "SELECT array->4 AS result FROM ArrowSample;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    g.test(
        "SELECT array->'3' AS result FROM ArrowSample;",
        Ok(select!(result F64; 4.25_f64)),
    )
    .await;

    g.test(
        "SELECT array->'foo' AS result FROM ArrowSample;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    g.test(
        "SELECT array->-1 AS result FROM ArrowSample;",
        Err(TranslateError::UnsupportedBinaryOperator("->-".to_owned()).into()),
    )
    .await;

    g.test(
        "SELECT array->(-1) AS result FROM ArrowSample;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    g.test(
        "SELECT array->CAST(-1 AS INT16) AS result FROM ArrowSample;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    g.test(
        "SELECT 1 -> 'foo' AS result;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    g.test(
        "SELECT TRUE -> 'foo' AS result;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    g.test(
        r#"SELECT '{"role":"admin"}'->'role' AS result;"#,
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    g.test(
        "SELECT object->TRUE AS result FROM ArrowSample;",
        Err(EvaluateError::FunctionRequiresIntegerOrStringValue("->".to_owned()).into()),
    )
    .await;

    g.test(
        "SELECT NULL->'role' AS result;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    let map_typed_selectors = [
        ("INT8", "CAST(1 AS INT8)"),
        ("INT16", "CAST(1 AS INT16)"),
        ("INT32", "CAST(1 AS INT32)"),
        ("INT64", "CAST(1 AS INT64)"),
        ("INT128", "CAST(1 AS INT128)"),
        ("UINT8", "CAST(1 AS UINT8)"),
        ("UINT16", "CAST(1 AS UINT16)"),
        ("UINT32", "CAST(1 AS UINT32)"),
        ("UINT64", "CAST(1 AS UINT64)"),
        ("UINT128", "CAST(1 AS UINT128)"),
    ];

    for (label, selector_expr) in map_typed_selectors {
        let sql = format!(
            "SELECT object->{} AS result FROM ArrowSample;",
            selector_expr
        );
        let test_name = format!("Arrow map selector uses {label}");

        g.named_test(
            &test_name,
            sql.as_str(),
            Ok(select!(result Str; "first".to_owned())),
        )
        .await;
    }

    let typed_selectors = [
        ("INT8", "CAST(3 AS INT8)"),
        ("INT16", "CAST(3 AS INT16)"),
        ("INT32", "CAST(3 AS INT32)"),
        ("INT64", "CAST(3 AS INT64)"),
        ("INT128", "CAST(3 AS INT128)"),
        ("UINT8", "CAST(3 AS UINT8)"),
        ("UINT16", "CAST(3 AS UINT16)"),
        ("UINT32", "CAST(3 AS UINT32)"),
        ("UINT64", "CAST(3 AS UINT64)"),
        ("UINT128", "CAST(3 AS UINT128)"),
    ];

    for (label, selector_expr) in typed_selectors {
        let sql = format!(
            "SELECT array->{} AS result FROM ArrowSample;",
            selector_expr
        );
        let test_name = format!("Arrow selector uses {label}");

        g.named_test(&test_name, sql.as_str(), Ok(select!(result F64; 4.25_f64)))
            .await;
    }
});
