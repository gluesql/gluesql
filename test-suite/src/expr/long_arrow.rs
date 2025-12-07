use {
    crate::{row, select, select_with_null, stringify_label, test_case},
    gluesql_core::{
        error::TranslateError,
        executor::EvaluateError,
        prelude::Value::{self, Str},
    },
};

test_case!(long_arrow, {
    let g = get_tester!();

    g.run("CREATE TABLE LongArrowSample (object MAP, array LIST);")
        .await;

    g.run(
        r#"
        INSERT INTO LongArrowSample VALUES (
            '{"id":1,"b":2,"name":"Han","price":4.25,"active":true,"nested":{"role":"admin"},"1":"first"}',
            '[1,"two",true,4.25,null]'
        );
        "#,
    )
    .await;

    // LongArrow (->>): Extract as text
    g.test(
        "SELECT object->>'b' AS result FROM LongArrowSample;",
        Ok(select!(result Str; "2".to_owned())),
    )
    .await;

    g.test(
        "SELECT object->>'name' AS result FROM LongArrowSample;",
        Ok(select!(result Str; "Han".to_owned())),
    )
    .await;

    g.test(
        "SELECT object->>'price' AS result FROM LongArrowSample;",
        Ok(select!(result Str; "4.25".to_owned())),
    )
    .await;

    g.test(
        "SELECT object->>'active' AS result FROM LongArrowSample;",
        Ok(select!(result Str; "TRUE".to_owned())),
    )
    .await;

    // LongArrow on nested map - returns JSON-like string
    g.test(
        r"SELECT object->>'nested' AS result FROM LongArrowSample;",
        Ok(select!(result Str; r#"{"role":"admin"}"#.to_owned())),
    )
    .await;

    // LongArrow on array elements
    g.test(
        "SELECT array->>0 AS result FROM LongArrowSample;",
        Ok(select!(result Str; "1".to_owned())),
    )
    .await;

    g.test(
        "SELECT array->>1 AS result FROM LongArrowSample;",
        Ok(select!(result Str; "two".to_owned())),
    )
    .await;

    g.test(
        "SELECT array->>2 AS result FROM LongArrowSample;",
        Ok(select!(result Str; "TRUE".to_owned())),
    )
    .await;

    g.test(
        "SELECT array->>3 AS result FROM LongArrowSample;",
        Ok(select!(result Str; "4.25".to_owned())),
    )
    .await;

    // LongArrow on NULL element in array
    g.test(
        "SELECT array->>4 AS result FROM LongArrowSample;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    // LongArrow with missing key
    g.test(
        "SELECT object->>'missing' AS result FROM LongArrowSample;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    // LongArrow with NULL base
    g.test(
        "SELECT NULL->>'key' AS result;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    // LongArrow with NULL selector
    g.test(
        "SELECT object->>NULL AS result FROM LongArrowSample;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    // LongArrow on array with negative index (via parentheses)
    g.test(
        "SELECT array->>(-1) AS result FROM LongArrowSample;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    // LongArrow on array with out-of-bounds index
    g.test(
        "SELECT array->>100 AS result FROM LongArrowSample;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    // LongArrow on array with string index
    g.test(
        "SELECT array->>'3' AS result FROM LongArrowSample;",
        Ok(select!(result Str; "4.25".to_owned())),
    )
    .await;

    // LongArrow with non-numeric string on array returns NULL
    g.test(
        "SELECT array->>'foo' AS result FROM LongArrowSample;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    // LongArrow on map with integer key
    g.test(
        "SELECT object->>1 AS result FROM LongArrowSample;",
        Ok(select!(result Str; "first".to_owned())),
    )
    .await;

    // LongArrow error cases
    g.test(
        "SELECT array->>-1 AS result FROM LongArrowSample;",
        Err(TranslateError::UnsupportedBinaryOperator("->>-".to_owned()).into()),
    )
    .await;

    g.test(
        "SELECT 1 ->> 'foo' AS result;",
        Err(EvaluateError::ArrowBaseRequiresMapOrList.into()),
    )
    .await;

    g.test(
        "SELECT TRUE ->> 'foo' AS result;",
        Err(EvaluateError::ArrowBaseRequiresMapOrList.into()),
    )
    .await;

    g.test(
        r#"SELECT '{"role":"admin"}' ->> 'role' AS result;"#,
        Err(EvaluateError::ArrowBaseRequiresMapOrList.into()),
    )
    .await;

    g.test(
        "SELECT object->>TRUE AS result FROM LongArrowSample;",
        Err(EvaluateError::ArrowSelectorRequiresIntegerOrString("Bool(true)".to_owned()).into()),
    )
    .await;

    // LongArrow with typed integer selectors on map
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
        let sql = format!("SELECT object->>{selector_expr} AS result FROM LongArrowSample;");
        let test_name = format!("LongArrow map selector uses {label}");

        g.named_test(
            &test_name,
            sql.as_str(),
            Ok(select!(result Str; "first".to_owned())),
        )
        .await;
    }

    // LongArrow with typed integer selectors on array
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
        let sql = format!("SELECT array->>{selector_expr} AS result FROM LongArrowSample;");
        let test_name = format!("LongArrow selector uses {label}");

        g.named_test(
            &test_name,
            sql.as_str(),
            Ok(select!(result Str; "4.25".to_owned())),
        )
        .await;
    }

    // LongArrow with CAST on negative value
    g.test(
        "SELECT array->>CAST(-1 AS INT16) AS result FROM LongArrowSample;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    // Chained arrow operators with long arrow at the end
    g.test(
        r"SELECT object->'nested'->>'role' AS result FROM LongArrowSample;",
        Ok(select!(result Str; "admin".to_owned())),
    )
    .await;

    // LongArrow on id (integer value in map)
    g.test(
        "SELECT object->>'id' AS result FROM LongArrowSample;",
        Ok(select!(result Str; "1".to_owned())),
    )
    .await;
});
