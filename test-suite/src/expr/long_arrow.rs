use {
    crate::{row, select, select_with_null, stringify_label, test_case},
    gluesql_core::{
        error::TranslateError,
        executor::EvaluateError,
        prelude::Value::{self, Str},
    },
    serde_json::json,
};

const CREATE_TABLE: &str = "
    CREATE TABLE LongArrowSample (
        object MAP,
        array  LIST
    )
";

fn insert_data() -> String {
    format!(
        "INSERT INTO LongArrowSample (object, array) VALUES ('{}', '{}')",
        json!({
            "id":     1,
            "b":      2,
            "name":   "Han",
            "price":  4.25,
            "active": true,
            "nested": {"role": "admin"},
            "1":      "first"
        }),
        json!([1, "two", true, 4.25, null]),
    )
}

// Extract MAP values as text via the ->> operator.
test_case!(map, {
    let g = get_tester!();
    g.run(CREATE_TABLE).await;
    g.run(&insert_data()).await;

    g.test(
        "SELECT object->>'id' AS result FROM LongArrowSample",
        Ok(select!(result Str; "1".to_owned())),
    )
    .await;

    g.test(
        "SELECT object->>'b' AS result FROM LongArrowSample",
        Ok(select!(result Str; "2".to_owned())),
    )
    .await;

    g.test(
        "SELECT object->>'name' AS result FROM LongArrowSample",
        Ok(select!(result Str; "Han".to_owned())),
    )
    .await;

    g.test(
        "SELECT object->>'price' AS result FROM LongArrowSample",
        Ok(select!(result Str; "4.25".to_owned())),
    )
    .await;

    g.test(
        "SELECT object->>'active' AS result FROM LongArrowSample",
        Ok(select!(result Str; "TRUE".to_owned())),
    )
    .await;

    g.test(
        r"SELECT object->>'nested' AS result FROM LongArrowSample",
        Ok(select!(result Str; r#"{"role":"admin"}"#.to_owned())),
    )
    .await;

    g.test(
        "SELECT object->>1 AS result FROM LongArrowSample",
        Ok(select!(result Str; "first".to_owned())),
    )
    .await;

    g.test(
        "SELECT object->>'missing' AS result FROM LongArrowSample",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;
});

// Extract LIST elements as text via the ->> operator.
test_case!(list, {
    let g = get_tester!();
    g.run(CREATE_TABLE).await;
    g.run(&insert_data()).await;

    g.test(
        "SELECT array->>0 AS result FROM LongArrowSample",
        Ok(select!(result Str; "1".to_owned())),
    )
    .await;

    g.test(
        "SELECT array->>1 AS result FROM LongArrowSample",
        Ok(select!(result Str; "two".to_owned())),
    )
    .await;

    g.test(
        "SELECT array->>2 AS result FROM LongArrowSample",
        Ok(select!(result Str; "TRUE".to_owned())),
    )
    .await;

    g.test(
        "SELECT array->>3 AS result FROM LongArrowSample",
        Ok(select!(result Str; "4.25".to_owned())),
    )
    .await;

    g.test(
        "SELECT array->>'3' AS result FROM LongArrowSample",
        Ok(select!(result Str; "4.25".to_owned())),
    )
    .await;

    // NULL element, negative index, out-of-bounds, non-numeric string
    g.test(
        "SELECT array->>4 AS result FROM LongArrowSample",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    g.test(
        "SELECT array->>(-1) AS result FROM LongArrowSample",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    g.test(
        "SELECT array->>100 AS result FROM LongArrowSample",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    g.test(
        "SELECT array->>'foo' AS result FROM LongArrowSample",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;
});

// NULL handling for the ->> operator.
test_case!(null, {
    let g = get_tester!();
    g.run(CREATE_TABLE).await;
    g.run(&insert_data()).await;

    g.test(
        "SELECT NULL->>'key' AS result",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;

    g.test(
        "SELECT object->>NULL AS result FROM LongArrowSample",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;
});

// Chaining -> and ->> operators to extract nested values as text.
test_case!(chaining, {
    let g = get_tester!();
    g.run(CREATE_TABLE).await;
    g.run(&insert_data()).await;

    g.test(
        "SELECT object->'nested'->>'role' AS result FROM LongArrowSample",
        Ok(select!(result Str; "admin".to_owned())),
    )
    .await;
});

// Typed integer selectors via CAST for the ->> operator.
test_case!(typed_selector, {
    let g = get_tester!();
    g.run(CREATE_TABLE).await;
    g.run(&insert_data()).await;

    // MAP: object->>CAST(1 AS <type>) → "first"
    g.test(
        "SELECT object->>CAST(1 AS INT8) AS result FROM LongArrowSample",
        Ok(select!(result Str; "first".to_owned())),
    )
    .await;
    g.test(
        "SELECT object->>CAST(1 AS INT16) AS result FROM LongArrowSample",
        Ok(select!(result Str; "first".to_owned())),
    )
    .await;
    g.test(
        "SELECT object->>CAST(1 AS INT32) AS result FROM LongArrowSample",
        Ok(select!(result Str; "first".to_owned())),
    )
    .await;
    g.test(
        "SELECT object->>CAST(1 AS INT64) AS result FROM LongArrowSample",
        Ok(select!(result Str; "first".to_owned())),
    )
    .await;
    g.test(
        "SELECT object->>CAST(1 AS INT128) AS result FROM LongArrowSample",
        Ok(select!(result Str; "first".to_owned())),
    )
    .await;
    g.test(
        "SELECT object->>CAST(1 AS UINT8) AS result FROM LongArrowSample",
        Ok(select!(result Str; "first".to_owned())),
    )
    .await;
    g.test(
        "SELECT object->>CAST(1 AS UINT16) AS result FROM LongArrowSample",
        Ok(select!(result Str; "first".to_owned())),
    )
    .await;
    g.test(
        "SELECT object->>CAST(1 AS UINT32) AS result FROM LongArrowSample",
        Ok(select!(result Str; "first".to_owned())),
    )
    .await;
    g.test(
        "SELECT object->>CAST(1 AS UINT64) AS result FROM LongArrowSample",
        Ok(select!(result Str; "first".to_owned())),
    )
    .await;
    g.test(
        "SELECT object->>CAST(1 AS UINT128) AS result FROM LongArrowSample",
        Ok(select!(result Str; "first".to_owned())),
    )
    .await;

    // LIST: array->>CAST(3 AS <type>) → "4.25"
    g.test(
        "SELECT array->>CAST(3 AS INT8) AS result FROM LongArrowSample",
        Ok(select!(result Str; "4.25".to_owned())),
    )
    .await;
    g.test(
        "SELECT array->>CAST(3 AS INT16) AS result FROM LongArrowSample",
        Ok(select!(result Str; "4.25".to_owned())),
    )
    .await;
    g.test(
        "SELECT array->>CAST(3 AS INT32) AS result FROM LongArrowSample",
        Ok(select!(result Str; "4.25".to_owned())),
    )
    .await;
    g.test(
        "SELECT array->>CAST(3 AS INT64) AS result FROM LongArrowSample",
        Ok(select!(result Str; "4.25".to_owned())),
    )
    .await;
    g.test(
        "SELECT array->>CAST(3 AS INT128) AS result FROM LongArrowSample",
        Ok(select!(result Str; "4.25".to_owned())),
    )
    .await;
    g.test(
        "SELECT array->>CAST(3 AS UINT8) AS result FROM LongArrowSample",
        Ok(select!(result Str; "4.25".to_owned())),
    )
    .await;
    g.test(
        "SELECT array->>CAST(3 AS UINT16) AS result FROM LongArrowSample",
        Ok(select!(result Str; "4.25".to_owned())),
    )
    .await;
    g.test(
        "SELECT array->>CAST(3 AS UINT32) AS result FROM LongArrowSample",
        Ok(select!(result Str; "4.25".to_owned())),
    )
    .await;
    g.test(
        "SELECT array->>CAST(3 AS UINT64) AS result FROM LongArrowSample",
        Ok(select!(result Str; "4.25".to_owned())),
    )
    .await;
    g.test(
        "SELECT array->>CAST(3 AS UINT128) AS result FROM LongArrowSample",
        Ok(select!(result Str; "4.25".to_owned())),
    )
    .await;

    // Negative CAST → NULL
    g.test(
        "SELECT array->>CAST(-1 AS INT16) AS result FROM LongArrowSample",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;
});

// Error cases for the ->> operator.
test_case!(error, {
    let g = get_tester!();
    g.run(CREATE_TABLE).await;
    g.run(&insert_data()).await;

    g.test(
        "SELECT array->>-1 AS result FROM LongArrowSample",
        Err(TranslateError::UnsupportedBinaryOperator("->>-".to_owned()).into()),
    )
    .await;

    g.test(
        "SELECT 1 ->> 'foo' AS result",
        Err(EvaluateError::ArrowBaseRequiresMapOrList.into()),
    )
    .await;

    g.test(
        "SELECT TRUE ->> 'foo' AS result",
        Err(EvaluateError::ArrowBaseRequiresMapOrList.into()),
    )
    .await;

    g.test(
        r#"SELECT '{"role":"admin"}' ->> 'role' AS result"#,
        Err(EvaluateError::ArrowBaseRequiresMapOrList.into()),
    )
    .await;

    g.test(
        "SELECT object->>TRUE AS result FROM LongArrowSample",
        Err(EvaluateError::ArrowSelectorRequiresIntegerOrString("Bool(true)".to_owned()).into()),
    )
    .await;
});
