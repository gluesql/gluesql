use {
    crate::{row, select, select_with_null, stringify_label, test_case},
    gluesql_core::{
        error::TranslateError,
        executor::EvaluateError,
        prelude::Value::{self, Str},
    },
};

// Extract MAP values as text via the ->> operator.
// Covers various value types (integer, string, float, bool, nested map),
// integer keys, and missing keys.
test_case!(map, {
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

    for (selector, expected) in [
        ("'id'", "1"),
        ("'b'", "2"),
        ("'name'", "Han"),
        ("'price'", "4.25"),
        ("'active'", "TRUE"),
        ("'nested'", r#"{"role":"admin"}"#),
        ("1", "first"),
    ] {
        g.named_test(
            &format!("object->>{selector}"),
            &format!("SELECT object->>{selector} AS result FROM LongArrowSample;"),
            Ok(select!(result Str; expected.to_owned())),
        )
        .await;
    }

    g.test(
        "SELECT object->>'missing' AS result FROM LongArrowSample;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;
});

// Extract LIST elements as text via the ->> operator.
// Covers integer index, string index, NULL element, negative index,
// out-of-bounds index, and non-numeric string index.
test_case!(list, {
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

    for (selector, expected) in [
        ("0", "1"),
        ("1", "two"),
        ("2", "TRUE"),
        ("3", "4.25"),
        ("'3'", "4.25"),
    ] {
        g.named_test(
            &format!("array->>{selector}"),
            &format!("SELECT array->>{selector} AS result FROM LongArrowSample;"),
            Ok(select!(result Str; expected.to_owned())),
        )
        .await;
    }

    for selector in ["4", "(-1)", "100", "'foo'"] {
        g.named_test(
            &format!("array->>{selector} returns NULL"),
            &format!("SELECT array->>{selector} AS result FROM LongArrowSample;"),
            Ok(select_with_null!(result; Value::Null)),
        )
        .await;
    }
});

// NULL handling for the ->> operator.
// Returns NULL when either the base or the selector is NULL.
test_case!(null, {
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

    for sql in [
        "SELECT NULL->>'key' AS result;",
        "SELECT object->>NULL AS result FROM LongArrowSample;",
    ] {
        g.test(sql, Ok(select_with_null!(result; Value::Null)))
            .await;
    }
});

// Chaining -> and ->> operators to extract nested values as text.
// e.g. object->'nested'->>'role'
test_case!(chaining, {
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

    g.test(
        r"SELECT object->'nested'->>'role' AS result FROM LongArrowSample;",
        Ok(select!(result Str; "admin".to_owned())),
    )
    .await;
});

// Typed integer selectors via CAST for the ->> operator.
// Verifies all integer types (INT8–UINT128) work as MAP keys and LIST indices,
// and that a negative CAST value returns NULL.
test_case!(typed_selector, {
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

    let int_types = [
        "INT8", "INT16", "INT32", "INT64", "INT128", "UINT8", "UINT16", "UINT32", "UINT64",
        "UINT128",
    ];

    for t in int_types {
        g.named_test(
            &format!("map ->> CAST(1 AS {t})"),
            &format!("SELECT object->>CAST(1 AS {t}) AS result FROM LongArrowSample;"),
            Ok(select!(result Str; "first".to_owned())),
        )
        .await;

        g.named_test(
            &format!("list ->> CAST(3 AS {t})"),
            &format!("SELECT array->>CAST(3 AS {t}) AS result FROM LongArrowSample;"),
            Ok(select!(result Str; "4.25".to_owned())),
        )
        .await;
    }

    g.test(
        "SELECT array->>CAST(-1 AS INT16) AS result FROM LongArrowSample;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;
});

// Error cases for the ->> operator.
// Covers unsupported operator (->>-), non-MAP/LIST base types,
// and non-integer/string selector types.
test_case!(error, {
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

    g.test(
        "SELECT array->>-1 AS result FROM LongArrowSample;",
        Err(TranslateError::UnsupportedBinaryOperator("->>-".to_owned()).into()),
    )
    .await;

    for sql in [
        "SELECT 1 ->> 'foo' AS result;",
        "SELECT TRUE ->> 'foo' AS result;",
        r#"SELECT '{"role":"admin"}' ->> 'role' AS result;"#,
    ] {
        g.test(sql, Err(EvaluateError::ArrowBaseRequiresMapOrList.into()))
            .await;
    }

    g.test(
        "SELECT object->>TRUE AS result FROM LongArrowSample;",
        Err(EvaluateError::ArrowSelectorRequiresIntegerOrString("Bool(true)".to_owned()).into()),
    )
    .await;
});
