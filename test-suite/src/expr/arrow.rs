use {
    crate::*,
    gluesql_core::prelude::Value::{self, Bool, F64, I64, Map, Str},
};

test_case!(arrow, {
    let g = get_tester!();

    g.run(
        r#"
        CREATE TABLE ArrowSample (object MAP, array LIST);
        INSERT INTO ArrowSample VALUES (
            '{"id":1,"b":2,"name":"Han","price":4.25,"active":true,"nested":{"role":"admin"}}',
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
        "SELECT object->'missing' AS result FROM ArrowSample;",
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
        "SELECT 1 -> 'foo' AS result;",
        Ok(select_with_null!(result; Value::Null)),
    )
    .await;
});
