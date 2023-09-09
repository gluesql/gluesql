use {
    crate::*,
    gluesql_core::{error::EvaluateError, prelude::Value::*},
};
test_case!(is_empty, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE IsEmpty (
            id INTEGER,
            list_items LIST,
            map_items MAP
        );",
    )
    .await;
    g.run(
        r#"
            INSERT INTO IsEmpty VALUES
            (1, '[]', '{"a": {"red": "cherry", "blue": 2}, "b": 20}'),
            (2, '[1, 2, 3]', '{"a": {"red": "berry", "blue": 3}, "b": 30, "c": true}'),
            (3, '[]', '{}'),
            (4, '[10]', '{}');
        "#,
    )
    .await;

    g.named_test(
        "is_empty for list, return true",
        r#"SELECT id FROM IsEmpty WHERE IS_EMPTY(list_items);"#,
        Ok(select!(id; I64; 1; 3)),
    )
    .await;
    g.named_test(
        "is_empty for list, return false",
        r#"SELECT IS_EMPTY(list_items) as result FROM IsEmpty WHERE id=2;"#,
        Ok(select!(result; Bool; false)),
    )
    .await;

    g.named_test(
        "is_empty for map, return true",
        r#"SELECT id FROM IsEmpty WHERE IS_EMPTY(map_items);"#,
        Ok(select!(id; I64; 3; 4)),
    )
    .await;
    g.named_test(
        "is_empty for map, return false",
        r#"SELECT IS_EMPTY(map_items) as result FROM IsEmpty WHERE id=1;"#,
        Ok(select!(result; Bool; false)),
    )
    .await;

    g.named_test(
        "other argument types, return error",
        r#"SELECT id FROM IsEmpty WHERE IS_EMPTY(id);"#,
        Err(EvaluateError::MapOrListTypeRequired.into()),
    )
    .await;
});
