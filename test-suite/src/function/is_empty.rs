use {
    crate::*,
    gluesql_core::{error::EvaluateError, prelude::Value::*},
};
test_case!(is_empty, async move {
    run!(
        "CREATE TABLE IsEmpty (
            id INTEGER,
            list_items LIST,
            map_items MAP
        );"
    );
    run!(
        r#"
            INSERT INTO IsEmpty VALUES
            (1, '[]', '{"a": {"red": "cherry", "blue": 2}, "b": 20}'),
            (2, '[1, 2, 3]', '{"a": {"red": "berry", "blue": 3}, "b": 30, "c": true}'),
            (3, '[]', '{}'),
            (4, '[10]', '{}');
        "#
    );

    test!(
        name: "is_empty for list, return true",
        sql: r#"SELECT id FROM IsEmpty WHERE IS_EMPTY(list_items);"#,
        expected: Ok(select!(id; I64; 1; 3))
    );
    test!(
        name: "is_empty for list, return false",
        sql: r#"SELECT IS_EMPTY(list_items) as result FROM IsEmpty WHERE id=2;"#,
        expected: Ok(select!(result; Bool; false))
    );

    test!(
        name: "is_empty for map, return true",
        sql: r#"SELECT id FROM IsEmpty WHERE IS_EMPTY(map_items);"#,
        expected: Ok(select!(id; I64; 3; 4))
    );
    test!(
        name: "is_empty for map, return false",
        sql: r#"SELECT IS_EMPTY(map_items) as result FROM IsEmpty WHERE id=1;"#,
        expected: Ok(select!(result; Bool; false))
    );

    test!(
        name: "other argument types, return error",
        sql: r#"SELECT id FROM IsEmpty WHERE IS_EMPTY(id);"#,
        expected: Err(EvaluateError::MapOrListTypeRequired.into())
    );
});
