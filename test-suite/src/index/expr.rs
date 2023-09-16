use {
    crate::{expr as parse_expr, *},
    gluesql_core::{
        ast::IndexOperator::*,
        error::AlterError,
        prelude::{Payload, Value::*},
    },
};

test_case!(expr, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE Test (
            id INTEGER,
            num INTEGER,
            name TEXT
        )
    ",
    )
    .await;

    g.run(
        "
        INSERT INTO Test
            (id, num, name)
        VALUES
            (1, 2, 'Hello');
    ",
    )
    .await;

    g.test("CREATE INDEX idx_id ON Test (id)", Ok(Payload::CreateIndex))
        .await;

    g.test(
        "CREATE INDEX idx_typed_string ON Test ((id))",
        Ok(Payload::CreateIndex),
    )
    .await;

    g.test(
        "CREATE INDEX idx_binary_op ON Test (num || name);",
        Ok(Payload::CreateIndex),
    )
    .await;

    g.test(
        "CREATE INDEX idx_unary_op ON Test (-num);",
        Ok(Payload::CreateIndex),
    )
    .await;

    g.test(
        "CREATE INDEX idx_cast ON Test (CAST(id AS TEXT));",
        Ok(Payload::CreateIndex),
    )
    .await;

    g.test(
        "CREATE INDEX idx_literal ON Test (100)",
        Err(AlterError::IdentifierNotFound(parse_expr("100")).into()),
    )
    .await;

    g.test(
        "INSERT INTO Test VALUES (4, 7, 'Well');",
        Ok(Payload::Insert(1)),
    )
    .await;

    g.test(
        "SELECT id, num, name FROM Test",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            4     7     "Well".to_owned()
        )),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE id <= 1",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_id, LtEq, "1"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE id <= (1)",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_id, LtEq, "(1)"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE num || name = '2Hello'",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_binary_op, Eq, "'2Hello'"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE (num || name) = '2Hello'",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_binary_op, Eq, "'2Hello'"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE '7Well' = (num || name)",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            4     7     "Well".to_owned()
        )),
        idx!(idx_binary_op, Eq, "'7Well'"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE -num < -2",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            4     7     "Well".to_owned()
        )),
        idx!(idx_unary_op, Lt, "-2"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE CAST(id AS TEXT) = '4'",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            4     7     "Well".to_owned()
        )),
        idx!(idx_cast, Eq, "'4'"),
    )
    .await;
});
