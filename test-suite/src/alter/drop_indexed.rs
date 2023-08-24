use {
    crate::*,
    gluesql_core::{
        ast::IndexOperator::*,
        error::{AlterError, FetchError, TranslateError},
        prelude::Value::*,
    },
};

test_case!(drop_indexed_table, {
    let g = get_tester!();

    g.run("DROP TABLE IF EXISTS Test;").await;
    g.run("CREATE TABLE Test (id INTEGER);").await;
    g.run("INSERT INTO Test VALUES (1), (2);").await;
    g.run("CREATE INDEX idx_id ON Test (id)").await;
    g.test_idx(
        "SELECT * FROM Test WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(idx_id, Eq, "1"),
    )
    .await;

    g.run("DROP TABLE Test;").await;
    g.test(
        "SELECT * FROM Test;",
        Err(FetchError::TableNotFound("Test".to_owned()).into()),
    )
    .await;

    g.run("CREATE TABLE Test (id INTEGER);").await;
    g.run("INSERT INTO Test VALUES (3), (4);").await;
    g.test_idx(
        "SELECT * FROM Test WHERE id = 3",
        Ok(select!(id I64; 3)),
        idx!(),
    )
    .await;

    g.run("CREATE INDEX idx_id ON Test (id)").await;
    g.test_idx(
        "SELECT * FROM Test WHERE id < 10",
        Ok(select!(id I64; 3; 4)),
        idx!(idx_id, Lt, "10"),
    )
    .await;

    g.test(
        "DROP INDEX Test",
        Err(TranslateError::InvalidParamsInDropIndex.into()),
    )
    .await;
    g.test(
        "DROP INDEX Test.idx_id.IndexC",
        Err(TranslateError::InvalidParamsInDropIndex.into()),
    )
    .await;
});

test_case!(drop_indexed_column, {
    let g = get_tester!();

    g.run(
        "
CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT
)",
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

    // create indexes
    for query in [
        "CREATE INDEX idx_name ON Test (num + 1)",
        "CREATE INDEX idx_id ON Test (id)",
        "CREATE INDEX idx_typed_string ON Test ((id))",
        "CREATE INDEX idx_binary_op ON Test (id || name);",
        "CREATE INDEX idx_unary_op ON Test (-id);",
        "CREATE INDEX idx_cast ON Test (CAST(id AS TEXT));",
    ] {
        g.run(query).await;
    }

    // check indexes working
    g.test(
        "CREATE INDEX idx_literal ON Test (100)",
        Err(AlterError::IdentifierNotFound(expr("100")).into()),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(),
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
        "SELECT id, num, name FROM Test WHERE id || name = '1Hello'",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_binary_op, Eq, "'1Hello'"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE -id >= -7",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_unary_op, GtEq, "-7"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE -id > -7",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_unary_op, Gt, "-7"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE CAST(id AS TEXT) = '1'",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_cast, Eq, "'1'"),
    )
    .await;

    g.test(
        "ALTER TABLE Noname DROP COLUMN id",
        Err(AlterError::TableNotFound("Noname".to_owned()).into()),
    )
    .await;

    g.run("ALTER TABLE Test DROP COLUMN id").await;

    g.test_idx(
        "SELECT * FROM Test",
        Ok(select!(
            num | name
            I64 | Str;
            2     "Hello".to_owned()
        )),
        idx!(),
    )
    .await;

    let schema = g
        .get_glue()
        .storage
        .fetch_schema("Test")
        .await
        .expect("error fetching schema")
        .expect("table not found");
    assert_eq!(schema.indexes.len(), 1, "Only idx_name remains.");
});
