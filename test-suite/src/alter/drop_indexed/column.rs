use super::*;

test_case!(column, {
    let g = get_tester!();

    g.run(
        "
CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT
)",
    );

    g.run(
        "
        INSERT INTO Test
            (id, num, name)
        VALUES
            (1, 2, 'Hello');
    ",
    );

    // create indexes
    for query in [
        "CREATE INDEX idx_name ON Test (num + 1)",
        "CREATE INDEX idx_id ON Test (id)",
        "CREATE INDEX idx_typed_string ON Test ((id))",
        "CREATE INDEX idx_binary_op ON Test (id || name);",
        "CREATE INDEX idx_unary_op ON Test (-id);",
        "CREATE INDEX idx_cast ON Test (CAST(id AS TEXT));",
    ] {
        g.run(query);
    }

    // check indexes working
    g.test(
        "CREATE INDEX idx_literal ON Test (100)",
        Err(AlterError::IndexExprRequiresColumnReference.into()),
    );

    g.test_idx(
        "SELECT id, num, name FROM Test",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(),
    );

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE id <= 1",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_id, LtEq, "1"),
    );

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE id <= (1)",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_id, LtEq, "(1)"),
    );

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE id || name = '1Hello'",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_binary_op, Eq, "'1Hello'"),
    );

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE -id >= -7",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_unary_op, GtEq, "-7"),
    );

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE -id > -7",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_unary_op, Gt, "-7"),
    );

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE CAST(id AS TEXT) = '1'",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_cast, Eq, "'1'"),
    );

    g.test(
        "ALTER TABLE Noname DROP COLUMN id",
        Err(AlterError::TableNotFound("Noname".to_owned()).into()),
    );

    g.run("ALTER TABLE Test DROP COLUMN id");

    g.test_idx(
        "SELECT * FROM Test",
        Ok(select!(
            num | name
            I64 | Str;
            2     "Hello".to_owned()
        )),
        idx!(),
    );

    let schema = g
        .get_glue()
        .storage
        .fetch_schema("Test")
        .expect("error fetching schema")
        .expect("table not found");
    assert_eq!(schema.indexes.len(), 1, "Only idx_name remains.");
});
