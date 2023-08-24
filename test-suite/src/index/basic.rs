use {
    crate::*,
    gluesql_core::{
        ast::IndexOperator::*,
        error::{AlterError, IndexError, TranslateError},
        prelude::{Payload, Value::*},
    },
};

test_case!(basic, {
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
            (1, 2, 'Hello'),
            (1, 17, 'World'),
            (11, 7, 'Great'),
            (4, 7, 'Job');
    ",
    )
    .await;

    g.test("CREATE INDEX idx_id ON Test (id)", Ok(Payload::CreateIndex))
        .await;
    g.test(
        "CREATE INDEX idx_name ON Test (name)",
        Ok(Payload::CreateIndex),
    )
    .await;
    g.test(
        "CREATE INDEX idx_id2 ON Test (id + num)",
        Ok(Payload::CreateIndex),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            11    7     "Great".to_owned();
            4     7     "Job".to_owned()
        )),
        idx!(),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE id < 20",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            4     7     "Job".to_owned();
            11    7     "Great".to_owned()
        )),
        idx!(idx_id, Lt, "20"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE 20 > id",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            4     7     "Job".to_owned();
            11    7     "Great".to_owned()
        )),
        idx!(idx_id, Lt, "20"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE id <= 4",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            4     7     "Job".to_owned()
        )),
        idx!(idx_id, LtEq, "4"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE 4 >= id",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            4     7     "Job".to_owned()
        )),
        idx!(idx_id, LtEq, "4"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE id >= 4",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            4     7     "Job".to_owned();
            11    7     "Great".to_owned()
        )),
        idx!(idx_id, GtEq, "4"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE 4 <= id",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            4     7     "Job".to_owned();
            11    7     "Great".to_owned()
        )),
        idx!(idx_id, GtEq, "4"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE id > 0",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            4     7     "Job".to_owned();
            11    7     "Great".to_owned()
        )),
        idx!(idx_id, Gt, "0"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE 4 < id",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            11    7     "Great".to_owned()
        )),
        idx!(idx_id, Gt, "4"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE id = 1",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned()
        )),
        idx!(idx_id, Eq, "1"),
    )
    .await;

    g.test(
        "INSERT INTO Test (id, num, name) VALUES (1, 30, 'New one')",
        Ok(Payload::Insert(1)),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE 1 = id",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            1     30    "New one".to_owned()
        )),
        idx!(idx_id, Eq, "1"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE name = 'New one'",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     30    "New one".to_owned()
        )),
        idx!(idx_name, Eq, "'New one'"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE id + num = 10",
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "num".to_owned(), "name".to_owned()],
            rows: vec![],
        }),
        idx!(idx_id2, Eq, "10"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE id + num < 11",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_id2, Lt, "11"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE 11 > id + num",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_id2, Lt, "11"),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE id + num = 18",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     17    "World".to_owned();
            11    7     "Great".to_owned()
        )),
        idx!(idx_id2, Eq, "18"),
    )
    .await;

    g.test("DELETE FROM Test WHERE id = 11", Ok(Payload::Delete(1)))
        .await;
    g.test_idx(
        "SELECT id, num, name FROM Test WHERE id + num = 3",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_id2, Eq, "3"),
    )
    .await;

    g.test(
        "UPDATE Test SET id = id + 1 WHERE id = 1;",
        Ok(Payload::Update(3)),
    )
    .await;

    g.test_idx(
        "SELECT * FROM Test WHERE 19 = id + num",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            2     17    "World".to_owned()
        )),
        idx!(idx_id2, Eq, "19"),
    )
    .await;

    g.test("DROP INDEX Test.idx_id2;", Ok(Payload::DropIndex))
        .await;
    g.test_idx(
        "SELECT * FROM Test WHERE id + num = 19",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            2     17    "World".to_owned()
        )),
        idx!(),
    )
    .await;

    g.test_idx(
        "SELECT id FROM Test WHERE id + num = id",
        Ok(Payload::Select {
            labels: vec!["id".to_owned()],
            rows: vec![],
        }),
        idx!(),
    )
    .await;

    g.test_idx(
        "SELECT id, num, name FROM Test WHERE id < 20",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            2     2     "Hello".to_owned();
            2     17    "World".to_owned();
            2     30    "New one".to_owned();
            4     7     "Job".to_owned()
        )),
        idx!(idx_id, Lt, "20"),
    )
    .await;

    g.test(
        "CREATE INDEX idx_com ON Test (id, num)",
        Err(TranslateError::CompositeIndexNotSupported.into()),
    )
    .await;

    g.test(
        "DROP INDEX Test.idx_id, Test.idx_id2",
        Err(TranslateError::TooManyParamsInDropIndex.into()),
    )
    .await;

    g.test(
        "CREATE INDEX idx_wow On Test (a.b)",
        Err(AlterError::UnsupportedIndexExpr(expr("a.b")).into()),
    )
    .await;

    g.test(
        "CREATE INDEX idx_wow ON Abc (name)",
        Err(AlterError::TableNotFound("Abc".to_owned()).into()),
    )
    .await;

    g.test(
        "DROP INDEX NoNameTable.idx_id",
        Err(IndexError::TableNotFound("NoNameTable".to_owned()).into()),
    )
    .await;

    g.test(
        "CREATE INDEX idx_name ON Test (name || id)",
        Err(IndexError::IndexNameAlreadyExists("idx_name".to_owned()).into()),
    )
    .await;

    g.test(
        "DROP INDEX Test.idx_aaa",
        Err(IndexError::IndexNameDoesNotExist("idx_aaa".to_owned()).into()),
    )
    .await;
});
