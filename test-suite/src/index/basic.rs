use {
    crate::*,
    gluesql_core::{
        ast::IndexOperator::*,
        executor::AlterError,
        prelude::{Payload, Value::*},
        store::IndexError,
        translate::TranslateError,
    },
};

test_case!(basic, async move {
    run!(
        "
CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT
)"
    );

    run!(
        "
        INSERT INTO Test
            (id, num, name)
        VALUES
            (1, 2, 'Hello'),
            (1, 17, 'World'),
            (11, 7, 'Great'),
            (4, 7, 'Job');
    "
    );

    test!("CREATE INDEX idx_id ON Test (id)", Ok(Payload::CreateIndex));
    test!(
        "CREATE INDEX idx_name ON Test (name)",
        Ok(Payload::CreateIndex)
    );
    test!(
        "CREATE INDEX idx_id2 ON Test (id + num)",
        Ok(Payload::CreateIndex)
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            11    7     "Great".to_owned();
            4     7     "Job".to_owned()
        )),
        idx!(),
        "SELECT id, num, name FROM Test"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            4     7     "Job".to_owned();
            11    7     "Great".to_owned()
        )),
        idx!(idx_id, Lt, "20"),
        "SELECT id, num, name FROM Test WHERE id < 20"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            4     7     "Job".to_owned();
            11    7     "Great".to_owned()
        )),
        idx!(idx_id, Lt, "20"),
        "SELECT id, num, name FROM Test WHERE 20 > id"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            4     7     "Job".to_owned()
        )),
        idx!(idx_id, LtEq, "4"),
        "SELECT id, num, name FROM Test WHERE id <= 4"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            4     7     "Job".to_owned()
        )),
        idx!(idx_id, LtEq, "4"),
        "SELECT id, num, name FROM Test WHERE 4 >= id"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            4     7     "Job".to_owned();
            11    7     "Great".to_owned()
        )),
        idx!(idx_id, GtEq, "4"),
        "SELECT id, num, name FROM Test WHERE id >= 4"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            4     7     "Job".to_owned();
            11    7     "Great".to_owned()
        )),
        idx!(idx_id, GtEq, "4"),
        "SELECT id, num, name FROM Test WHERE 4 <= id"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            4     7     "Job".to_owned();
            11    7     "Great".to_owned()
        )),
        idx!(idx_id, Gt, "0"),
        "SELECT id, num, name FROM Test WHERE id > 0"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            11    7     "Great".to_owned()
        )),
        idx!(idx_id, Gt, "4"),
        "SELECT id, num, name FROM Test WHERE 4 < id"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned()
        )),
        idx!(idx_id, Eq, "1"),
        "SELECT id, num, name FROM Test WHERE id = 1"
    );

    test!(
        "INSERT INTO Test (id, num, name) VALUES (1, 30, 'New one')",
        Ok(Payload::Insert(1))
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            1     30    "New one".to_owned()
        )),
        idx!(idx_id, Eq, "1"),
        "SELECT id, num, name FROM Test WHERE 1 = id"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     30    "New one".to_owned()
        )),
        idx!(idx_name, Eq, "'New one'"),
        "SELECT id, num, name FROM Test WHERE name = 'New one'"
    );

    test_idx!(
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "num".to_owned(), "name".to_owned()],
            rows: vec![]
        }),
        idx!(idx_id2, Eq, "10"),
        "SELECT id, num, name FROM Test WHERE id + num = 10"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_id2, Lt, "11"),
        "SELECT id, num, name FROM Test WHERE id + num < 11"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_id2, Lt, "11"),
        "SELECT id, num, name FROM Test WHERE 11 > id + num"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     17    "World".to_owned();
            11    7     "Great".to_owned()
        )),
        idx!(idx_id2, Eq, "18"),
        "SELECT id, num, name FROM Test WHERE id + num = 18"
    );

    test!("DELETE FROM Test WHERE id = 11", Ok(Payload::Delete(1)));
    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_id2, Eq, "3"),
        "SELECT id, num, name FROM Test WHERE id + num = 3"
    );

    test!(
        "UPDATE Test SET id = id + 1 WHERE id = 1;",
        Ok(Payload::Update(3))
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            2     17    "World".to_owned()
        )),
        idx!(idx_id2, Eq, "19"),
        "SELECT * FROM Test WHERE 19 = id + num"
    );

    test!("DROP INDEX Test.idx_id2;", Ok(Payload::DropIndex));
    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            2     17    "World".to_owned()
        )),
        idx!(),
        "SELECT * FROM Test WHERE id + num = 19"
    );

    test_idx!(
        Ok(Payload::Select {
            labels: vec!["id".to_owned()],
            rows: vec![],
        }),
        idx!(),
        "SELECT id FROM Test WHERE id + num = id"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            2     2     "Hello".to_owned();
            2     17    "World".to_owned();
            2     30    "New one".to_owned();
            4     7     "Job".to_owned()
        )),
        idx!(idx_id, Lt, "20"),
        "SELECT id, num, name FROM Test WHERE id < 20"
    );

    test!(
        "CREATE INDEX idx_com ON Test (id, num)",
        Err(TranslateError::CompositeIndexNotSupported.into())
    );

    test!(
        "DROP INDEX Test.idx_id, Test.idx_id2",
        Err(TranslateError::TooManyParamsInDropIndex.into())
    );

    test!(
        "CREATE INDEX idx_wow On Test (a.b)",
        Err(AlterError::UnsupportedIndexExpr(expr!("a.b")).into())
    );

    test!(
        "CREATE INDEX idx_wow ON Abc (name)",
        Err(AlterError::TableNotFound("Abc".to_owned()).into())
    );

    test!(
        "DROP INDEX NoNameTable.idx_id",
        Err(IndexError::TableNotFound("NoNameTable".to_owned()).into())
    );

    test!(
        "CREATE INDEX idx_name ON Test (name || id)",
        Err(IndexError::IndexNameAlreadyExists("idx_name".to_owned()).into())
    );

    test!(
        "DROP INDEX Test.idx_aaa",
        Err(IndexError::IndexNameDoesNotExist("idx_aaa".to_owned()).into())
    );
});
