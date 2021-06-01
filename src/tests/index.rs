use crate::*;

test_case!(index, async move {
    run!(
        r#"
CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT
)"#
    );

    run!("INSERT INTO Test (id, num, name) VALUES (1, 2, \"Hello\")");
    run!("INSERT INTO Test (id, num, name) VALUES (1, 17, \"World\")");
    run!("INSERT INTO Test (id, num, name) VALUES (11, 7, \"Great\"), (4, 7, \"Job\")");

    test!(Ok(Payload::CreateIndex), "CREATE INDEX idx_id ON Test (id)");
    test!(
        Ok(Payload::CreateIndex),
        "CREATE INDEX idx_name ON Test (name)"
    );
    test!(
        Ok(Payload::CreateIndex),
        "CREATE INDEX idx_id2 ON Test (id + num)"
    );

    use Value::*;

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            4     7     "Job".to_owned();
            11    7     "Great".to_owned()
        )),
        "SELECT id, num, name FROM Test WHERE id < 20"
    );

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            4     7     "Job".to_owned();
            11    7     "Great".to_owned()
        )),
        "SELECT id, num, name FROM Test WHERE 20 > id"
    );

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            4     7     "Job".to_owned()
        )),
        "SELECT id, num, name FROM Test WHERE id <= 4"
    );

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            4     7     "Job".to_owned()
        )),
        "SELECT id, num, name FROM Test WHERE 4 >= id"
    );

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            4     7     "Job".to_owned();
            11    7     "Great".to_owned()
        )),
        "SELECT id, num, name FROM Test WHERE id >= 4"
    );

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            4     7     "Job".to_owned();
            11    7     "Great".to_owned()
        )),
        "SELECT id, num, name FROM Test WHERE 4 <= id"
    );

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            11    7     "Great".to_owned()
        )),
        "SELECT id, num, name FROM Test WHERE id > 4"
    );

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            11    7     "Great".to_owned()
        )),
        "SELECT id, num, name FROM Test WHERE 4 < id"
    );

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned()
        )),
        "SELECT id, num, name FROM Test WHERE id = 1"
    );

    test!(
        Ok(Payload::Insert(1)),
        "INSERT INTO Test (id, num, name) VALUES (1, 30, \"New one\")"
    );

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     17    "World".to_owned();
            1     30    "New one".to_owned()
        )),
        "SELECT id, num, name FROM Test WHERE 1 = id"
    );

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     30    "New one".to_owned()
        )),
        r#"SELECT id, num, name FROM Test WHERE name = "New one""#
    );

    test!(
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "num".to_owned(), "name".to_owned()],
            rows: vec![]
        }),
        "SELECT id, num, name FROM Test WHERE id + num = 10"
    );

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        "SELECT id, num, name FROM Test WHERE id + num < 11"
    );

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        "SELECT id, num, name FROM Test WHERE 11 > id + num"
    );

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     17    "World".to_owned();
            11    7     "Great".to_owned()
        )),
        "SELECT id, num, name FROM Test WHERE id + num = 18"
    );

    test!(Ok(Payload::Delete(1)), "DELETE FROM Test WHERE id = 11");
    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        "SELECT id, num, name FROM Test WHERE id + num = 3"
    );

    test!(
        Ok(Payload::Update(3)),
        "UPDATE Test SET id = id + 1 WHERE id = 1;"
    );

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            2     17    "World".to_owned()
        )),
        "SELECT * FROM Test WHERE 19 = id + num"
    );

    test!(Ok(Payload::DropIndex), "DROP INDEX Test.idx_id2;");
    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            2     17    "World".to_owned()
        )),
        "SELECT * FROM Test WHERE id + num = 19"
    );

    test!(
        Ok(Payload::Select {
            labels: vec!["id".to_owned()],
            rows: vec![],
        }),
        "SELECT id FROM Test WHERE id + num = id"
    );

    test!(
        Err(TranslateError::CompositeIndexNotSupported.into()),
        "CREATE INDEX idx_com ON Test (id, num)"
    );

    test!(
        Err(TranslateError::TooManyParamsInDropIndex.into()),
        "DROP INDEX Test.idx_id, Test.idx_id2"
    );

    test!(
        Err(EvaluateError::UnsupportedStatelessExpr(format!(
            "{:#?}",
            ast::Expr::CompoundIdentifier(vec!["a".to_owned(), "b".to_owned()])
        ))
        .into()),
        "CREATE INDEX idx_wow On Test (a.b)"
    );

    test!(
        Err(IndexError::TableNotFound("Abc".to_owned()).into()),
        "CREATE INDEX idx_wow ON Abc (name)"
    );

    test!(
        Err(IndexError::IndexNameAlreadyExists("idx_name".to_owned()).into()),
        "CREATE INDEX idx_name ON Test (name || id)"
    );

    test!(
        Err(IndexError::IndexNameDoesNotExist("idx_aaa".to_owned()).into()),
        "DROP INDEX Test.idx_aaa"
    );
});
