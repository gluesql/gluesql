use {crate::*, gluesql_core::prelude::*, Value::*};

test_case!(basic, async move {
    run!(
        "
        CREATE TABLE TxTest (
            id INTEGER,
            name TEXT
        );
    "
    );
    run!(
        "
        INSERT INTO TxTest VALUES
            (1, 'Friday'),
            (2, 'Phone');
    "
    );

    test!("BEGIN;", Ok(Payload::StartTransaction));
    test!(
        "INSERT INTO TxTest VALUES (3, 'New one');",
        Ok(Payload::Insert(1))
    );
    test!("ROLLBACK;", Ok(Payload::Rollback));
    test!(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        ))
    );

    test!("BEGIN;", Ok(Payload::StartTransaction));
    test!(
        "INSERT INTO TxTest VALUES (3, 'Vienna');",
        Ok(Payload::Insert(1))
    );
    test!(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned();
            3     "Vienna".to_owned()
        ))
    );

    test!("COMMIT;", Ok(Payload::Commit));
    test!(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned();
            3     "Vienna".to_owned()
        ))
    );

    // DELETE
    test!("BEGIN;", Ok(Payload::StartTransaction));
    test!("DELETE FROM TxTest WHERE id = 3;", Ok(Payload::Delete(1)));
    test!(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        ))
    );
    test!("ROLLBACK;", Ok(Payload::Rollback));
    test!(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned();
            3     "Vienna".to_owned()
        ))
    );
    test!("BEGIN;", Ok(Payload::StartTransaction));
    test!("DELETE FROM TxTest WHERE id = 3;", Ok(Payload::Delete(1)));
    test!(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        ))
    );
    test!("COMMIT;", Ok(Payload::Commit));
    test!(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        ))
    );

    // UPDATE
    test!("BEGIN;", Ok(Payload::StartTransaction));
    test!(
        "UPDATE TxTest SET name = 'Sunday' WHERE id = 1;",
        Ok(Payload::Update(1))
    );
    test!(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Sunday".to_owned();
            2     "Phone".to_owned()
        ))
    );
    test!("ROLLBACK;", Ok(Payload::Rollback));
    test!(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        ))
    );
    test!("BEGIN;", Ok(Payload::StartTransaction));
    test!(
        "UPDATE TxTest SET name = 'Sunday' WHERE id = 1;",
        Ok(Payload::Update(1))
    );
    test!(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Sunday".to_owned();
            2     "Phone".to_owned()
        ))
    );
    test!("COMMIT;", Ok(Payload::Commit));
    test!(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Sunday".to_owned();
            2     "Phone".to_owned()
        ))
    );

    run!("BEGIN;");
    run!("SELECT * FROM TxTest;");
    run!("ROLLBACK;");

    run!("BEGIN;");
    run!("SELECT * FROM TxTest;");
    run!("COMMIT;");

    run!("BEGIN;");
    run!("COMMIT;");
});
