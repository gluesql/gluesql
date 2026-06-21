use {crate::*, Value::*, gluesql_core::prelude::*};

test_case!(basic, {
    let g = get_tester!();

    for query in [
        "
        CREATE TABLE TxTest (
            id INTEGER,
            name TEXT
        );
    ",
        "
        INSERT INTO TxTest VALUES
            (1, 'Friday'),
            (2, 'Phone');
    ",
    ] {
        g.run(query);
    }

    g.test("BEGIN;", Ok(Payload::StartTransaction));
    g.test(
        "INSERT INTO TxTest VALUES (3, 'New one');",
        Ok(Payload::Insert(1)),
    );
    g.test("ROLLBACK;", Ok(Payload::Rollback));
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        )),
    );

    g.test("BEGIN;", Ok(Payload::StartTransaction));
    g.test(
        "INSERT INTO TxTest VALUES (3, 'Vienna');",
        Ok(Payload::Insert(1)),
    );
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned();
            3     "Vienna".to_owned()
        )),
    );

    g.test("COMMIT;", Ok(Payload::Commit));
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned();
            3     "Vienna".to_owned()
        )),
    );

    // DELETE
    g.test("BEGIN;", Ok(Payload::StartTransaction));
    g.test("DELETE FROM TxTest WHERE id = 3;", Ok(Payload::Delete(1)));
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        )),
    );
    g.test("ROLLBACK;", Ok(Payload::Rollback));
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned();
            3     "Vienna".to_owned()
        )),
    );
    g.test("BEGIN;", Ok(Payload::StartTransaction));
    g.test("DELETE FROM TxTest WHERE id = 3;", Ok(Payload::Delete(1)));
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        )),
    );
    g.test("COMMIT;", Ok(Payload::Commit));
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        )),
    );

    // UPDATE
    g.test("BEGIN;", Ok(Payload::StartTransaction));
    g.test(
        "UPDATE TxTest SET name = 'Sunday' WHERE id = 1;",
        Ok(Payload::Update(1)),
    );
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Sunday".to_owned();
            2     "Phone".to_owned()
        )),
    );
    g.test("ROLLBACK;", Ok(Payload::Rollback));
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        )),
    );
    g.test("BEGIN;", Ok(Payload::StartTransaction));
    g.test(
        "UPDATE TxTest SET name = 'Sunday' WHERE id = 1;",
        Ok(Payload::Update(1)),
    );
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Sunday".to_owned();
            2     "Phone".to_owned()
        )),
    );
    g.test("COMMIT;", Ok(Payload::Commit));
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Sunday".to_owned();
            2     "Phone".to_owned()
        )),
    );

    for query in [
        "BEGIN;",
        "SELECT * FROM TxTest;",
        "ROLLBACK;",
        "BEGIN;",
        "SELECT * FROM TxTest;",
        "COMMIT;",
        "BEGIN;",
        "COMMIT;",
    ] {
        g.run(query);
    }
});
