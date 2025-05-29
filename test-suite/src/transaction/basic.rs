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
        g.run(query).await;
    }

    g.test("BEGIN;", Ok(Payload::StartTransaction)).await;
    g.test(
        "INSERT INTO TxTest VALUES (3, 'New one');",
        Ok(Payload::Insert(1)),
    )
    .await;
    g.test("ROLLBACK;", Ok(Payload::Rollback)).await;
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        )),
    )
    .await;

    g.test("BEGIN;", Ok(Payload::StartTransaction)).await;
    g.test(
        "INSERT INTO TxTest VALUES (3, 'Vienna');",
        Ok(Payload::Insert(1)),
    )
    .await;
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned();
            3     "Vienna".to_owned()
        )),
    )
    .await;

    g.test("COMMIT;", Ok(Payload::Commit)).await;
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned();
            3     "Vienna".to_owned()
        )),
    )
    .await;

    // DELETE
    g.test("BEGIN;", Ok(Payload::StartTransaction)).await;
    g.test("DELETE FROM TxTest WHERE id = 3;", Ok(Payload::Delete(1)))
        .await;
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        )),
    )
    .await;
    g.test("ROLLBACK;", Ok(Payload::Rollback)).await;
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned();
            3     "Vienna".to_owned()
        )),
    )
    .await;
    g.test("BEGIN;", Ok(Payload::StartTransaction)).await;
    g.test("DELETE FROM TxTest WHERE id = 3;", Ok(Payload::Delete(1)))
        .await;
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        )),
    )
    .await;
    g.test("COMMIT;", Ok(Payload::Commit)).await;
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        )),
    )
    .await;

    // UPDATE
    g.test("BEGIN;", Ok(Payload::StartTransaction)).await;
    g.test(
        "UPDATE TxTest SET name = 'Sunday' WHERE id = 1;",
        Ok(Payload::Update(1)),
    )
    .await;
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Sunday".to_owned();
            2     "Phone".to_owned()
        )),
    )
    .await;
    g.test("ROLLBACK;", Ok(Payload::Rollback)).await;
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        )),
    )
    .await;
    g.test("BEGIN;", Ok(Payload::StartTransaction)).await;
    g.test(
        "UPDATE TxTest SET name = 'Sunday' WHERE id = 1;",
        Ok(Payload::Update(1)),
    )
    .await;
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Sunday".to_owned();
            2     "Phone".to_owned()
        )),
    )
    .await;
    g.test("COMMIT;", Ok(Payload::Commit)).await;
    g.test(
        "SELECT id, name FROM TxTest",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Sunday".to_owned();
            2     "Phone".to_owned()
        )),
    )
    .await;

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
        g.run(query).await;
    }
});
