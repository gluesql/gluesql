#![cfg(feature = "transaction")]

use crate::*;
use prelude::*;

test_case!(basic, async move {
    use Value::*;

    run!(
        "
        CREATE TABLE TxTest (
            id INTEGER,
            name TEXT
        );
    "
    );
    run!(
        r#"
        INSERT INTO TxTest VALUES
            (1, "Friday"),
            (2, "Phone");
    "#
    );

    test!(Ok(Payload::StartTransaction), "BEGIN;");
    test!(
        Ok(Payload::Insert(1)),
        r#"INSERT INTO TxTest VALUES (3, "New one");"#
    );
    test!(Ok(Payload::Rollback), "ROLLBACK;");
    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        )),
        "SELECT id, name FROM TxTest"
    );

    test!(Ok(Payload::StartTransaction), "BEGIN;");
    test!(
        Ok(Payload::Insert(1)),
        r#"INSERT INTO TxTest VALUES (3, "Vienna");"#
    );
    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned();
            3     "Vienna".to_owned()
        )),
        "SELECT id, name FROM TxTest"
    );

    test!(Ok(Payload::Commit), "COMMIT;");
    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned();
            3     "Vienna".to_owned()
        )),
        "SELECT id, name FROM TxTest"
    );

    // DELETE
    test!(Ok(Payload::StartTransaction), "BEGIN;");
    test!(Ok(Payload::Delete(1)), "DELETE FROM TxTest WHERE id = 3;");
    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        )),
        "SELECT id, name FROM TxTest"
    );
    test!(Ok(Payload::Rollback), "ROLLBACK;");
    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned();
            3     "Vienna".to_owned()
        )),
        "SELECT id, name FROM TxTest"
    );
    test!(Ok(Payload::StartTransaction), "BEGIN;");
    test!(Ok(Payload::Delete(1)), "DELETE FROM TxTest WHERE id = 3;");
    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        )),
        "SELECT id, name FROM TxTest"
    );
    test!(Ok(Payload::Commit), "COMMIT;");
    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        )),
        "SELECT id, name FROM TxTest"
    );

    // UPDATE
    test!(Ok(Payload::StartTransaction), "BEGIN;");
    test!(
        Ok(Payload::Update(1)),
        r#"UPDATE TxTest SET name = "Sunday" WHERE id = 1;"#
    );
    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Sunday".to_owned();
            2     "Phone".to_owned()
        )),
        "SELECT id, name FROM TxTest"
    );
    test!(Ok(Payload::Rollback), "ROLLBACK;");
    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Friday".to_owned();
            2     "Phone".to_owned()
        )),
        "SELECT id, name FROM TxTest"
    );
    test!(Ok(Payload::StartTransaction), "BEGIN;");
    test!(
        Ok(Payload::Update(1)),
        r#"UPDATE TxTest SET name = "Sunday" WHERE id = 1;"#
    );
    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Sunday".to_owned();
            2     "Phone".to_owned()
        )),
        "SELECT id, name FROM TxTest"
    );
    test!(Ok(Payload::Commit), "COMMIT;");
    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            1     "Sunday".to_owned();
            2     "Phone".to_owned()
        )),
        "SELECT id, name FROM TxTest"
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
