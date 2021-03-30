use crate::*;
use Value::*;

test_case!(async move {
    let test_cases = vec![
        ("CREATE TABLE Foo (id INTEGER, something TEXT, amount FLOAT NULL DEFAULT NULL, bools BOOLEAN DEFAULT true);", Ok(Payload::Create)),
        ("INSERT INTO Foo (id, something) VALUES (1, 'b'), (2, 'c');", Ok(Payload::Insert(2))),
        (
            "ALTER TABLE Foo DROP COLUMN IF EXISTS something;",
            Ok(Payload::AlterTable),
        ),
        (
            "ALTER TABLE Foo DROP COLUMN IF EXISTS something;",
            Ok(Payload::AlterTable),
        ),
        (
            "ALTER TABLE Foo DROP COLUMN something;",
            Err(AlterTableError::DroppingColumnNotFound("something".to_owned()).into()),
        ),
        (
            "ALTER TABLE Foo DROP COLUMN amount;",
            Ok(Payload::AlterTable),
        ),
        (
            "SELECT * FROM Foo;",
            Ok(select_with_null!(
                id   | bools;
                I64(1) Bool(true);
                I64(2) Bool(true)
            )),
        ),
        (
            "ALTER TABLE Foo DROP COLUMN IF EXISTS bools;",
            Ok(Payload::AlterTable),
        ),
        (
            "SELECT * FROM Foo;",
            Ok(select_with_null!(
                id;
                I64(1);
                I64(2)
            )),
        ),
        (
            "ALTER TABLE Foo DROP COLUMN IF EXISTS bools;",
            Ok(Payload::AlterTable),
        ),
        (
            "SELECT * FROM Foo;",
            Ok(select_with_null!(
                id;
                I64(1);
                I64(2)
            )),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
