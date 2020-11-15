//! Tests in this file are only executed when feature = "alter-table" is enabled.

use crate::*;
use Value::*;

test_case!(rename, async move {
    let test_cases = vec![
        ("CREATE TABLE Foo (id INTEGER);", Ok(Payload::Create)),
        (
            "INSERT INTO Foo VALUES (1), (2), (3);",
            Ok(Payload::Insert(3)),
        ),
        ("SELECT id FROM Foo", Ok(select!(id; I64; 1; 2; 3))),
        (
            "ALTER TABLE Foo2 RENAME TO Bar;",
            Err(AlterTableError::TableNotFound("Foo2".to_owned()).into()),
        ),
        ("ALTER TABLE Foo RENAME TO Bar;", Ok(Payload::AlterTable)),
        ("SELECT id FROM Bar", Ok(select!(id; I64; 1; 2; 3))),
        (
            "ALTER TABLE Bar RENAME COLUMN id TO new_id",
            Ok(Payload::AlterTable),
        ),
        ("SELECT new_id FROM Bar", Ok(select!(new_id; I64; 1; 2; 3))),
        (
            "ALTER TABLE Bar RENAME COLUMN hello TO idid",
            Err(AlterTableError::RenamingColumnNotFound.into()),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});

test_case!(add_drop, async move {
    let test_cases = vec![
        ("CREATE TABLE Foo (id INTEGER);", Ok(Payload::Create)),
        ("INSERT INTO Foo VALUES (1), (2);", Ok(Payload::Insert(2))),
        ("SELECT * FROM Foo;", Ok(select!(id; I64; 1; 2))),
        (
            "ALTER TABLE Foo ADD COLUMN amount INTEGER",
            Err(AlterTableError::DefaultValueRequired("amount INT".to_owned()).into()),
        ),
        (
            "ALTER TABLE Foo ADD COLUMN id INTEGER",
            Err(AlterTableError::AddingColumnAlreadyExists("id".to_owned()).into()),
        ),
        (
            "ALTER TABLE Foo ADD COLUMN amount INTEGER DEFAULT 10",
            Ok(Payload::AlterTable),
        ),
        (
            "SELECT * FROM Foo;",
            Ok(select!(id | amount; I64 | I64; 1 10; 2 10)),
        ),
        (
            "ALTER TABLE Foo ADD COLUMN opt BOOLEAN NULL",
            Ok(Payload::AlterTable),
        ),
        (
            "SELECT * FROM Foo;",
            Ok(select!(
                id  | amount | opt
                I64 | I64    | OptBool;
                1     10       None;
                2     10       None
            )),
        ),
        (
            "ALTER TABLE Foo ADD COLUMN opt2 BOOLEAN NULL DEFAULT true",
            Ok(Payload::AlterTable),
        ),
        (
            "SELECT * FROM Foo;",
            Ok(select!(
                id  | amount | opt     | opt2
                I64 | I64    | OptBool | OptBool;
                1     10       None      Some(true);
                2     10       None      Some(true)
            )),
        ),
        (
            "ALTER TABLE Foo ADD COLUMN something INTEGER DEFAULT (SELECT id FROM Bar LIMIT 1)",
            Err(ValueError::ExprNotSupported("(SELECT id FROM Bar LIMIT 1)".to_owned()).into()),
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
            Ok(select!(
                id  | opt     | opt2
                I64 | OptBool | OptBool;
                1     None      Some(true);
                2     None      Some(true)
            )),
        ),
        (
            "ALTER TABLE Foo DROP COLUMN IF EXISTS opt2;",
            Ok(Payload::AlterTable),
        ),
        (
            "SELECT * FROM Foo;",
            Ok(select!(
                id  | opt
                I64 | OptBool;
                1     None;
                2     None
            )),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
