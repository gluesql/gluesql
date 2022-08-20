#![cfg(feature = "alter-table")]

use {
    crate::*,
    gluesql_core::{data::Value::*, prelude::Payload, store::AlterTableError},
};

test_case!(alter_table_rename, async move {
    let test_cases = [
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

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});

test_case!(alter_table_add_drop, async move {
    use gluesql_core::{
        ast::*, executor::AlterError, executor::EvaluateError, store::*, translate::TranslateError,
    };
    let test_cases = [
        ("CREATE TABLE Foo (id INTEGER);", Ok(Payload::Create)),
        ("INSERT INTO Foo VALUES (1), (2);", Ok(Payload::Insert(2))),
        ("SELECT * FROM Foo;", Ok(select!(id; I64; 1; 2))),
        (
            "ALTER TABLE Foo ADD COLUMN amount INTEGER",
            Err(AlterTableError::DefaultValueRequired(ColumnDef {
                name: "amount".to_owned(),
                data_type: DataType::Int,
                options: vec![],
            })
            .into()),
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
            Ok(select_with_null!(
                id     | amount  | opt;
                I64(1)   I64(10)   Null;
                I64(2)   I64(10)   Null
            )),
        ),
        (
            "ALTER TABLE Foo ADD COLUMN opt2 BOOLEAN NULL DEFAULT true",
            Ok(Payload::AlterTable),
        ),
        (
            "SELECT * FROM Foo;",
            Ok(select_with_null!(
                id     | amount  | opt  | opt2;
                I64(1)   I64(10)   Null   Bool(true);
                I64(2)   I64(10)   Null   Bool(true)
            )),
        ),
        (
            "ALTER TABLE Foo ADD COLUMN something INTEGER DEFAULT (SELECT id FROM Bar LIMIT 1)",
            Err(
                EvaluateError::UnsupportedStatelessExpr(expr!("(SELECT id FROM Bar LIMIT 1)"))
                    .into(),
            ),
        ),
        (
            "ALTER TABLE Foo ADD COLUMN something SOMEWHAT",
            Err(TranslateError::UnsupportedDataType("SOMEWHAT".to_owned()).into()),
        ),
        (
            "ALTER TABLE Foo ADD COLUMN something FLOAT UNIQUE",
            Err(AlterError::UnsupportedDataTypeForUniqueColumn(
                "something".to_owned(),
                DataType::Float,
            )
            .into()),
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
                id     | opt  | opt2;
                I64(1)   Null   Bool(true);
                I64(2)   Null   Bool(true)
            )),
        ),
        (
            "ALTER TABLE Foo DROP COLUMN IF EXISTS opt2;",
            Ok(Payload::AlterTable),
        ),
        (
            "SELECT * FROM Foo;",
            Ok(select_with_null!(
                id     | opt;
                I64(1)   Null;
                I64(2)   Null
            )),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
