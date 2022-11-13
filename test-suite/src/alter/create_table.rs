use {
    crate::*,
    gluesql_core::{
        data::value::Value::{Null, Str, I64},
        executor::{AlterError, EvaluateError},
        prelude::Payload,
        translate::TranslateError,
    },
};

test_case!(create_table, async move {
    let test_cases = [
        (
            "
        CREATE TABLE CreateTable1 (
            id INTEGER NULL,
            num INTEGER,
            name TEXT
        )",
            Ok(Payload::Create),
        ),
        (
            "
        CREATE TABLE CreateTable1 (
            id INTEGER NULL,
            num INTEGER,
            name TEXT
        )",
            Err(AlterError::TableAlreadyExists("CreateTable1".to_owned()).into()),
        ),
        (
            "
        CREATE TABLE IF NOT EXISTS CreateTable2 (
            id INTEGER NULL,
            num INTEGER,
            name TEXT
        )",
            Ok(Payload::Create),
        ),
        (
            "
        CREATE TABLE IF NOT EXISTS CreateTable2 (
            id2 INTEGER NULL,
        )",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO CreateTable2 VALUES (NULL, 1, '1');",
            Ok(Payload::Insert(1)),
        ),
        (
            "INSERT INTO CreateTable2 VALUES (2, 2, '2');",
            Ok(Payload::Insert(1)),
        ),
        (
            "CREATE TABLE Gluery (id SOMEWHAT);",
            Err(TranslateError::UnsupportedDataType("SOMEWHAT".to_owned()).into()),
        ),
        (
            "CREATE TABLE Gluery (id GLOBE);",
            Err(TranslateError::UnsupportedDataType("GLOBE".to_owned()).into()),
        ),
        (
            "CREATE TABLE Gluery (id INTEGER CHECK (true));",
            Err(TranslateError::UnsupportedColumnOption("CHECK (true)".to_owned()).into()),
        ),
        (
            "
        CREATE TABLE CreateTable3 (
            id INTEGER,
            ratio FLOAT UNIQUE
        )",
            Err(AlterError::UnsupportedDataTypeForUniqueColumn(
                "ratio".to_owned(),
                gluesql_core::ast::DataType::Float,
            )
            .into()),
        ),
        (
            "CREATE TABLE Gluery (id INTEGER DEFAULT (SELECT id FROM Wow))",
            Err(EvaluateError::UnsupportedStatelessExpr(expr!("(SELECT id FROM Wow)")).into()),
        ),
        (
            // Create schema only
            "CREATE TABLE TargetTable AS SELECT * FROM CreateTable2 WHERE 1 = 0",
            Ok(Payload::Create),
        ),
        (
            "CREATE TABLE TargetTableWithData AS SELECT * FROM CreateTable2",
            Ok(Payload::Create),
        ),
        (
            "SELECT * FROM TargetTableWithData",
            Ok(select_with_null!(
                id     | num    | name;
                Null     I64(1)   Str("1".to_owned());
                I64(2)   I64(2)   Str("2".to_owned())
            )),
        ),
        (
            "CREATE TABLE TargetTableWithLimit AS SELECT * FROM CreateTable2 LIMIT 1",
            Ok(Payload::Create),
        ),
        (
            "SELECT * FROM TargetTableWithLimit",
            Ok(select_with_null!(
                id     | num    | name;
                Null     I64(1)   Str("1".to_owned())
            )),
        ),
        (
            "CREATE TABLE TargetTableWithOffset AS SELECT * FROM CreateTable2 OFFSET 1",
            Ok(Payload::Create),
        ),
        (
            "SELECT * FROM TargetTableWithOffset",
            Ok(select_with_null!(
                id     | num    | name;
                I64(2)   I64(2)   Str("2".to_owned())
            )),
        ),
        (
            // Target Table already exists
            "CREATE TABLE TargetTableWithData AS SELECT * FROM CreateTable2",
            Err(AlterError::TableAlreadyExists("TargetTableWithData".to_owned()).into()),
        ),
        (
            // Source table does not exists
            "CREATE TABLE TargetTableWithData2 AS SELECT * FROM NonExistentTable",
            Err(AlterError::CtasSourceTableNotFound("NonExistentTable".to_owned()).into()),
        ),
        (
            // Cannot create table with duplicate column name
            "CREATE TABLE DuplicateColumns (id INT, id INT)",
            Err(AlterError::DuplicateColumnName("id".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
