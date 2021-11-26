use crate::*;
use Value::*;

test_case!(create_table, async move {
    use {
        executor::{AlterError, EvaluateError},
        prelude::Payload,
        translate::TranslateError,
    };
    let test_cases = vec![
        (
            r#"
        CREATE TABLE CreateTable1 (
            id INTEGER NULL,
            num INTEGER,
            name TEXT
        )"#,
            Ok(Payload::Create),
        ),
        (
            r#"
        CREATE TABLE CreateTable1 (
            id INTEGER NULL,
            num INTEGER,
            name TEXT
        )"#,
            Err(AlterError::TableAlreadyExists("CreateTable1".to_owned()).into()),
        ),
        (
            r#"
        CREATE TABLE IF NOT EXISTS CreateTable2 (
            id INTEGER NULL,
            num INTEGER,
            name TEXT
        )"#,
            Ok(Payload::Create),
        ),
        (
            r#"
        CREATE TABLE IF NOT EXISTS CreateTable2 (
            id2 INTEGER NULL,
        )"#,
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO CreateTable2 VALUES (NULL, 1, "1");"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "CREATE TABLE Gluery (id SOMEWHAT);",
            Err(TranslateError::UnsupportedDataType("SOMEWHAT".to_owned()).into()),
        ),
        (
            "CREATE TABLE Gluery (id BYTEA);",
            Err(TranslateError::UnsupportedDataType("BYTEA".to_owned()).into()),
        ),
        (
            "CREATE TABLE Gluery (id INTEGER CHECK (true));",
            Err(TranslateError::UnsupportedColumnOption("CHECK (true)".to_owned()).into()),
        ),
        (
            "CREATE TABLE Glue (id INTEGER PRIMARY KEY)",
            Err(TranslateError::UnsupportedColumnOption(("PRIMARY KEY").to_owned()).into()),
        ),
        (
            r#"
        CREATE TABLE CreateTable3 (
            id INTEGER,
            ratio FLOAT UNIQUE
        )"#,
            Err(AlterError::UnsupportedDataTypeForUniqueColumn(
                "ratio".to_owned(),
                ast::DataType::Float,
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
                Null     I64(1)   Str("1".to_owned())
            )),
        ),
        (
            // Target Table already exists
            "CREATE TABLE TargetTableWithData AS SELECT * FROM CreateTable2",
            Err(AlterError::TableAlreadyExists("TargetTableWithData".to_owned()).into()),
        ),
        (
            // source table does not exists
            "CREATE TABLE TargetTableWithData2 AS SELECT * FROM NonExistentTable",
            Err(SelectError::TableAliasNotFound("NonExistentTable".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
