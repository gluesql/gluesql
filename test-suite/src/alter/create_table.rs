use {
    crate::*,
    gluesql_core::{
        data::{
            value::Value::{Null, Str, I64},
            Literal, ValueError,
        },
        error::{AlterError, EvaluateError, TranslateError},
        executor::FetchError,
        prelude::{DataType::Int, Payload, Value},
    },
    serde_json::json,
    std::borrow::Cow,
};

test_case!(create_table, {
    let g = get_tester!();

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
            Err(EvaluateError::UnsupportedStatelessExpr(expr("(SELECT id FROM Wow)")).into()),
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
            "CREATE TABLE TargetTableWithLiteral AS SELECT num, 'literal' as literal_col FROM CreateTable2",
            Ok(Payload::Create),
        ),
        (
            "SELECT * FROM TargetTableWithLiteral",
            Ok(select_with_null!(
                num    | "literal_col";
                I64(1)   Str("literal".to_owned());
                I64(2)   Str("literal".to_owned())
            )),
        ),
        (
            "CREATE TABLE Schemaless",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO Schemaless VALUES ('{"id": 1, "name": "Glue"}'), ('{"id": 2, "name": "SQL"}')"#,
            Ok(Payload::Insert(2)),
        ),
        (
            "CREATE TABLE TargetTableFromSchemaless AS SELECT * FROM Schemaless",
            Ok(Payload::Create),
        ),
        (
            "SELECT * FROM TargetTableFromSchemaless",
            Ok(select_map![
                json!({"name": "Glue", "id": 1}),
                json!({"name": "SQL", "id": 2})
            ]),
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
            "CREATE TABLE IncompatibleDataTypeCtasWithLiteral AS VALUES (1), ('b')",
            Err(ValueError::IncompatibleLiteralForDataType{
                data_type: Int,
                literal: format!("{:?}", Literal::Text(Cow::Owned("b".to_owned()))),
            }.into()),
        ),
        (
            "CREATE TABLE IncompatibleDataTypeCtasWithValue AS SELECT CASE ID WHEN 1 THEN 1 ELSE 'b' END AS wrongColumn FROM (VALUES (1), (2)) AS SUB (ID)",
            Err(ValueError::IncompatibleDataType{
                data_type: Int,
                value: Value::Str("b".to_owned())
            }.into()),
        ),
        (
            "SELECT COUNT(*) FROM IncompatibleDataTypeCtasWithValue",
            Err(FetchError::TableNotFound("IncompatibleDataTypeCtasWithValue".to_owned()).into()),
        ),
        (
            // Cannot create table with duplicate column name
            "CREATE TABLE DuplicateColumns (id INT, id INT)",
            Err(AlterError::DuplicateColumnName("id".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
