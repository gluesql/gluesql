use crate::*;

test_case!(create_table, async move {
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
            "CREATE TABLE Gluery (id INTEGER CHECK (true));",
            Err(TranslateError::UnsupportedColumnOption("CHECK (true)".to_owned()).into()),
        ),
        (
            r#"
        CREATE TABLE CreateTable3 (
            id INTEGER,
            ratio FLOAT UNIQUE
        )"#,
            Err(AlterError::UnsupportedDataTypeForUniqueColumn(
                "ratio".to_owned(),
                format!("{:?}", ast::DataType::Float),
            )
            .into()),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
