use {
    crate::*,
    gluesql_core::{
        executor::{AlterError, FetchError, Payload},
        translate::TranslateError,
    },
};

test_case!(drop_table, async move {
    let create_sql = r#"
CREATE TABLE DropTable (
    id INT,
    num INT,
    name TEXT
)"#;

    run!(create_sql);

    let sqls = ["INSERT INTO DropTable (id, num, name) VALUES (1, 2, \"Hello\")"];

    for sql in sqls.iter() {
        run!(sql);
    }

    use gluesql_core::prelude::Value::*;

    let sqls = vec![
        (
            "SELECT id, num, name FROM DropTable;",
            Ok(select!(
                id  | num | name
                I64 | I64 | Str;
                1     2     "Hello".to_owned()
            )),
        ),
        ("DROP TABLE DropTable;", Ok(Payload::DropTable)),
        (
            "DROP TABLE DropTable;",
            Err(AlterError::TableNotFound("DropTable".to_owned()).into()),
        ),
        (
            r#"
CREATE TABLE DropTable (
    id INT,
    num INT,
    name TEXT
)"#,
            Ok(Payload::Create),
        ),
        ("DROP TABLE IF EXISTS DropTable;", Ok(Payload::DropTable)),
        ("DROP TABLE IF EXISTS DropTable;", Ok(Payload::DropTable)),
        (
            "SELECT id, num, name FROM DropTable;",
            Err(FetchError::TableNotFound("DropTable".to_owned()).into()),
        ),
        (create_sql, Ok(Payload::Create)),
        (
            "SELECT id, num, name FROM DropTable;",
            Ok(select!(id | num | name)),
        ),
        (
            "DROP VIEW DropTable;",
            Err(TranslateError::UnsupportedStatement("DROP VIEW DropTable".to_owned()).into()),
        ),
        (
            r#"
        CREATE TABLE DropTable1 (
            id INT,
            num INT,
            name TEXT
        )"#,
            Ok(Payload::Create),
        ),
        (
            r#"
        CREATE TABLE DropTable2 (
            id INT,
            num INT,
            name TEXT
        )"#,
            Ok(Payload::Create),
        ),
        ("DROP TABLE DropTable1, DropTable2;", Ok(Payload::DropTable)),
        (
            "SELECT id, num, name FROM DropTable1;",
            Err(FetchError::TableNotFound("DropTable1".to_owned()).into()),
        ),
        (
            "SELECT id, num, name FROM DropTable2;",
            Err(FetchError::TableNotFound("DropTable2".to_owned()).into()),
        ),
        (
            r#"
        CREATE TABLE DropTable1 (
            id INT,
            num INT,
            name TEXT
        )"#,
            Ok(Payload::Create),
        ),
        (
            r#"
        CREATE TABLE DropTable2 (
            id INT,
            num INT,
            name TEXT
        )"#,
            Ok(Payload::Create),
        ),
        (
            "DROP TABLE IF EXISTS DropTable1, DropTable2;",
            Ok(Payload::DropTable),
        ),
        (
            "SELECT id, num, name FROM DropTable1;",
            Err(FetchError::TableNotFound("DropTable1".to_owned()).into()),
        ),
        (
            "SELECT id, num, name FROM DropTable2;",
            Err(FetchError::TableNotFound("DropTable2".to_owned()).into()),
        ),
        (
            r#"
        CREATE TABLE DropTable1 (
            id INT,
            num INT,
            name TEXT
        )"#,
            Ok(Payload::Create),
        ),
        (
            "DROP TABLE IF EXISTS DropTable1, DropTable2;",
            Ok(Payload::DropTable),
        ),
        (
            "SELECT id, num, name FROM DropTable1;",
            Err(FetchError::TableNotFound("DropTable1".to_owned()).into()),
        ),
        (
            "SELECT id, num, name FROM DropTable2;",
            Err(FetchError::TableNotFound("DropTable2".to_owned()).into()),
        ),
    ];

    for (sql, expected) in sqls {
        test!(expected, sql);
    }
});
