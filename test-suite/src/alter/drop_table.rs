use {
    crate::*,
    gluesql_core::{
        error::{AlterError, FetchError, TranslateError},
        prelude::{Payload, Value::*},
    },
};

test_case!(drop_table, {
    let g = get_tester!();

    let create_sql = "
CREATE TABLE DropTable (
    id INT,
    num INT,
    name TEXT
)";

    g.run(create_sql).await;

    let sqls = ["INSERT INTO DropTable (id, num, name) VALUES (1, 2, 'Hello')"];

    for sql in sqls {
        g.run(sql).await;
    }

    let sqls = [
        (
            "SELECT id, num, name FROM DropTable;",
            Ok(select!(
                id  | num | name
                I64 | I64 | Str;
                1     2     "Hello".to_owned()
            )),
        ),
        ("DROP TABLE DropTable;", Ok(Payload::DropTable(1))),
        (
            "DROP TABLE DropTable;",
            Err(AlterError::TableNotFound("DropTable".to_owned()).into()),
        ),
        (
            "
CREATE TABLE DropTable (
    id INT,
    num INT,
    name TEXT
)",
            Ok(Payload::Create),
        ),
        ("DROP TABLE IF EXISTS DropTable;", Ok(Payload::DropTable(1))),
        ("DROP TABLE IF EXISTS DropTable;", Ok(Payload::DropTable(0))),
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
            "
        CREATE TABLE DropTable1 (
            id INT,
            num INT,
            name TEXT
        )",
            Ok(Payload::Create),
        ),
        (
            "
        CREATE TABLE DropTable2 (
            id INT,
            num INT,
            name TEXT
        )",
            Ok(Payload::Create),
        ),
        (
            "DROP TABLE DropTable1, DropTable2;",
            Ok(Payload::DropTable(2)),
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
            "
        CREATE TABLE DropTable1 (
            id INT,
            num INT,
            name TEXT
        )",
            Ok(Payload::Create),
        ),
        (
            "
        CREATE TABLE DropTable2 (
            id INT,
            num INT,
            name TEXT
        )",
            Ok(Payload::Create),
        ),
        (
            "DROP TABLE IF EXISTS DropTable1, DropTable2;",
            Ok(Payload::DropTable(2)),
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
            "
        CREATE TABLE DropTable1 (
            id INT,
            num INT,
            name TEXT
        )",
            Ok(Payload::Create),
        ),
        (
            "DROP TABLE IF EXISTS DropTable1, DropTable2;",
            Ok(Payload::DropTable(1)),
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
        g.test(sql, expected).await;
    }
});
