mod helper;

use gluesql::{ExecuteError, Payload, Row, StoreError, Tester, Value};
use sled_storage::SledTester;

#[test]
fn drop_table() {
    let tester = SledTester::new("data/migrate");

    let create_sql = r#"
CREATE TABLE DropTable (
    id INT,
    num INT,
    name TEXT
)"#;

    tester.run(create_sql).unwrap();

    let sqls = ["INSERT INTO DropTable (id, num, name) VALUES (1, 2, \"Hello\")"];

    sqls.iter().for_each(|sql| {
        tester.run(sql).unwrap();
    });

    use Value::*;

    let sqls = vec![
        (
            "SELECT id, num, name FROM DropTable;",
            Ok(select!(
                I64 I64 Str;
                1   2   "Hello".to_owned()
            )),
        ),
        ("DROP TABLE DropTable;", Ok(Payload::DropTable)),
        (
            "SELECT id, num, name FROM DropTable;",
            Err(StoreError::SchemaNotFound.into()),
        ),
        (create_sql, Ok(Payload::Create)),
        (
            "SELECT id, num, name FROM DropTable;",
            Ok(Payload::Select(vec![])),
        ),
        (
            "DROP VIEW DropTable;",
            Err(ExecuteError::DropTypeNotSupported.into()),
        ),
    ];

    sqls.into_iter().for_each(|(sql, expected)| {
        let found = tester.run(sql);

        assert_eq!(expected, found);
    });
}
