mod helper;

use gluesql::{ExecuteError, Payload, Row, StoreError, Value};
use helper::{Helper, SledHelper};

#[test]
fn drop_table() {
    let helper = SledHelper::new("data/migrate");

    let create_sql = r#"
CREATE TABLE DropTable (
    id INT,
    num INT,
    name TEXT
)"#;

    helper.run(create_sql).unwrap();

    let sqls = ["INSERT INTO DropTable (id, num, name) VALUES (1, 2, \"Hello\")"];

    sqls.iter().for_each(|sql| {
        helper.run(sql).unwrap();
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
        let found = helper.run(sql);

        assert_eq!(expected, found);
    });
}
