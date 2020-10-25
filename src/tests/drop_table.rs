use crate::*;

pub fn drop_table(mut tester: impl tests::Tester) {
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
                id  | num | name
                I64 | I64 | Str;
                1     2     "Hello".to_owned()
            )),
        ),
        ("DROP TABLE DropTable;", Ok(Payload::DropTable)),
        (
            "DROP TABLE DropTable;",
            Err(ExecuteError::TableNotExists.into()),
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
            Err(StoreError::SchemaNotFound.into()),
        ),
        (create_sql, Ok(Payload::Create)),
        (
            "SELECT id, num, name FROM DropTable;",
            Ok(select!(id | num | name)),
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
