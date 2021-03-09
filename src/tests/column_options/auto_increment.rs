use crate::*;

test_case!(auto_increment, async move {
    use Value::*;

    let test_cases = vec![
        (
            "CREATE TABLE Test (id INTEGER AUTO_INCREMENT NOT NULL, name TEXT)",
            Payload::Create,
        ),
        (
            r#"INSERT INTO Test (name) VALUES ('test1')"#,
            Payload::Insert(1),
        ),
        (
            r#"SELECT * FROM Test"#,
            select!(
            id  | name
            I64 | Str;
            1    "test1".to_owned()),
        ),
        (
            r#"INSERT INTO Test (name) VALUES ('test2')"#,
            Payload::Insert(1),
        ),
        (
            r#"SELECT * FROM Test"#,
            select!(
            id  | name
            I64 | Str;
            1    "test1".to_owned();
            2    "test2".to_owned()),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(Ok(expected), sql);
    }
});
