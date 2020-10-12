use crate::*;

pub fn migrate(mut tester: impl tests::Tester) {
    let sql = r#"
CREATE TABLE Test (
    id INT,
    num INT,
    name TEXT
)"#;

    tester.run(sql).unwrap();

    let sqls = [
        "INSERT INTO Test (id, num, name) VALUES (1, 2, \"Hello\")",
        "INSERT INTO Test (id, num, name) VALUES (1, 9, \"World\")",
        "INSERT INTO Test (id, num, name) VALUES (3, 4, \"Great\")",
    ];

    sqls.iter().for_each(|sql| {
        tester.run(sql).unwrap();
    });

    let error_cases = vec![
        (
            ValueError::ExprNotSupported("3 * 2".to_owned()).into(),
            "INSERT INTO Test (id, num) VALUES (3 * 2, 1);",
        ),
        (
            ValueError::FailedToParseNumber.into(),
            "INSERT INTO Test (id, num) VALUES (1.1, 1);",
        ),
        (
            EvaluateError::UnsupportedCompoundIdentifier("Here.User.id".to_owned()).into(),
            "SELECT * FROM Test WHERE Here.User.id = 1",
        ),
        (
            JoinError::NaturalOnJoinNotSupported.into(),
            "SELECT * FROM Test NATURAL JOIN Test",
        ),
        (
            TableError::TableFactorNotSupported.into(),
            "SELECT * FROM (SELECT * FROM Test) as A;",
        ),
    ];

    error_cases.into_iter().for_each(|(error, sql)| {
        let error = Err(error);

        assert_eq!(error, tester.run(sql));
    });

    use Value::*;

    let found = tester
        .run("SELECT id, num, name FROM Test")
        .expect("select");
    let expected = select!(
        I64 I64 Str;
        1   2   "Hello".to_owned();
        1   9   "World".to_owned();
        3   4   "Great".to_owned()
    );
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT id, num, name FROM Test WHERE id = 1")
        .expect("select");
    let expected = select!(
        I64 I64 Str;
        1   2   "Hello".to_owned();
        1   9   "World".to_owned()
    );
    assert_eq!(expected, found);

    tester.run_and_print("UPDATE Test SET id = 2");

    let found = tester
        .run("SELECT id, num, name FROM Test")
        .expect("select");
    let expected = select!(
        I64 I64 Str;
        2   2   "Hello".to_owned();
        2   9   "World".to_owned();
        2   4   "Great".to_owned()
    );
    assert_eq!(expected, found);

    let found = tester.run("SELECT id FROM Test").expect("select");
    let expected = select!(I64; 2; 2; 2);
    assert_eq!(expected, found);

    let found = tester.run("SELECT id, num FROM Test").expect("select");
    let expected = select!(I64 I64; 2 2; 2 9; 2 4);
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT id, num FROM Test LIMIT 1 OFFSET 1")
        .expect("select");
    let expected = select!(I64 I64; 2 9);
    assert_eq!(expected, found);
}
