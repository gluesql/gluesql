use crate::*;

test_case!(migrate, async move {
    use gluesql_core::{
        data::ValueError, executor::EvaluateError, prelude::Value::*, translate::TranslateError, prelude::Payload,
    };

    run!(
        "
        CREATE TABLE Test (
            id INT,
            num INT,
            name TEXT
        );
    "
    );
    run!(
        r#"
        INSERT INTO Test (id, num, name) VALUES
            (1,     2,     "Hello"),
            (-(-1), 9,     "World"),
            (+3,    2 * 2, "Great");
        "#
    );

    let error_cases = vec![
        (
            ValueError::FailedToParseNumber.into(),
            r#"INSERT INTO Test (id, num, name) VALUES (1.1, 1, "good");"#,
        ),
        (
            EvaluateError::UnsupportedStatelessExpr(expr!("a.b")).into(),
            "INSERT INTO Test (id, num, name) VALUES (1, 1, a.b);",
        ),
        (
            EvaluateError::UnsupportedCompoundIdentifier(expr!("Here.User.id")).into(),
            "SELECT * FROM Test WHERE Here.User.id = 1",
        ),
        (
            TranslateError::UnsupportedJoinConstraint("NATURAL".to_owned()).into(),
            "SELECT * FROM Test NATURAL JOIN Test",
        ),
        (
            TranslateError::UnsupportedQueryTableFactor("(SELECT * FROM Test) AS A".to_owned())
                .into(),
            "SELECT * FROM (SELECT * FROM Test) as A;",
        ),
    ];

    for (error, sql) in error_cases {
        test!(Err(error), sql);
    }

    let found = run!("SELECT id, num, name FROM Test");
    let expected = select!(
        id  | num | name
        I64 | I64 | Str;
        1     2     "Hello".to_owned();
        1     9     "World".to_owned();
        3     4     "Great".to_owned()
    );
    assert_eq!(expected, found);

    let found = run!("SELECT id, num, name FROM Test WHERE id = 1");
    let expected = select!(
        id  | num | name
        I64 | I64 | Str;
        1     2     "Hello".to_owned();
        1     9     "World".to_owned()
    );
    assert_eq!(expected, found);

    run!("UPDATE Test SET id = 2");

    let found = run!("SELECT id, num, name FROM Test");
    let expected = select!(
        id  | num | name;
        I64 | I64 | Str;
        2     2     "Hello".to_owned();
        2     9     "World".to_owned();
        2     4     "Great".to_owned()
    );
    assert_eq!(expected, found);

    let found = run!("SELECT id FROM Test");
    let expected = select!(id; I64; 2; 2; 2);
    assert_eq!(expected, found);

    let found = run!("SELECT id, num FROM Test");
    let expected = select!(id | num; I64 | I64; 2 2; 2 9; 2 4);
    assert_eq!(expected, found);

    let found = run!("SELECT id, num FROM Test LIMIT 1 OFFSET 1");
    let expected = select!(id | num; I64 | I64; 2 9);
    assert_eq!(expected, found);

    //this insert statement works in mysql (so should we support it to?)
    assert_eq!(
        run!(r#"INSERT INTO Test (id, num, name) VALUES (1.1, 1, "good");"#),
        Payload::Insert(1)
    );
});
