use {
    crate::*,
    gluesql_core::{
        ast::Expr,
        error::{EvaluateError, FetchError, TranslateError, ValueError},
        prelude::Value::*,
    },
};

test_case!(migrate, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE Test (
            id INT,
            num INT,
            name TEXT
        );
    ",
    )
    .await;
    g.run(
        "
        INSERT INTO Test (id, num, name) VALUES
            (1,     2,     'Hello'),
            (-(-1), 9,     'World'),
            (+3,    2 * 2, 'Great');
        ",
    )
    .await;

    let error_cases = [
        (
            "INSERT INTO Test (id, num, name) VALUES (1.1, 1, 'good');",
            ValueError::FailedToParseNumber.into(),
        ),
        (
            "INSERT INTO Test (id, num, name) VALUES (1, 1, a.b);",
            EvaluateError::ContextRequiredForIdentEvaluation(Expr::CompoundIdentifier {
                alias: "a".to_owned(),
                ident: "b".to_owned(),
            })
            .into(),
        ),
        (
            "SELECT * FROM Test WHERE Here.User.id = 1",
            TranslateError::UnsupportedExpr("Here.User.id".to_owned()).into(),
        ),
        (
            "SELECT * FROM Test NATURAL JOIN Test",
            TranslateError::UnsupportedJoinConstraint("NATURAL".to_owned()).into(),
        ),
        (
            "SELECT 1 ^ 2 FROM Test;",
            TranslateError::UnsupportedBinaryOperator("^".to_owned()).into(),
        ),
        (
            "SELECT * FROM Test UNION SELECT * FROM Test;",
            TranslateError::UnsupportedQuerySetExpr(
                "SELECT * FROM Test UNION SELECT * FROM Test".to_owned(),
            )
            .into(),
        ),
        (
            "SELECT * FROM Test WHERE noname = 1;",
            EvaluateError::IdentifierNotFound("noname".to_owned()).into(),
        ),
        (
            "SELECT * FROM Nothing;",
            FetchError::TableNotFound("Nothing".to_owned()).into(),
        ),
        (
            "TRUNCATE TABLE ProjectUser;",
            TranslateError::UnsupportedStatement("TRUNCATE TABLE ProjectUser".to_owned()).into(),
        ),
    ];

    for (sql, error) in error_cases {
        g.test(sql, Err(error)).await;
    }

    let found = "SELECT id, num, name FROM Test";
    let expected = select!(
        id  | num | name
        I64 | I64 | Str;
        1     2     "Hello".to_owned();
        1     9     "World".to_owned();
        3     4     "Great".to_owned()
    );
    g.test(found, Ok(expected)).await;

    let found = "SELECT id, num, name FROM Test WHERE id = 1";
    let expected = select!(
        id  | num | name
        I64 | I64 | Str;
        1     2     "Hello".to_owned();
        1     9     "World".to_owned()
    );
    g.test(found, Ok(expected)).await;

    g.run("UPDATE Test SET id = 2").await;

    let found = "SELECT id, num, name FROM Test";
    let expected = select!(
        id  | num | name;
        I64 | I64 | Str;
        2     2     "Hello".to_owned();
        2     9     "World".to_owned();
        2     4     "Great".to_owned()
    );
    g.test(found, Ok(expected)).await;

    let found = "SELECT id FROM Test";
    let expected = select!(id; I64; 2; 2; 2);
    g.test(found, Ok(expected)).await;

    let found = "SELECT id, num FROM Test";
    let expected = select!(id | num; I64 | I64; 2 2; 2 9; 2 4);
    g.test(found, Ok(expected)).await;

    let found = "SELECT id, num FROM Test LIMIT 1 OFFSET 1";
    let expected = select!(id | num; I64 | I64; 2 9);
    g.test(found, Ok(expected)).await;

    let found = "SELECT id, num FROM Test LIMIT 1 OFFSET 1";
    let expected = select!(id | num; I64 | I64; 2 9);
    g.test(found, Ok(expected)).await;
});
