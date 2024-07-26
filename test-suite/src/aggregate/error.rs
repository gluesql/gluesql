use {
    crate::*,
    gluesql_core::error::{EvaluateError, TranslateError},
};

test_case!(error, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE Item (
            id INTEGER,
            quantity INTEGER,
            age INTEGER NULL,
            total INTEGER
        );
    ",
    )
    .await;
    g.run(
        "
        INSERT INTO Item (id, quantity, age, total) VALUES
            (1, 10,   11, 1),
            (2,  0,   90, 2),
            (3,  9, NULL, 3),
            (4,  3,    3, 1),
            (5, 25, NULL, 1);
    ",
    )
    .await;

    let test_cases = [
        (
            "SELECT SUM(num) FROM Item;",
            EvaluateError::IdentifierNotFound("num".to_owned()).into(),
        ),
        (
            "SELECT COUNT(Foo.*) FROM Item;",
            TranslateError::QualifiedWildcardInCountNotSupported("Foo.*".to_owned()).into(),
        ),
        (
            "SELECT SUM(*) FROM Item;",
            TranslateError::WildcardFunctionArgNotAccepted.into(),
        ),
    ];

    for (sql, error) in test_cases {
        g.test(sql, Err(error)).await;
    }
});
