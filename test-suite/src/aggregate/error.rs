use {
    crate::*,
    gluesql_core::{data::KeyError, executor::EvaluateError, translate::TranslateError},
};

test_case!(error, async move {
    run!(
        "
        CREATE TABLE Item (
            id INTEGER,
            quantity INTEGER,
            age INTEGER NULL,
            total INTEGER,
        );
    "
    );
    run!(
        "
        INSERT INTO Item (id, quantity, age, total) VALUES
            (1, 10,   11, 1),
            (2,  0,   90, 2),
            (3,  9, NULL, 3),
            (4,  3,    3, 1),
            (5, 25, NULL, 1);
    "
    );

    let test_cases = [
        (
            EvaluateError::ValueNotFound("num".to_owned()).into(),
            "SELECT SUM(num) FROM Item;",
        ),
        (
            TranslateError::QualifiedWildcardInCountNotSupported("Foo.*".to_owned()).into(),
            "SELECT COUNT(Foo.*) FROM Item;",
        ),
        (
            TranslateError::WildcardFunctionArgNotAccepted.into(),
            "SELECT SUM(*) FROM Item;",
        ),
    ];

    for (error, sql) in test_cases {
        test!(sql, Err(error));
    }
});

test_case!(error_group_by, async move {
    run!(
        "
        CREATE TABLE Item (
            id INTEGER,
            quantity INTEGER NULL,
            city TEXT,
            ratio FLOAT,
        );
    "
    );
    run!(
        "
        INSERT INTO Item (id, quantity, city, ratio) VALUES
            (1,   10,   \"Seoul\",  0.2),
            (2,    0,   \"Dhaka\",  0.9),
            (3, NULL, \"Beijing\",  1.1),
            (3,   30, \"Daejeon\",  3.2),
            (4,   11,   \"Seoul\",   11),
            (5,   24, \"Seattle\", 6.11);
    "
    );
    test!(
        "SELECT * FROM Item GROUP BY ratio;",
        Err(KeyError::FloatTypeKeyNotSupported.into())
    );
});
