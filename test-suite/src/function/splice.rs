use {
    crate::*,
    gluesql_core::{executor::EvaluateError, prelude::Value::*},
};

test_case!(splice, async move {
    run!(
        "
        CREATE TABLE ListTable (
            id INTEGER,
            items LIST
        );
        "
    );

    run!(
        r#"
        INSERT INTO ListTable VALUES
            (1, '[1, 2, 3]'),
            (2, '["1", "2", "3"]'),
            (3, '["1", 2, 3]')
        "#
    );

    test! (
        name: "SPLICE(CAST('[1, 2, 3, 4, 5]' AS List), 1, 3) should return '[1, 4, 5]'",
        sql: "SELECT SPLICE(CAST('[1, 2, 3, 4, 5]' AS List), 1, 3) AS actual",
        expected: Ok(select!(actual List; vec![I64(1), I64(4), I64(5)]))
    );

    test!(
        name: "SPLICE(CAST('[1, 2, 3, 4, 5]' AS List), 1, 3, CAST('[100, 99]' AS List)) should return '[1, 100, 99, 4, 5]'",
        sql: "SELECT SPLICE(CAST('[1, 2, 3, 4, 5]' AS List), 1, 3, CAST('[100, 99]' AS List)) AS actual",
        expected: Ok(select!(actual List; vec![I64(1), I64(100), I64(99), I64(4), I64(5)]))
    );

    test!(
        name: "SPLICE(CAST('[1, 2, 3]' AS List), -1, 2, CAST('[100, 99]' AS List)) should return '[100, 99, 3]'",
        sql: "SELECT SPLICE(CAST('[1, 2, 3]' AS List), -1, 2, CAST('[100, 99]' AS List)) AS actual",
        expected: Ok(select!(actual List; vec![I64(100), I64(99), I64(3)]))
    );

    test!(
        name: "SPLICE(CAST('[1, 2, 3]' AS List), 1, 100, CAST('[100, 99]' AS List)) should return '[1, 100, 99]')",
        sql: "SELECT SPLICE(CAST('[1, 2, 3]' AS List), 1, 100, CAST('[100, 99]' AS List)) AS actual",
        expected: Ok(select!(actual List; vec!(I64(1), I64(100), I64(99))))
    );

    test!(
        name: "SPLICE(3, 1, 2) sholud return EvaluateError::ListTypeRequired",
        sql: "SELECT SPLICE(1, 2, 3) AS actual",
        expected: Err(EvaluateError::ListTypeRequired.into())
    );

    test!(
        name: "SPLICE(CAST('[1, 2, 3]' AS List), 2, 4, 9) should return EvaluateError::ListTypeRequired",
        sql:"SELECT SPLICE(CAST('[1, 2, 3]' AS List), 2, 4, 9), AS actual",
        expected: Err(EvaluateError::ListTypeRequired.into())
    );
});
