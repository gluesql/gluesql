use {
    crate::*,
    gluesql_core::{executor::EvaluateError, prelude::Value::*},
};

test_case!(splice, {
    let g = get_tester!();
    g.run(
        "
        CREATE TABLE ListTable (
            id INTEGER,
            items LIST
        );
        ",
    )
    .await;

    g.run(
        r#"
        INSERT INTO ListTable VALUES
            (1, '[1, 2, 3]'),
            (2, '["1", "2", "3"]'),
            (3, '["1", 2, 3]')
        "#,
    )
    .await;

    g.named_test(
        "SPLICE(CAST('[1, 2, 3, 4, 5]' AS List), 1, 3) should return '[1, 4, 5]'",
        "SELECT SPLICE(CAST('[1, 2, 3, 4, 5]' AS List), 1, 3) AS actual",
        Ok(select!(actual List; vec![I64(1), I64(4), I64(5)])),
    )
    .await;

    g.named_test(
        "SPLICE(CAST('[1, 2, 3, 4, 5]' AS List), 1, 3, CAST('[100, 99]' AS List)) should return '[1, 100, 99, 4, 5]'",
        "SELECT SPLICE(CAST('[1, 2, 3, 4, 5]' AS List), 1, 3, CAST('[100, 99]' AS List)) AS actual",
        Ok(select!(actual List; vec![I64(1), I64(100), I64(99), I64(4), I64(5)]))
    ).await;

    g.named_test(
        "SPLICE(CAST('[1, 2, 3]' AS List), -1, 2, CAST('[100, 99]' AS List)) should return '[100, 99, 3]'",
        "SELECT SPLICE(CAST('[1, 2, 3]' AS List), -1, 2, CAST('[100, 99]' AS List)) AS actual",
        Ok(select!(actual List; vec![I64(100), I64(99), I64(3)]))
    ).await;

    g.named_test(
        "SPLICE(CAST('[1, 2, 3]' AS List), 1, 100, CAST('[100, 99]' AS List)) should return '[1, 100, 99]')",
        "SELECT SPLICE(CAST('[1, 2, 3]' AS List), 1, 100, CAST('[100, 99]' AS List)) AS actual",
        Ok(select!(actual List; vec!(I64(1), I64(100), I64(99))))
    ).await;

    g.named_test(
        "SPLICE(3, 1, 2) sholud return EvaluateError::ListTypeRequired",
        "SELECT SPLICE(1, 2, 3) AS actual",
        Err(EvaluateError::ListTypeRequired.into()),
    )
    .await;

    g.named_test(
        "SPLICE(CAST('[1, 2, 3]' AS List), 2, 4, 9) should return EvaluateError::ListTypeRequired",
        "SELECT SPLICE(CAST('[1, 2, 3]' AS List), 2, 4, 9), AS actual",
        Err(EvaluateError::ListTypeRequired.into()),
    )
    .await;
});
