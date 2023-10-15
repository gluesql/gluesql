use {
    crate::*,
    gluesql_core::{executor::EvaluateError, prelude::Value::*},
};

test_case!(dedup, {
    let g = get_tester!();

    g.named_test(
        "DEDUP(CAST('[1, 2, 3, 3, 4, 5, 5]' AS List)) should return '[1, 2, 3, 4, 5]'",
        "SELECT DEDUP(CAST('[1, 2, 3, 3, 4, 5, 5]' AS List)) as actual",
        Ok(select!(actual List; vec![I64(1), I64(2), I64(3), I64(4), I64(5)])),
    )
    .await;

    g.named_test(
        "DEDUP(CAST('['1', 1, '1']' AS List)) should return '['1', 1]'",
        r#"SELECT DEDUP(CAST('["1", 1, 1, "1", "1"]' AS List)) as actual"#,
        Ok(select!(actual List; vec![Str("1".to_owned()), I64(1), Str("1".to_owned())])),
    )
    .await;

    g.named_test(
        "DEDUP with invalid value should return EvaluateError::ListTypeRequired",
        "SELECT DEDUP(1) AS actual",
        Err(EvaluateError::ListTypeRequired.into()),
    )
    .await;
});
