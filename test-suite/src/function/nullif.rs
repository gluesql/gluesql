use {
    crate::*,
    chrono::NaiveDate,
    gluesql_core::{error::TranslateError, prelude::Value::*},
};

test_case!(nullif, {
    let g = get_tester!();

    g.named_test(
        "NULLIF with equal integers should return NULL",
        "SELECT NULLIF(0, 0) AS result",
        Ok(select_with_null!("result"; Null)),
    )
    .await;

    g.named_test(
        "NULLIF with different integers should return first arguments",
        "SELECT NULLIF(1, 0) AS result",
        Ok(select_with_null!("result"; I64(1))),
    )
    .await;

    g.named_test(
        "NULLIF with equal strings should return NULL",
        "SELECT NULLIF('hello', 'hello') AS result",
        Ok(select_with_null!("result"; Null)),
    )
    .await;

    g.named_test(
        "NULLIF with different strings should return first arguments",
        "SELECT NULLIF('hello', 'helle') AS result",
        Ok(select_with_null!("result"; Str("hello".to_owned()))),
    )
    .await;

    g.named_test(
        "NULLIF with equal date should return NULL",
        "SELECT NULLIF(TO_DATE('2025-01-01', '%Y-%m-%d'), TO_DATE('2025-01-01', '%Y-%m-%d')) AS result",
        Ok(select_with_null!("result"; Null)),
    ).await;

    g.named_test(
        "NULLIF with different date should return first arguments",
        "SELECT NULLIF(TO_DATE('2025-01-01', '%Y-%m-%d'), TO_DATE('2025-01-02', '%Y-%m-%d')) AS result",
        Ok(select!(result; Date; NaiveDate::from_ymd_opt(2025, 1, 1).unwrap())),
    ).await;

    g.named_test(
        "NULLIF with zero argument should throw EvaluateError",
        "SELECT NULLIF() AS result",
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "NULLIF".to_owned(),
            expected: 2,
            found: 0,
        }
        .into()),
    )
    .await;

    g.named_test(
        "NULLIF with one argument should throw EvaluateError",
        "SELECT NULLIF(1) AS result",
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "NULLIF".to_owned(),
            expected: 2,
            found: 1,
        }
        .into()),
    )
    .await;
});
