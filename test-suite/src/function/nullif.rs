use {
    crate::*,
    chrono::{NaiveDate, NaiveTime},
    gluesql_core::{ast_builder::null, error::{EvaluateError, TranslateError}, prelude::{Payload, Value::*}},
    rust_decimal::Decimal,
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
    ).await;

    g.named_test(
        "NULLIF with equal strings should return NULL",
        "SELECT NULLIF('hello', 'hello') AS result",
        Ok(select_with_null!("result"; Null)),
    ).await;

    g.named_test(
        "NULLIF with different strings should return first arguments",
        "SELECT NULLIF('hello', 'helle') AS result",
        Ok(select_with_null!("result"; Str("hello".to_string()))),
    ).await;

    g.named_test(
        "NULLIF with zero argument should throw EvaluateError",
        "SELECT NULLIF() AS result",
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "NULLIF".to_string(),
            expected: 2,
            found: 0,
        }.into()),
    ).await;

    g.named_test(
        "NULLIF with one argument should throw EvaluateError",
        "SELECT NULLIF(1) AS result",
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "NULLIF".to_string(),
            expected: 2,
            found: 1,
        }.into()),
    ).await;
});