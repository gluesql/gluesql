use {
    crate::*,
    gluesql_core::{executor::EvaluateError, prelude::Value::*},
};

test_case!(trunc, {
    let g = get_tester!();

    g.named_test(
        "TRUNC(0.3) should return 0.0",
        "SELECT TRUNC(0.3) as actual",
        Ok(select!(actual F64; 0.0)),
    )
    .await;

    g.named_test(
        "TRUNC(-0.8) should return -0.0 (truncate toward zero)",
        "SELECT TRUNC(-0.8) as actual",
        Ok(select!(actual F64; -0.0)),
    )
    .await;

    g.named_test(
        "TRUNC(10) should return 10.0 (integer unchanged)",
        "SELECT TRUNC(10) as actual",
        Ok(select!(actual F64; 10.0)),
    )
    .await;

    g.named_test(
        "TRUNC(6.87421) should return 6.0",
        "SELECT TRUNC(6.87421) as actual",
        Ok(select!(actual F64; 6.0)),
    )
    .await;

    g.named_test(
        "TRUNC(-3.7) should return -3.0 (truncate toward zero)",
        "SELECT TRUNC(-3.7) as actual",
        Ok(select!(actual F64; -3.0)),
    )
    .await;

    g.named_test(
        "TRUNC with string should return EvaluateError::FunctionRequiresFloatValue",
        "SELECT TRUNC('string') AS actual",
        Err(EvaluateError::FunctionRequiresFloatValue("TRUNC".to_owned()).into()),
    )
    .await;

    g.named_test(
        "TRUNC with boolean should return EvaluateError::FunctionRequiresFloatValue",
        "SELECT TRUNC(TRUE) AS actual",
        Err(EvaluateError::FunctionRequiresFloatValue("TRUNC".to_owned()).into()),
    )
    .await;

    g.named_test(
        "TRUNC with NULL should return NULL",
        "SELECT TRUNC(NULL) AS actual",
        Ok(select_with_null!(actual; Null)),
    )
    .await;
});
