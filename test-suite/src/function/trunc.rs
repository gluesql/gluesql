use {
    crate::*,
    gluesql_core::{error::TranslateError, executor::EvaluateError, prelude::Value::*},
};

test_case!(trunc, {
    let g = get_tester!();

    g.named_test(
        "TRUNC(0.3) should return 0.0",
        "SELECT TRUNC(0.3) AS actual",
        Ok(select!(actual F64; 0.0)),
    )
    .await;

    g.named_test(
        "TRUNC(-0.8) should return -0.0 (truncate toward zero)",
        "SELECT TRUNC(-0.8) AS actual",
        Ok(select!(actual F64; -0.0)),
    )
    .await;

    g.named_test(
        "TRUNC(10) should return 10.0 (integer unchanged)",
        "SELECT TRUNC(10) AS actual",
        Ok(select!(actual F64; 10.0)),
    )
    .await;

    g.named_test(
        "TRUNC(6.87421) should return 6.0",
        "SELECT TRUNC(6.87421) AS actual",
        Ok(select!(actual F64; 6.0)),
    )
    .await;

    g.named_test(
        "TRUNC(-3.7) should return -3.0 (truncate toward zero)",
        "SELECT TRUNC(-3.7) AS actual",
        Ok(select!(actual F64; -3.0)),
    )
    .await;

    g.named_test(
        "TRUNC with multiple numeric inputs should truncate each value",
        "SELECT
            TRUNC(0.3) AS trunc1,
            TRUNC(-0.8) AS trunc2,
            TRUNC(10) AS trunc3,
            TRUNC(6.87421) AS trunc4
        ;",
        Ok(select!(
            trunc1 | trunc2 | trunc3 | trunc4
            F64    | F64    | F64    | F64;
            0.0      0.0      10.0     6.0
        )),
    )
    .await;

    g.named_test(
        "TRUNC with string should return EvaluateError::FunctionRequiresFloatValue",
        "SELECT TRUNC('string') AS actual",
        Err(EvaluateError::FunctionRequiresFloatValue("TRUNC".to_owned()).into()),
    )
    .await;

    g.named_test(
        "TRUNC with TRUE should return EvaluateError::FunctionRequiresFloatValue",
        "SELECT TRUNC(TRUE) AS actual",
        Err(EvaluateError::FunctionRequiresFloatValue("TRUNC".to_owned()).into()),
    )
    .await;

    g.named_test(
        "TRUNC with FALSE should return EvaluateError::FunctionRequiresFloatValue",
        "SELECT TRUNC(FALSE) AS actual",
        Err(EvaluateError::FunctionRequiresFloatValue("TRUNC".to_owned()).into()),
    )
    .await;

    g.named_test(
        "TRUNC with NULL should return NULL",
        "SELECT TRUNC(NULL) AS actual",
        Ok(select_with_null!(actual; Null)),
    )
    .await;

    g.named_test(
        "TRUNC with two arguments should return TranslateError::FunctionArgsLengthNotMatching",
        "SELECT TRUNC('string', 'string2') AS actual",
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "TRUNC".to_owned(),
            expected: 1,
            found: 2,
        }
        .into()),
    )
    .await;
});
