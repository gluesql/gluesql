use {
    crate::*,
<<<<<<< HEAD
    gluesql_core::{error::TranslateError, executor::EvaluateError, prelude::Value::*},
=======
    gluesql_core::{executor::EvaluateError, prelude::Value::*},
>>>>>>> f3cb1e30 (test: add comprehensive TRUNC function tests)
};

test_case!(trunc, {
    let g = get_tester!();

    g.named_test(
        "TRUNC(0.3) should return 0.0",
<<<<<<< HEAD
        "SELECT TRUNC(0.3) AS actual",
=======
        "SELECT TRUNC(0.3) as actual",
>>>>>>> f3cb1e30 (test: add comprehensive TRUNC function tests)
        Ok(select!(actual F64; 0.0)),
    )
    .await;

    g.named_test(
        "TRUNC(-0.8) should return -0.0 (truncate toward zero)",
<<<<<<< HEAD
        "SELECT TRUNC(-0.8) AS actual",
=======
        "SELECT TRUNC(-0.8) as actual",
>>>>>>> f3cb1e30 (test: add comprehensive TRUNC function tests)
        Ok(select!(actual F64; -0.0)),
    )
    .await;

    g.named_test(
        "TRUNC(10) should return 10.0 (integer unchanged)",
<<<<<<< HEAD
        "SELECT TRUNC(10) AS actual",
=======
        "SELECT TRUNC(10) as actual",
>>>>>>> f3cb1e30 (test: add comprehensive TRUNC function tests)
        Ok(select!(actual F64; 10.0)),
    )
    .await;

    g.named_test(
        "TRUNC(6.87421) should return 6.0",
<<<<<<< HEAD
        "SELECT TRUNC(6.87421) AS actual",
=======
        "SELECT TRUNC(6.87421) as actual",
>>>>>>> f3cb1e30 (test: add comprehensive TRUNC function tests)
        Ok(select!(actual F64; 6.0)),
    )
    .await;

    g.named_test(
        "TRUNC(-3.7) should return -3.0 (truncate toward zero)",
<<<<<<< HEAD
        "SELECT TRUNC(-3.7) AS actual",
=======
        "SELECT TRUNC(-3.7) as actual",
>>>>>>> f3cb1e30 (test: add comprehensive TRUNC function tests)
        Ok(select!(actual F64; -3.0)),
    )
    .await;

    g.named_test(
<<<<<<< HEAD
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
=======
>>>>>>> f3cb1e30 (test: add comprehensive TRUNC function tests)
        "TRUNC with string should return EvaluateError::FunctionRequiresFloatValue",
        "SELECT TRUNC('string') AS actual",
        Err(EvaluateError::FunctionRequiresFloatValue("TRUNC".to_owned()).into()),
    )
    .await;

    g.named_test(
<<<<<<< HEAD
        "TRUNC with TRUE should return EvaluateError::FunctionRequiresFloatValue",
=======
        "TRUNC with boolean should return EvaluateError::FunctionRequiresFloatValue",
>>>>>>> f3cb1e30 (test: add comprehensive TRUNC function tests)
        "SELECT TRUNC(TRUE) AS actual",
        Err(EvaluateError::FunctionRequiresFloatValue("TRUNC".to_owned()).into()),
    )
    .await;

    g.named_test(
<<<<<<< HEAD
        "TRUNC with FALSE should return EvaluateError::FunctionRequiresFloatValue",
        "SELECT TRUNC(FALSE) AS actual",
        Err(EvaluateError::FunctionRequiresFloatValue("TRUNC".to_owned()).into()),
    )
    .await;

    g.named_test(
=======
>>>>>>> f3cb1e30 (test: add comprehensive TRUNC function tests)
        "TRUNC with NULL should return NULL",
        "SELECT TRUNC(NULL) AS actual",
        Ok(select_with_null!(actual; Null)),
    )
    .await;
<<<<<<< HEAD

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
=======
>>>>>>> f3cb1e30 (test: add comprehensive TRUNC function tests)
});
