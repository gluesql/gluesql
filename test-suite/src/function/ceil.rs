use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::Value::*,
    },
};

test_case!(ceil, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT
                CEIL(0.3) AS ceil1, 
                CEIL(-0.8) AS ceil2, 
                CEIL(10) AS ceil3, 
                CEIL(6.87421) AS ceil4
            ;",
            Ok(select!(
                "ceil1" | "ceil2" | "ceil3" | "ceil4";
                F64     | F64     | F64     | F64;
                1.0       0.0       10.0      7.0
            )),
        ),
        (
            "SELECT CEIL('string') AS ceil;",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("CEIL")).into()),
        ),
        (
            "SELECT CEIL(NULL) AS ceil;",
            Ok(select_with_null!(ceil; Null)),
        ),
        (
            "SELECT CEIL(TRUE) AS ceil;",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("CEIL")).into()),
        ),
        (
            "SELECT CEIL(FALSE) AS ceil;",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("CEIL")).into()),
        ),
        (
            "SELECT CEIL('string' TO DAY) AS ceil;",
            Err(TranslateError::UnsupportedExpr("CEIL('string' TO DAY)".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
