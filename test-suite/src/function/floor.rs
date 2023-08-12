use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::Value::*,
    },
};

test_case!(floor, {
    let g = get_tester!();

    let test_cases = [
        (
            r#"
            SELECT 
                FLOOR(0.3) as floor1, 
                FLOOR(-0.8) as floor2, 
                FLOOR(10) as floor3, 
                FLOOR(6.87421) as floor4 
            ;"#,
            Ok(select!(
                floor1 | floor2              | floor3 | floor4
                F64    | F64                 | F64    | F64;
                0.0      f64::from(-1)   10.0     6.0
            )),
        ),
        (
            "SELECT FLOOR('string') AS floor",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("FLOOR")).into()),
        ),
        (
            "SELECT FLOOR(NULL) AS floor",
            Ok(select_with_null!(floor; Null)),
        ),
        (
            "SELECT FLOOR(TRUE) AS floor",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("FLOOR")).into()),
        ),
        (
            "SELECT FLOOR(FALSE) AS floor",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("FLOOR")).into()),
        ),
        (
            "SELECT FLOOR('string' TO DAY) AS floor",
            Err(TranslateError::UnsupportedExpr("FLOOR('string' TO DAY)".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
