use {
    crate::*,
    gluesql_core::{
        executor::{EvaluateError, Payload},
        prelude::Value::*,
        translate::TranslateError,
    },
};

test_case!(ceil, async move {
    let test_cases = [
        (
            "CREATE TABLE SingleItem (id INTEGER DEFAULT CEIL(0.5))",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT CEIL(0.3) AS ceil1, 
            CEIL(-0.8) AS ceil2, 
            CEIL(10) AS ceil3, 
            CEIL(6.87421) AS ceil4 
            FROM SingleItem",
            Ok(select!(
                "ceil1"        | "ceil2"                   | "ceil3"             | "ceil4";
                F64            | F64                       | F64                 | F64 ;
                0.3_f64.ceil()   f64::ceil(-0.8_f64)         f64::from(10).ceil()  6.87421_f64.ceil()
            )),
        ),
        (
            "SELECT CEIL('string') AS ceil FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("CEIL")).into()),
        ),
        (
            "SELECT CEIL(NULL) AS ceil FROM SingleItem",
            Ok(select_with_null!(ceil; Null)),
        ),
        (
            "SELECT CEIL(TRUE) AS ceil FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("CEIL")).into()),
        ),
        (
            "SELECT CEIL(FALSE) AS ceil FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("CEIL")).into()),
        ),
        (
            "SELECT CEIL('string' TO DAY) AS ceil FROM SingleItem",
            Err(TranslateError::UnsupportedExpr("CEIL('string' TO DAY)".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
