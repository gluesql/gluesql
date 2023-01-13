use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value::*},
        translate::TranslateError,
    },
};

test_case!(floor, async move {
    let test_cases = [
        (
            "CREATE TABLE SingleItem (id INTEGER DEFAULT FLOOR(3.3))",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"
            SELECT 
            FLOOR(0.3) as floor1, 
            FLOOR(-0.8) as floor2, 
            FLOOR(10) as floor3, 
            FLOOR(6.87421) as floor4 
            FROM SingleItem"#,
            Ok(select!(
                floor1          | floor2                 | floor3               | floor4
                F64             | F64                    | F64                  | F64;
                0.3_f64.floor()   f64::floor(-0.8_f64)     f64::from(10).floor()  6.87421_f64.floor()
            )),
        ),
        (
            "SELECT FLOOR('string') AS floor FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("FLOOR")).into()),
        ),
        (
            "SELECT FLOOR(NULL) AS floor FROM SingleItem",
            Ok(select_with_null!(floor; Null)),
        ),
        (
            "SELECT FLOOR(TRUE) AS floor FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("FLOOR")).into()),
        ),
        (
            "SELECT FLOOR(FALSE) AS floor FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("FLOOR")).into()),
        ),
        (
            "SELECT FLOOR('string' TO DAY) AS floor FROM SingleItem",
            Err(TranslateError::UnsupportedExpr("FLOOR('string' TO DAY)".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
