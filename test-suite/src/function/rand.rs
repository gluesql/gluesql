use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value::*},
        translate::TranslateError,
    },
};

test_case!(rand, async move {
    let test_cases = [
        (
            "CREATE TABLE SingleItem (qty INTEGER DEFAULT ROUND(RAND()*100))",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (ROUND(RAND(1)*100))"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT RAND(123) AS rand1, RAND(789.0) AS rand2 FROM SingleItem",
            Ok(select!(
                rand1                 | rand2
                F64                   | F64;
                0.17325464426155657     0.9635218234007941
            )),
        ),
        (
            "SELECT RAND('string') AS rand FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("RAND")).into()),
        ),
        (
            "SELECT RAND(NULL) AS rand FROM SingleItem",
            Ok(select_with_null!(rand; Null)),
        ),
        (
            "SELECT RAND(TRUE) AS rand FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("RAND")).into()),
        ),
        (
            "SELECT RAND(FALSE) AS rand FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("RAND")).into()),
        ),
        (
            "SELECT RAND('string', 'string2') AS rand FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotWithinRange {
                name: "RAND".to_owned(),
                expected_minimum: 0,
                expected_maximum: 1,
                found: 2,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
