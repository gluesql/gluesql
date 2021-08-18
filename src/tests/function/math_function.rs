use crate::*;

test_case!(acos, async move {
    use Value::F64;

    let test_cases = vec![
        (
            "CREATE TABLE SingleItem (id INTEGER PRIMARY KEY)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT ACOS(0.5) AS acos1, ACOS(1) AS acos2 FROM SingleItem",
            Ok(select!(
                "acos1" | "acos2";
                F64 | F64 ;
                0.5_f64.acos()  1.0_f64.acos()
            )),
        ),
        (
            "SELECT ACOS(3) AS acos FROM SingleItem",
            Err(EvaluateError::OutOfRange(3.0_f64.to_string()).into()),
        ),
        (
            "SELECT ACOS('-3') AS acos FROM SingleItem",
            Err(EvaluateError::OutOfRange((-3.0_f64).to_string()).into()),
        ),
        (
            "SELECT ACOS('string') AS acos FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue("ACOS".to_string()).into()),
        ),
        (
            "SELECT ACOS(null) AS acos FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue("ACOS".to_string()).into()),
        ),
        (
            "SELECT ACOS(true) AS acos FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue("ACOS".to_string()).into()),
        ),
        (
            "SELECT ACOS() AS acos FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ACOS".to_owned(),
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT ASIN(1.0, 2.0) AS sin FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ASIN".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});

test_case!(asin, async move {
    use Value::F64;

    let test_cases = vec![
        (
            "CREATE TABLE SingleItem (id INTEGER PRIMARY KEY)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT ASIN(0.5) AS asin1, ASIN(1) AS asin2 FROM SingleItem",
            Ok(select!(
                "asin1" | "asin2" ;
                F64 | F64 ;
                0.5_f64.asin()  1.0_f64.asin()
            )),
        ),
        (
            "SELECT ASIN(3) AS asin FROM SingleItem",
            Err(EvaluateError::OutOfRange(3.0_f64.to_string()).into()),
        ),
        (
            "SELECT ASIN('-3') AS sin FROM SingleItem",
            Err(EvaluateError::OutOfRange((-3.0_f64).to_string()).into()),
        ),
        (
            "SELECT ASIN('string') AS asin FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue("ASIN".to_string()).into()),
        ),
        (
            "SELECT ASIN(null) AS asin FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue("ASIN".to_string()).into()),
        ),
        (
            "SELECT ASIN(true) AS asin FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue("ASIN".to_string()).into()),
        ),
        (
            "SELECT ASIN() AS asin FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ASIN".to_owned(),
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT ASIN(1.0, 2.0) AS sin FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ASIN".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
