use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::{
            Payload,
            Value::{self, *},
        },
    },
};

test_case!(sin, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT SIN(0.5) AS sin1, SIN(1) AS sin2",
            Ok(select!(
                "sin1"          | "sin2"
                F64             | F64;
                0.5_f64.sin()     1.0_f64.sin()
            )),
        ),
        (
            "SELECT SIN(null) AS sin",
            Ok(select_with_null!(sin; Value::Null)),
        ),
        (
            "SELECT SIN(true) AS sin",
            Err(EvaluateError::FunctionRequiresFloatValue("SIN".to_owned()).into()),
        ),
        (
            "SELECT SIN(false) AS sin",
            Err(EvaluateError::FunctionRequiresFloatValue("SIN".to_owned()).into()),
        ),
        (
            "SELECT SIN('string') AS sin",
            Err(EvaluateError::FunctionRequiresFloatValue("SIN".to_owned()).into()),
        ),
        (
            "SELECT SIN() AS sin",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "SIN".to_owned(),
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT SIN(1.0, 2.0) AS sin",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "SIN".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});

test_case!(cos, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT COS(0.5) AS cos1, COS(1) AS cos2",
            Ok(select!(
                "cos1"        | "cos2"
                F64           | F64;
                0.5_f64.cos()   1.0_f64.cos()
            )),
        ),
        (
            "SELECT COS(null) AS cos",
            Ok(select_with_null!(cos; Value::Null)),
        ),
        (
            "SELECT COS(true) AS cos",
            Err(EvaluateError::FunctionRequiresFloatValue("COS".to_owned()).into()),
        ),
        (
            "SELECT COS(false) AS cos",
            Err(EvaluateError::FunctionRequiresFloatValue("COS".to_owned()).into()),
        ),
        (
            "SELECT COS('string') AS cos",
            Err(EvaluateError::FunctionRequiresFloatValue("COS".to_owned()).into()),
        ),
        (
            "SELECT COS() AS cos",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "COS".to_owned(),
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT COS(1.0, 2.0) AS cos",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "COS".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});

test_case!(tan, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT TAN(0.5) AS tan1, TAN(1) AS tan2",
            Ok(select!(
                "tan1"        | "tan2"
                F64           | F64;
                0.5_f64.tan()   1.0_f64.tan()
            )),
        ),
        (
            "SELECT TAN(null) AS tan",
            Ok(select_with_null!(tan; Value::Null)),
        ),
        (
            "SELECT TAN(true) AS tan",
            Err(EvaluateError::FunctionRequiresFloatValue("TAN".to_owned()).into()),
        ),
        (
            "SELECT TAN(false) AS tan",
            Err(EvaluateError::FunctionRequiresFloatValue("TAN".to_owned()).into()),
        ),
        (
            "SELECT TAN('string') AS tan",
            Err(EvaluateError::FunctionRequiresFloatValue("TAN".to_owned()).into()),
        ),
        (
            "SELECT TAN() AS tan",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "TAN".to_owned(),
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT TAN(1.0, 2.0) AS tan",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "TAN".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});

test_case!(asin, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT ASIN(0.5) AS asin1, ASIN(1) AS asin2",
            Ok(select!(
                "asin1"        | "asin2"
                F64            | F64;
                0.5_f64.asin()   1.0_f64.asin()
            )),
        ),
        (
            "SELECT ASIN('string') AS asin",
            Err(EvaluateError::FunctionRequiresFloatValue("ASIN".to_owned()).into()),
        ),
        (
            "SELECT ASIN(null) AS asin",
            Ok(select_with_null!(asin; Null)),
        ),
        (
            "SELECT ASIN() AS asin",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ASIN".to_owned(),
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT ASIN(1.0, 2.0) AS sin",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ASIN".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});

test_case!(acos, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT ACOS(0.5) AS acos1, ACOS(1) AS acos2",
            Ok(select!(
                "acos1"        | "acos2";
                F64            | F64 ;
                0.5_f64.acos()   1.0_f64.acos()
            )),
        ),
        (
            "SELECT ACOS('string') AS acos",
            Err(EvaluateError::FunctionRequiresFloatValue("ACOS".to_owned()).into()),
        ),
        (
            "SELECT ACOS(null) AS acos",
            Ok(select_with_null!(acos; Null)),
        ),
        (
            "SELECT ACOS(true) AS acos",
            Err(EvaluateError::FunctionRequiresFloatValue("ACOS".to_owned()).into()),
        ),
        (
            "SELECT ACOS() AS acos",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ACOS".to_owned(),
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT ACOS(1.0, 2.0) AS acos",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ACOS".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});

test_case!(atan, {
    let g = get_tester!();

    let test_cases = [
        (
            "CREATE TABLE SingleItem (id INTEGER DEFAULT ATAN(3.14))",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT ATAN(0.5) AS atan1, ATAN(1) AS atan2",
            Ok(select!(
                "atan1"        | "atan2";
                F64            | F64 ;
                0.5_f64.atan()   1.0_f64.atan()
            )),
        ),
        (
            "SELECT ATAN('string') AS atan",
            Err(EvaluateError::FunctionRequiresFloatValue("ATAN".to_owned()).into()),
        ),
        (
            "SELECT ATAN(null) AS atan",
            Ok(select_with_null!(atan; Null)),
        ),
        (
            "SELECT ATAN(true) AS atan",
            Err(EvaluateError::FunctionRequiresFloatValue("ATAN".to_owned()).into()),
        ),
        (
            "SELECT ATAN() AS atan",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ATAN".to_owned(),
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT ATAN(1.0, 2.0) AS atan",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ATAN".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
