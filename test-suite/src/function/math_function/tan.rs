use super::*;

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
        g.test(sql, expected);
    }
});
