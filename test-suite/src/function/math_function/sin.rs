use super::*;

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
        g.test(sql, expected);
    }
});
