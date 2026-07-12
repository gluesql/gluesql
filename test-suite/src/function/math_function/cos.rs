use super::*;

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
        g.test(sql, expected);
    }
});
