use super::*;

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
        g.test(sql, expected);
    }
});
