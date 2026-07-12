use super::*;

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
        g.test(sql, expected);
    }
});
