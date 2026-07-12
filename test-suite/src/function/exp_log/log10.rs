use super::*;

test_case!(log10, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT
                LOG10(64.0) as log10_1,
                LOG10(0.04) as log10_2
            ;",
            Ok(select!(
                log10_1           | log10_2;
                F64               | F64;
                64.0_f64.log10()    0.04_f64.log10()
            )),
        ),
        (
            "SELECT LOG10(10) as log10_with_int",
            Ok(select!(
                log10_with_int
                F64;
                f64::from(10).log10()
            )),
        ),
        (
            "SELECT LOG10('string') AS log10",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("LOG10")).into()),
        ),
        (
            "SELECT LOG10(NULL) AS log10",
            Ok(select_with_null!(log10; Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected);
    }
});
