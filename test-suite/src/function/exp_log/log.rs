use super::*;

test_case!(log, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT
                LOG(64.0, 2.0) as log_1,
                LOG(0.04, 10.0) as log_2
            ;",
            Ok(select!(
                log_1               | log_2;
                F64                 | F64;
                64.0_f64.log(2.0)     0.04_f64.log(10.0)
            )),
        ),
        (
            "SELECT LOG(10, 10) as log_with_int",
            Ok(select!(
                log_with_int
                F64;
                f64::from(10).log(10.0)
            )),
        ),
        (
            "SELECT LOG('string', 10) AS log",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("LOG")).into()),
        ),
        (
            "SELECT LOG(10, 'string') AS log",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("LOG")).into()),
        ),
        (
            "SELECT LOG(NULL, 10) AS log",
            Ok(select_with_null!(log; Null)),
        ),
        (
            "SELECT LOG(10, NULL) AS log",
            Ok(select_with_null!(log; Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected);
    }
});
