use super::*;

test_case!(log2, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT
                LOG2(64.0) as log2_1,
                LOG2(0.04) as log2_2
            ;",
            Ok(select!(
                log2_1          | log2_2;
                F64             | F64;
                64.0_f64.log2()   0.04_f64.log2()
            )),
        ),
        (
            "SELECT LOG2(32) as log2_with_int;",
            Ok(select!(
                log2_with_int
                F64;
                f64::from(32).log2()
            )),
        ),
        (
            "SELECT LOG2('string') AS log2;",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("LOG2")).into()),
        ),
        (
            "SELECT LOG2(NULL) AS log2",
            Ok(select_with_null!(log2; Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected);
    }
});
