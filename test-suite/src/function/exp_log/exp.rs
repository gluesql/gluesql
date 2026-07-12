use super::*;

test_case!(exp, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT
                EXP(2.0) as exp1,
                EXP(5.5) as exp2
            ;",
            Ok(select!(
                exp1            | exp2;
                F64             | F64;
                2.0_f64.exp()     5.5_f64.exp()
            )),
        ),
        (
            "SELECT EXP(3) as exp_with_int;",
            Ok(select!(
                exp_with_int
                F64;
                f64::from(3).exp()
            )),
        ),
        (
            "SELECT EXP('string') AS exp;",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("EXP")).into()),
        ),
        ("SELECT EXP(NULL) AS exp", Ok(select_with_null!(exp; Null))),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected);
    }
});
