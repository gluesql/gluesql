use {
    crate::*,
    gluesql_core::{error::EvaluateError, prelude::Value::*},
};

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
        g.test(sql, expected).await;
    }
});

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
        g.test(sql, expected).await;
    }
});

test_case!(ln, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT
                LN(64.0) as ln1,
                LN(0.04) as ln2
            ;",
            Ok(select!(
                ln1             | ln2;
                F64             | F64;
                64.0_f64.ln()     0.04_f64.ln()
            )),
        ),
        (
            "SELECT LN(10) as ln_with_int",
            Ok(select!(
                ln_with_int
                F64;
                f64::from(10).ln()
            )),
        ),
        (
            "SELECT LN('string') AS log10",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("LN")).into()),
        ),
        ("SELECT LN(NULL) AS ln", Ok(select_with_null!(ln; Null))),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});

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
        g.test(sql, expected).await;
    }
});

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
        g.test(sql, expected).await;
    }
});
