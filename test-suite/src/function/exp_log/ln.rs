use super::*;

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
        g.test(sql, expected);
    }
});
