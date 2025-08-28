use {crate::*, gluesql_core::prelude::Value::*};

test_case!(trunc, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT
                TRUNC(-42.8) AS trunc1,
                TRUNC(42.8) AS trunc2
            ;",
            Ok(select!(
                "trunc1" | "trunc2";
                F64 | F64;
                -42.0 42.0
            )),
        ),
        (
            "SELECT
                TRUNC(-42) AS trunc1,
                TRUNC(42) AS trunc2
            ;",
            Ok(select!(
                "trunc1" | "trunc2";
                I64 | I64;
                -42 42
            )),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
