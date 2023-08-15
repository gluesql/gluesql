use {
    crate::*,
    gluesql_core::{error::TranslateError, prelude::Value::*},
};

test_case!(pi, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT PI() AS pi",
            Ok(select!(
                pi
                F64;
                std::f64::consts::PI
            )),
        ),
        (
            "SELECT PI(0) AS pi",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "PI".to_owned(),
                expected: 0,
                found: 1,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
