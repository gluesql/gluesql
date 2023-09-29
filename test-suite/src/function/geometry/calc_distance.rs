use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::{Payload, Value::*},
    },
};

test_case!(calc_distance, {
    let g = get_tester!();

    let test_cases = [
        (
            "CREATE TABLE Foo (geo1 Point, geo2 Point, bar Float)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO Foo VALUES (POINT(0.3134, 3.156), POINT(1.415, 3.231), 3)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT CALC_DISTANCE(geo1, geo2) AS georesult FROM Foo"#,
            Ok(select!(
                georesult
                F64;
                1.104150152832485_f64

            )),
        ),
        (
            r#"SELECT CALC_DISTANCE(geo1) AS georesult FROM Foo"#,
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "CALC_DISTANCE".to_owned(),
                expected: 2,
                found: 1,
            }
            .into()),
        ),
        (
            r#"SELECT CALC_DISTANCE(geo1, bar) AS georesult FROM Foo"#,
            Err(EvaluateError::FunctionRequiresPointValue("CALC_DISTANCE".to_owned()).into()),
        ),
        (
            r#"SELECT CALC_DISTANCE(geo1, NULL) AS georesult FROM Foo"#,
            Ok(select_with_null!(georesult; Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
