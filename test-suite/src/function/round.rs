use crate::*;

test_case!(round, async move {
    use gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value::*},
        translate::TranslateError,
    };

    let test_cases = vec![
        (
            "CREATE TABLE SingleItem (id INTEGER DEFAULT ROUND(3.5))",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT ROUND(0.3) AS round1, ROUND(-0.8) AS round2, ROUND(10) AS round3, ROUND(6.87421) AS round4 FROM SingleItem",
            Ok(select!(
                round1          | round2                       | round3               | round4
                F64             | F64                          | F64                  | F64;
                0.3_f64.round()   f64::round(-0.8_f64)           f64::from(10).round()  6.87421_f64.round()
            )),
        ),
        (
            "SELECT ROUND('string') AS round FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ROUND")).into()),
        ),
        (
            "SELECT ROUND(NULL) AS round FROM SingleItem",
            Ok(select_with_null!(round; Null)),
        ),
        (
            "SELECT ROUND(TRUE) AS round FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ROUND")).into()),
        ),
        (
            "SELECT ROUND(FALSE) AS round FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ROUND")).into()),
        ),
        (
            "SELECT ROUND('string', 'string2') AS round FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ROUND".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
