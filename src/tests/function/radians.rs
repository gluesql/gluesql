use crate::*;

test_case!(radians, async move {
    use {
        executor::EvaluateError,
        prelude::{Payload, Value::*},
        translate::TranslateError,
    };

    let test_cases = vec![
        ("CREATE TABLE SingleItem (id FLOAT)", Ok(Payload::Create)),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT
            RADIANS(180.0) as radians_1,
            RADIANS(360.0) as radians_2
            FROM SingleItem",
            Ok(select!(
                radians_1       | radians_2;
                F64             | F64;
                180.0_f64.to_radians()   360.0_f64.to_radians()
            )),
        ),
        (
            "SELECT RADIANS(90) as radians_with_int FROM SingleItem",
            Ok(select!(
                radians_with_int
                F64;
                f64::from(90).to_radians()
            )),
        ),
        (
            "SELECT RADIANS(0) as radians_with_zero FROM SingleItem",
            Ok(select!(
                radians_with_zero
                F64;
                f64::from(0).to_radians()
            )),
        ),
        (
            "SELECT RADIANS(-900) as radians_with_zero FROM SingleItem",
            Ok(select!(
                radians_with_zero
                F64;
                f64::from(-900).to_radians()
            )),
        ),
        (
            "SELECT RADIANS(900) as radians_with_zero FROM SingleItem",
            Ok(select!(
                radians_with_zero
                F64;
                f64::from(900).to_radians()
            )),
        ),
        (
            "SELECT RADIANS(DEGREES(90)) as degrees_to_radians FROM SingleItem",
            Ok(select!(
                degrees_to_radians
                F64;
                f64::from(90).to_degrees().to_radians()
            )),
        ),
        (
            "SELECT RADIANS(0, 0) as radians_arg2 FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "RADIANS".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
        (
            "SELECT RADIANS() as radians_arg0 FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "RADIANS".to_owned(),
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT RADIANS('string') AS radians FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("RADIANS")).into()),
        ),
        (
            "SELECT RADIANS(NULL) AS radians FROM SingleItem",
            Ok(select_with_null!(radians; Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
