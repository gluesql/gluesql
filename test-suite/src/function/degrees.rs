use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value::*},
        translate::TranslateError,
    },
};

test_case!(degrees, async move {
    let test_cases = [
        (
            "CREATE TABLE SingleItem (id FLOAT DEFAULT DEGREES(90))",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT
            DEGREES(180.0) as degrees_1,
            DEGREES(360.0) as degrees_2
            FROM SingleItem",
            Ok(select!(
                degrees_1       | degrees_2;
                F64             | F64;
                180.0_f64.to_degrees()   360.0_f64.to_degrees()
            )),
        ),
        (
            "SELECT DEGREES(90) as degrees_with_int FROM SingleItem",
            Ok(select!(
                degrees_with_int
                F64;
                f64::from(90).to_degrees()
            )),
        ),
        (
            "SELECT DEGREES(0) as degrees_with_zero FROM SingleItem",
            Ok(select!(
                degrees_with_zero
                F64;
                f64::from(0).to_degrees()
            )),
        ),
        (
            "SELECT DEGREES(-900) as degrees_with_zero FROM SingleItem",
            Ok(select!(
                degrees_with_zero
                F64;
                f64::from(-900).to_degrees()
            )),
        ),
        (
            "SELECT DEGREES(900) as degrees_with_zero FROM SingleItem",
            Ok(select!(
                degrees_with_zero
                F64;
                f64::from(900).to_degrees()
            )),
        ),
        (
            "SELECT DEGREES(RADIANS(90)) as radians_to_degrees FROM SingleItem",
            Ok(select!(
                radians_to_degrees
                F64;
                f64::from(90).to_radians().to_degrees()
            )),
        ),
        (
            "SELECT DEGREES(0, 0) as degrees_arg2 FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "DEGREES".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
        (
            "SELECT DEGREES() as degrees_arg0 FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "DEGREES".to_owned(),
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT DEGREES('string') AS degrees FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("DEGREES")).into()),
        ),
        (
            "SELECT DEGREES(NULL) AS degrees FROM SingleItem",
            Ok(select_with_null!(degrees; Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
