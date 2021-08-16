use crate::*;

test_case!(floor, async move {
    use Value::F64;

    let test_cases = vec![
        (
            "CREATE TABLE SingleItem (id INTEGER PRIMARY KEY)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT FLOOR(0.3) AS floor FROM SingleItem",
            Ok(select!(
                "floor";
                F64;
                0.3_f64.floor()
            )),
        ),
        (
            "SELECT FLOOR(-0.8) AS floor FROM SingleItem",
            Ok(select!(
                "floor";
                F64;
                (-0.8_f64).floor()
            )),
        ),
        (
            "SELECT FLOOR(10) AS floor FROM SingleItem",
            Ok(select!(
                "floor";
                F64;
                f64::from(10).floor()
            )),
        ),
        (
            "SELECT FLOOR('6.87421') AS floor FROM SingleItem",
            Ok(select!(
                "floor";
                F64;
                6.87421_f64.floor()
            )),
        ),
        (
            "SELECT FLOOR('7') AS floor FROM SingleItem",
            Ok(select!(
                "floor";
                F64;
                f64::from(7).floor()
            )),
        ),
        (
            "SELECT FLOOR('string') AS floor FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("FLOOR")).into()),
        ),
        (
            "SELECT FLOOR(NULL) AS floor FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("FLOOR")).into()),
        ),
        (
            "SELECT FLOOR(TRUE) AS floor FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("FLOOR")).into()),
        ),
        (
            "SELECT FLOOR(FALSE) AS floor FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("FLOOR")).into()),
        ),
        (
            "SELECT FLOOR('string', 'string2') AS floor FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "FLOOR".to_owned(),
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
