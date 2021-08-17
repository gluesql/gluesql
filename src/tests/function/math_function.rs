use crate::*;

test_case!(asin, async move {
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
            "SELECT ASIN(0.5) AS asin FROM SingleItem",
            Ok(select!(
                "asin";
                F64;
                f64::from(0.5).asin()
            )),
        ),
        (
            "SELECT ASIN(1) AS asin FROM SingleItem",
            Ok(select!(
                "asin";
                F64;
                f64::from(1).asin()
            )),
        ),
        (
            "SELECT ASIN(-1) AS asin FROM SingleItem",
            Ok(select!(
                "asin";
                F64;
                f64::from(-1).asin()
            )),
        ),
        (
            "SELECT ASIN(0.976543125) AS asin FROM SingleItem",
            Ok(select!(
                "asin";
                F64;
                f64::from(0.976543125).asin()
            )),
        ),
        (
            "SELECT ASIN(3) AS asin FROM SingleItem",
            Err(EvaluateError::OutOfRange(f64::from(3.0).to_string().to_owned()).into()),
        ),
        (
            "SELECT ASIN('-3') AS sin FROM SingleItem",
            Err(EvaluateError::OutOfRange(f64::from(-3.0).to_string().to_owned()).into()),
        ),
        (
            "SELECT ASIN('string') AS asin FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue("ASIN".to_string()).into()),
        ),
        (
            "SELECT ASIN(null) AS asin FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue("ASIN".to_string()).into()),
        ),
        (
            "SELECT ASIN(true) AS asin FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue("ASIN".to_string()).into()),
        ),
        (
            "SELECT ASIN() AS asin FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ASIN".to_owned(),
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT ASIN(1.0, 2.0) AS sin FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ASIN".to_owned(),
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
