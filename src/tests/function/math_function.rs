use crate::*;

test_case!(sin, async move {
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
            "SELECT SIN(0.5) AS sin FROM SingleItem",
            Ok(select!(
                "sin";
                F64;
                f64::from(0.5).sin()
            )),
        ),
        (
            "SELECT SIN(1) AS sin FROM SingleItem",
            Ok(select!(
                "sin";
                F64;
                f64::from(1.0).sin()
            )),
        ),
        (
            "SELECT SIN(-1) AS sin FROM SingleItem",
            Ok(select!(
                "sin";
                F64;
                f64::from(-1.0).sin()
            )),
        ),
        (
            "SELECT SIN(0.976543125) AS sin FROM SingleItem",
            Ok(select!(
                "sin";
                F64;
                f64::from(0.976543125).sin()
            )),
        ),
        (
            "SELECT SIN(3) AS sin FROM SingleItem",
            Ok(select!(
                "sin";
                F64;
                f64::from(3).sin()
            )),
        ),
        (
            "SELECT SIN('3.14') AS sin FROM SingleItem",
            Ok(select!(
                "sin";
                F64;
                f64::from(3.14).sin()
            )),
        ),
        (
            "SELECT SIN('string') AS sin FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue("SIN".to_string()).into()),
        ),
        (
            "SELECT SIN(null) AS sin FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue("SIN".to_string()).into()),
        ),
        (
            "SELECT SIN(true) AS sin FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue("SIN".to_string()).into()),
        ),
        (
            "SELECT SIN() AS sin FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "SIN".to_owned(),
                expected: 1,
                found: 0,
            }.into())
        ),
        (
            "SELECT SIN(1.0, 2.0) AS sin FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "SIN".to_owned(),
                expected: 1,
                found: 2,
            }.into())
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});


test_case!(cos, async move {
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
            "SELECT COS(0.5) AS cos FROM SingleItem",
            Ok(select!(
                "cos";
                F64;
                f64::from(0.5).cos()
            )),
        ),
        (
            "SELECT COS(1) AS cos FROM SingleItem",
            Ok(select!(
                "cos";
                F64;
                f64::from(1.0).cos()
            )),
        ),
        (
            "SELECT COS(-1) AS cos FROM SingleItem",
            Ok(select!(
                "cos";
                F64;
                f64::from(-1.0).cos()
            )),
        ),
        (
            "SELECT COS(0.976543125) AS cos FROM SingleItem",
            Ok(select!(
                "cos";
                F64;
                f64::from(0.976543125).cos()
            )),
        ),
        (
            "SELECT COS(3) AS cos FROM SingleItem",
            Ok(select!(
                "cos";
                F64;
                f64::from(3).cos()
            )),
        ),
        (
            "SELECT COS('3.14') AS cos FROM SingleItem",
            Ok(select!(
                "cos";
                F64;
                f64::from(3.14).cos()
            )),
        ),
        (
            "SELECT COS('string') AS sin FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue("COS".to_string()).into()),
        ),
        (
            "SELECT COS(null) AS sin FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue("COS".to_string()).into()),
        ),
        (
            "SELECT COS(true) AS sin FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue("COS".to_string()).into()),
        ),
        (
            "SELECT COS() AS sin FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "COS".to_owned(),
                expected: 1,
                found: 0,
            }.into())
        ),
        (
            "SELECT COS(1.0, 2.0) AS sin FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "COS".to_owned(),
                expected: 1,
                found: 2,
            }.into())
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
