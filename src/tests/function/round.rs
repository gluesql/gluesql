use crate::*;

test_case!(round, async move {
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
            "SELECT ROUND(0.3) AS round FROM SingleItem",
            Ok(select!(
                "round";
                F64;
                f64::from(0.3).round()
            )),
        ),
        (
            "SELECT ROUND(-0.8) AS round FROM SingleItem",
            Ok(select!(
                "round";
                F64;
                f64::from(-0.8).round()
            )),
        ),
        (
            "SELECT ROUND(10) AS round FROM SingleItem",
            Ok(select!(
                "round";
                F64;
                f64::from(10).round()
            )),
        ),
        (
            "SELECT ROUND('6.87421') AS round FROM SingleItem",
            Ok(select!(
                "round";
                F64;
                f64::from(6.87421).round()
            )),
        ),
        (
            "SELECT ROUND('7') AS round FROM SingleItem",
            Ok(select!(
                "round";
                F64;
                f64::from(7).round()
            )),
        ),
        (
            "SELECT ROUND('string') AS round FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ROUND")).into()),
        ),
        (
            "SELECT ROUND(NULL) AS round FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ROUND")).into()),
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
