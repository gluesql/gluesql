use crate::*;

test_case!(ceil, async move {
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
            "SELECT CEIL(0.3) AS ceil FROM SingleItem",
            Ok(select!(
                "ceil";
                F64;
                0.3_f64.ceil()
            )),
        ),
        (
            "SELECT CEIL(-0.8) AS ceil FROM SingleItem",
            Ok(select!(
                "ceil";
                F64;
                (-0.8_f64).ceil()
            )),
        ),
        (
            "SELECT CEIL(10) AS ceil FROM SingleItem",
            Ok(select!(
                "ceil";
                F64;
                f64::from(10).ceil()
            )),
        ),
        (
            "SELECT CEIL('6.87421') AS ceil FROM SingleItem",
            Ok(select!(
                "ceil";
                F64;
                6.87421_f64.ceil()
            )),
        ),
        (
            "SELECT CEIL('7') AS ceil FROM SingleItem",
            Ok(select!(
                "ceil";
                F64;
                f64::from(7).ceil()
            )),
        ),
        (
            "SELECT CEIL('string') AS ceil FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("CEIL")).into()),
        ),
        (
            "SELECT CEIL(NULL) AS ceil FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("CEIL")).into()),
        ),
        (
            "SELECT CEIL(TRUE) AS ceil FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("CEIL")).into()),
        ),
        (
            "SELECT CEIL(FALSE) AS ceil FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("CEIL")).into()),
        ),
        (
            "SELECT CEIL('string', 'string2') AS ceil FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "CEIL".to_owned(),
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
