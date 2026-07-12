use super::*;

test_case!(atan, {
    let g = get_tester!();

    let test_cases = [
        (
            "CREATE TABLE SingleItem (id INTEGER DEFAULT ATAN(3.14))",
            Ok(Payload::Create),
        ),
        (r"INSERT INTO SingleItem VALUES (0)", Ok(Payload::Insert(1))),
        (
            "SELECT ATAN(0.5) AS atan1, ATAN(1) AS atan2",
            Ok(select!(
                "atan1"        | "atan2";
                F64            | F64 ;
                0.5_f64.atan()   1.0_f64.atan()
            )),
        ),
        (
            "SELECT ATAN('string') AS atan",
            Err(EvaluateError::FunctionRequiresFloatValue("ATAN".to_owned()).into()),
        ),
        (
            "SELECT ATAN(null) AS atan",
            Ok(select_with_null!(atan; Null)),
        ),
        (
            "SELECT ATAN(true) AS atan",
            Err(EvaluateError::FunctionRequiresFloatValue("ATAN".to_owned()).into()),
        ),
        (
            "SELECT ATAN() AS atan",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ATAN".to_owned(),
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT ATAN(1.0, 2.0) AS atan",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ATAN".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected);
    }
});
