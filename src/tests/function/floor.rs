use crate::*;

test_case!(floor, async move {
    use Value::{Null, F64};

    let test_cases = vec![
        ("CREATE TABLE SingleItem (id INTEGER)", Ok(Payload::Create)),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"
            SELECT 
            FLOOR(0.3) as floor1, 
            FLOOR(-0.8) as floor2, 
            FLOOR(10) as floor3, 
            FLOOR(6.87421) as floor4 
            FROM SingleItem"#,
            Ok(select!(
                floor1          | floor2                 | floor3               | floor4
                F64             | F64                    | F64                  | F64;
                0.3_f64.floor()   f64::floor(-0.8_f64)     f64::from(10).floor()  6.87421_f64.floor()
            )),
        ),
        (
            "SELECT FLOOR('string') AS floor FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("FLOOR")).into()),
        ),
        (
            "SELECT FLOOR(NULL) AS floor FROM SingleItem",
            Ok(select_with_null!(floor; Null)),
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
