use crate::*;

test_case!(abs, async move{
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
            ABS(0) as abs1,
            ABS(-10.25) as abs2,
            ABS(10) as abs3
            FROM SingleItem
            "#,
            Ok(select!(
                abs1        | abs2                  | abs3      
                F64         | F64                   | F64 
                0_f64.abs()   f64::abs(-10.25_f64)    10_f64.abs()
            ))
        ),
        (
            "SELECT ABS('string') AS abs FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ABS")).into()),
        ),
        (
            "SELECT ABS(NULL) AS abs FROM SingleItem",
            Ok(select_with_null!(abs; Null)),
        ),
        (
            "SELECT ABS(TRUE) AS abs FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ABS")).into()),
        ),
        (
            "SELECT ABS(FALSE) AS abs FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ABS")).into()),
        ),
        (
            "SELECT ABS('string', 'string2') AS abs FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ABS".to_owned(),
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