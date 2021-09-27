use crate::*;

test_case!(sign, async move{
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
            SIGN(0) as sign1,
            SIGN(-10) as sign2,
            SIGN(10) as sign3
            FROM SingleItem
            "#,
            Ok(select!(
                sign1         | sign2                 | sign3      
                F64           | F64                   | F64 
                0_f64.signum()  f64::signum(-10_f64)    10_f64.signum()
            ))
        ),
        (
            "SELECT SIGN('string') AS sign FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("SIGN")).into()),
        ),
        (
            "SELECT SIGN(NULL) AS sign FROM SingleItem",
            Ok(select_with_null!(abs; Null)),
        ),
        (
            "SELECT SIGN(TRUE) AS sign FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("SIGN")).into()),
        ),
        (
            "SELECT SIGN(FALSE) AS sign FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("SIGN")).into()),
        ),
        (
            "SELECT SIGN('string', 'string2') AS sign FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "SIGN".to_owned(),
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