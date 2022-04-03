use crate::*;

test_case!(sign, async move {
    use gluesql_core::{
        executor::EvaluateError, executor::Payload, prelude::Value::*, translate::TranslateError,
    };
    let test_cases = vec![
        (
            "CREATE TABLE SingleItem (id INTEGER)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT sign(1) AS sign1, 
                    sign(-1) AS sign2, 
                    sign(+1) AS sign3 
            FROM SingleItem",
            Ok(select!(
                "sign1"        | "sign2"                   | "sign3";
                F64           | F64                      | F64;
                1_f64.signum()        f64::signum(-1_f64)            f64::from(1).signum()
            )),
        ),
        (
            "SELECT sign(1.0) AS sign1, 
                    sign(-1.0) AS sign2, 
                    sign(+1.0) AS sign3 
            FROM SingleItem",

            Ok(select!(
                "sign1"        | "sign2"                   | "sign3";
                F64           | F64                      | F64;
                1.0_f64.signum()  f64::signum(-1.0_f64)         f64::from(1.0).signum()
            )),
        ),
        
        (
            "SELECT sign(0.0) AS sign1, 
                    sign(-0.0) AS sign2, 
                    sign(+0.0) AS sign3 
            FROM SingleItem",

            Ok(select!(
                "sign1"        | "sign2"                   | "sign3";
                F64           | F64                      | F64;
                0_f64.signum()  f64::from(0.0).signum()         f64::from(0.0).signum()
            )),
        ),
        (
            "SELECT sign(0) AS sign1, 
                    sign(-0) AS sign2, 
                    sign(+0) AS sign3 
            FROM SingleItem",

            Ok(select!(
                "sign1"        | "sign2"                   | "sign3";
                F64           | F64                      | F64;
                0_f64.signum()        f64::signum(0_f64)               f64::from(0).signum()
            )),
        ),
        (
            "SELECT sign('string') AS sign FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("SIGN")).into()),
        ),
        (
            "SELECT sign(NULL) AS sign FROM SingleItem",
            Ok(select_with_null!(sign; Null)),
        ),
        (
            "SELECT sign(TRUE) AS sign FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("SIGN")).into()),
        ),
        (
            "SELECT sign(FALSE) AS sign FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("SIGN")).into()),
        ),
        (
            "SELECT sign('string', 'string2') AS sign FROM SingleItem",
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
