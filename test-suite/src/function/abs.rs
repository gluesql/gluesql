use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::{Payload, Value::*},
    },
};

test_case!(abs, async move {
    let test_cases = [
        (
            "SELECT ABS(1) AS ABS1, 
                    ABS(-1) AS ABS2, 
                    ABS(+1) AS ABS3",
            Ok(select!(
                "ABS1" | "ABS2" | "ABS3";
                I64    | I64    | I64;
                1        1        1
            )),
        ),
        (
            "SELECT ABS(1.5) AS ABS1, 
                    ABS(-1.5) AS ABS2, 
                    ABS(+1.5) AS ABS3;",
            Ok(select!(
                "ABS1" | "ABS2" | "ABS3";
                F64    | F64    | F64;
                1.5      1.5      1.5
            )),
        ),
        (
            "SELECT ABS(0) AS ABS1, 
                    ABS(-0) AS ABS2, 
                    ABS(+0) AS ABS3;",
            Ok(select!(
                "ABS1" | "ABS2" | "ABS3";
                I64    | I64    | I64;
                0        0        0
            )),
        ),
        (
            "CREATE TABLE SingleItem (id integer, int8 int8, dec decimal)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0, -1, -2)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT ABS(id) AS ABS1, 
                    ABS(int8) AS ABS2, 
                    ABS(dec) AS ABS3 
            FROM SingleItem",
            Ok(select!(
                "ABS1"  | "ABS2" | "ABS3";
                I64     | I8     |  Decimal;
                0         1         2.into()
            )),
        ),
        (
            "SELECT ABS('string') AS ABS FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ABS")).into()),
        ),
        ("SELECT ABS(NULL) AS ABS;", Ok(select_with_null!(ABS; Null))),
        (
            "SELECT ABS(TRUE) AS ABS;",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ABS")).into()),
        ),
        (
            "SELECT ABS(FALSE) AS ABS;",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ABS")).into()),
        ),
        (
            "SELECT ABS('string', 'string2') AS ABS",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ABS".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
