use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value::*},
    },
};

test_case!(gcd_lcm, async move {
    let test_cases = [
        (
            r#"
        CREATE TABLE GcdI64 (
            left INTEGER NULL DEFAULT GCD(3, 4),
            right INTEGER NULL DEFAULT LCM(10, 2)
         )"#,
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO GcdI64 VALUES (0, 3), (2,4), (6,8), (3,5), (1, NULL), (NULL, 1);"#,
            Ok(Payload::Insert(6)),
        ),
        (
            r#"SELECT GCD(left, right) AS test FROM GcdI64"#,
            Ok(select_with_null!(
                test;
                I64(3);
                I64(2);
                I64(2);
                I64(1);
                Null;
                Null
            )),
        ),
        (
            r#"
        CREATE TABLE GcdStr (
            left TEXT,
            right INTEGER
         )"#,
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO GcdStr VALUES ("TEXT", 0);"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT GCD(left, right) AS test FROM GcdStr"#,
            Err(EvaluateError::FunctionRequiresIntegerValue("GCD".to_owned()).into()),
        ),
        (
            r#"SELECT GCD(right, left) AS test FROM GcdStr"#,
            Err(EvaluateError::FunctionRequiresIntegerValue("GCD".to_owned()).into()),
        ),
        (
            r#"
        CREATE TABLE LcmI64 (
            left INTEGER NULL DEFAULT true,
            right INTEGER NULL DEFAULT true
         )"#,
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO LcmI64 VALUES (0, 3), (2,4), (6,8), (3,5), (1, NULL), (NULL, 1);"#,
            Ok(Payload::Insert(6)),
        ),
        (
            r#"SELECT LCM(left, right) AS test FROM LcmI64"#,
            Ok(select_with_null!(
                test;
                I64(0);
                I64(4);
                I64(24);
                I64(15);
                Null;
                Null
            )),
        ),
        (
            r#"
        CREATE TABLE LcmStr (
            left TEXT,
            right INTEGER
         )"#,
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO LcmStr VALUES ("TEXT", 0);"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT LCM(left, right) AS test FROM LcmStr"#,
            Err(EvaluateError::FunctionRequiresIntegerValue("LCM".to_owned()).into()),
        ),
        (
            r#"SELECT LCM(right, left) AS test FROM LcmStr"#,
            Err(EvaluateError::FunctionRequiresIntegerValue("LCM".to_owned()).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
