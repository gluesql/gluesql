use crate::*;
use test::*;

test_case!(gcd_lcm, async move {
    use prelude::Value::I64;

    let test_cases = vec![
        (
            r#"
        CREATE TABLE GcdI64 (
            left INTEGER NULL DEFAULT true,
            right INTEGER NULL DEFAULT true
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
            Err(EvaluateError::FunctionRequiresIntegerValue("GCD".to_string()).into()),
        ),
        (
            r#"SELECT GCD(right, left) AS test FROM GcdStr"#,
            Err(EvaluateError::FunctionRequiresIntegerValue("GCD".to_string()).into()),
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
            Err(EvaluateError::FunctionRequiresIntegerValue("LCM".to_string()).into()),
        ),
        (
            r#"SELECT LCM(right, left) AS test FROM LcmStr"#,
            Err(EvaluateError::FunctionRequiresIntegerValue("LCM".to_string()).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
