use crate::*;

test_case!(gcd, async move {
    use Value::I64;

    let test_cases = vec![
        (
            r#"
        CREATE TABLE GcdI64 (
            left INTEGER,
            right INTEGER
         )"#,
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO GcdI64 VALUES (0, 3), (2,4), (6,8), (3,5);"#,
            Ok(Payload::Insert(4)),
        ),
        (
            r#"SELECT GCD(left, right) AS test FROM GcdI64"#,
            Ok(select!("test"; I64; 3; 2; 2; 1)),
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
            Err(EvaluateError::FunctionRequiresIntegerValue("Gcd".to_string()).into()),
        ),
        (
            r#"SELECT GCD(right, left) AS test FROM GcdStr"#,
            Err(EvaluateError::FunctionRequiresIntegerValue("Gcd".to_string()).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
