use crate::*;

test_case!(gcd, async move {
    use Value::F64;

    let test_cases = vec![
        (
            r#"
        CREATE TABLE GcdF64 (
            left FLOAT,
            right FLOAT
         )"#,
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO GcdF64 VALUES (3.0, 0.0);"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"INSERT INTO GcdF64 VALUES (2.0, 4.0);"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"INSERT INTO GcdF64 VALUES (6.0, 8.0);"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT GCD(left, right) AS test FROM GcdF64 WHERE left = 3.0"#,
            Ok(select!("test"; F64; 3.0)),
        ),
        (
            r#"SELECT GCD(right, left) AS test FROM GcdF64 WHERE left = 3.0"#,
            Ok(select!("test"; F64; 3.0)),
        ),
        (
            r#"SELECT GCD(left, right) AS test FROM GcdF64 WHERE left = 2.0"#,
            Ok(select!("test"; F64; 2.0)),
        ),
        (
            r#"SELECT GCD(left, right) AS test FROM GcdF64 WHERE left = 6.0"#,
            Ok(select!("test"; F64; 2.0)),
        ),
        (
            r#"
        CREATE TABLE GcdI64 (
            left INTEGER,
            right INTEGER
         )"#,
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO GcdI64 VALUES (1, 0);"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"INSERT INTO GcdI64 VALUES (3, 5);"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT GCD(left, right) AS test FROM GcdI64 WHERE left = 1"#,
            Ok(select!("test"; F64; 1.0)),
        ),
        (
            r#"SELECT GCD(left, right) AS test FROM GcdI64 WHERE left = 3"#,
            Ok(select!("test"; F64; 1.0)),
        ),
        (
            r#"
        CREATE TABLE GcdStr (
            left TEXT,
            right FLOAT
         )"#,
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO GcdStr VALUES ("TEXT", 0.0);"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT GCD(left, right) AS test FROM GcdStr"#,
            Err(EvaluateError::FunctionRequiresF64Value("Gcd".to_string()).into()),
        ),
        (
            r#"SELECT GCD(right, left) AS test FROM GcdStr"#,
            Err(EvaluateError::FunctionRequiresF64Value("Gcd".to_string()).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
