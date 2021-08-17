use crate::*;

test_case!(div_mod, async move {
    use Value::{Null, Str};
    let test_cases = vec![
        (
            "CREATE TABLE FloatDiv (dividend FLOAT, divisor FLOAT)",
            Ok(Payload::Create),
        ),
        (
            r#"
            INSERT INTO 
                FloatDiv (dividend, divisor) 
            VALUES 
                (12.0, 3.0), (12.34, 56.78), (-12.3, 4.0)
            "#,
            Ok(Payload::Insert(3)),
        ),
        (
            "
            SELECT 
                DIV(dividend, divisor),
                MOD(dividend, divisor) 
            FROM FloatDiv
            ",
            Ok(select!(
                "DIV(dividend, divisor)" | "MOD(dividend, divisor)"
                Str                      | Str;
                "4".to_owned()             "0".to_owned();
                "0".to_owned()             "12.34".to_owned();
                "-3".to_owned()            "-0.3".to_owned()
            )),
        ),
        (
            "
            SELECT 
                DIV(dividend, divisor) as quotient,
                MOD(dividend, divisor) as remainder 
            FROM FloatDiv LIMIT 1
            ",
            Ok(select!(
                quotient          | remainder
                Str               | Str;
                "4".to_owned()      "0".to_owned()
            )),
        ),
        (
            "SELECT DIV(1.0, 0.0) AS quotient FROM FloatDiv",
            Err(EvaluateError::InvalidDivisorZero.into()),
        ),
        (
            r#"SELECT DIV(1.0, "dividend") AS quotient FROM FloatDiv"#,
            Err(EvaluateError::FunctionRequiresFloatValue("DIV".to_owned()).into()),
        ),
        (
            "SELECT DIV(1.0) AS quotient FROM FloatDiv",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "DIV".to_owned(),
                expected: 2,
                found: 1,
            }
            .into()),
        ),
        (
            "SELECT MOD(1.0, 2, 3) AS remainder FROM FloatDiv",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "MOD".to_owned(),
                expected: 2,
                found: 3,
            }
            .into()),
        ),
        (
            "CREATE TABLE IntDiv (dividend INTEGER, divisor INTEGER)",
            Ok(Payload::Create),
        ),
        (
            r#"
            INSERT INTO 
                IntDiv (dividend, divisor) 
            VALUES 
                (12, 3), (12, 7), (12, 34), (-12, 7)
            "#,
            Ok(Payload::Insert(4)),
        ),
        (
            "
            SELECT 
                DIV(dividend, divisor),
                MOD(dividend, divisor) 
            FROM IntDiv 
            ",
            Ok(select!(
                "DIV(dividend, divisor)" | "MOD(dividend, divisor)"
                Str                      | Str;
                "4".to_owned()             "0".to_owned();
                "1".to_owned()             "5".to_owned();
                "0".to_owned()             "12".to_owned();
                "-1".to_owned()            "-5".to_owned()
            )),
        ),
        (
            "
            SELECT 
                DIV(dividend, divisor) as quotient,
                MOD(dividend, divisor) as remainder 
            FROM IntDiv LIMIT 1
            ",
            Ok(select!(
                quotient          | remainder
                Str               | Str;
                "4".to_owned()      "0".to_owned()
            )),
        ),
        (
            "SELECT DIV(1, 0) AS quotient FROM IntDiv",
            Err(EvaluateError::InvalidDivisorZero.into()),
        ),
        (
            "CREATE TABLE MixDiv (dividend INTEGER NULL, divisor FLOAT NULL)",
            Ok(Payload::Create),
        ),
        (
            r#"
            INSERT INTO 
                MixDiv (dividend, divisor) 
            VALUES 
                (12, 3.0), (12, 34.0), (12, -5.2),
                (12, NULL), (NULL, 34.0), (NULL, NULL)
            "#,
            Ok(Payload::Insert(6)),
        ),
        (
            "
            SELECT 
                DIV(dividend, divisor),
                MOD(dividend, divisor) 
            FROM MixDiv 
            ",
            Ok(select_with_null!(
                "DIV(dividend, divisor)" | "MOD(dividend, divisor)";
                Str("4".to_owned())         Str("0".to_owned());
                Str("0".to_owned())         Str("12".to_owned());
                Str("-2".to_owned())        Str("1.6".to_owned());
                Null                        Null;
                Null                        Null;
                Null                        Null
            )),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
