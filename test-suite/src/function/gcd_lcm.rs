use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, ValueError},
        prelude::{Payload, Value::*},
    },
};

test_case!(gcd_lcm, {
    let g = get_tester!();

    let test_cases = [
        (
            "
            CREATE TABLE GcdI64 (
            left INTEGER NULL DEFAULT GCD(3, 4),
            right INTEGER NULL DEFAULT LCM(10, 2)
         )",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO GcdI64 VALUES (0, 3), (2,4), (6,8), (3,5), (1, NULL), (NULL, 1);",
            Ok(Payload::Insert(6)),
        ),
        (
            "SELECT GCD(left, right) AS test FROM GcdI64",
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
            "
            CREATE TABLE GcdStr (
            left TEXT,
            right INTEGER
         )",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO GcdStr VALUES ('TEXT', 0);",
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT GCD(left, right) AS test FROM GcdStr",
            Err(EvaluateError::FunctionRequiresIntegerValue("GCD".to_owned()).into()),
        ),
        (
            "SELECT GCD(right, left) AS test FROM GcdStr",
            Err(EvaluateError::FunctionRequiresIntegerValue("GCD".to_owned()).into()),
        ),
        (
            "
            CREATE TABLE LcmI64 (
            left INTEGER NULL DEFAULT true,
            right INTEGER NULL DEFAULT true
         )",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO LcmI64 VALUES (0, 3), (2,4), (6,8), (3,5), (1, NULL), (NULL, 1);",
            Ok(Payload::Insert(6)),
        ),
        (
            "SELECT LCM(left, right) AS test FROM LcmI64",
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
            "
            CREATE TABLE LcmStr (
            left TEXT,
            right INTEGER
         )",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO LcmStr VALUES ('TEXT', 0);",
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT LCM(left, right) AS test FROM LcmStr",
            Err(EvaluateError::FunctionRequiresIntegerValue("LCM".to_owned()).into()),
        ),
        (
            "SELECT LCM(right, left) AS test FROM LcmStr",
            Err(EvaluateError::FunctionRequiresIntegerValue("LCM".to_owned()).into()),
        ),
        // Check edge cases
        (
            "SELECT GCD(0, 0) as test",
            Ok(select_with_null!(test; I64(0))),
        ),
        (
            "VALUES(
                GCD(-1, -1),
                GCD(-2, 0),
                GCD(-14, 7)
            )",
            Ok(select_with_null!(
                column1 | column2 | column3;
                I64(1)    I64(2)    I64(7)
            )),
        ),
        (
            // check i64::MIN overflow error
            "SELECT GCD(-9223372036854775808, -9223372036854775808)",
            Err(ValueError::GcdLcmOverflow(i64::MIN).into()),
        ),
        (
            "SELECT LCM(0, 0) as test",
            Ok(select_with_null!(test; I64(0))),
        ),
        (
            "VALUES(
                LCM(-3, -5),
                LCM(-13, 0),
                LCM(-12, 2)
            )
            ",
            Ok(select_with_null!(
                column1 | column2 | column3;
                I64(15)   I64(0)    I64(12)
            )),
        ),
        (
            // check i64::MIN overflow error
            "SELECT LCM(-9223372036854775808, -9223372036854775808)",
            Err(ValueError::GcdLcmOverflow(i64::MIN).into()),
        ),
        (
            // 10^10 + 19 and 10^10 + 33 are prime numbers
            // LCM(10^10+19, 10^10+33) = (10^10+19)*(10^10+33)
            // this result is out of i64 range.
            "SELECT LCM(10000000019, 10000000033)",
            Err(ValueError::LcmResultOutOfRange.into()),
        ),
        (
            "SELECT gcd(1.0, 1);",
            Err(EvaluateError::FunctionRequiresIntegerValue("GCD".to_owned()).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
