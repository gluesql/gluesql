use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value::*},
        translate::TranslateError,
    },
};

test_case!(div_mod, async move {
    let eval_div = |dividend, divisor| (dividend / divisor) as i64;
    let eval_mod = |dividend, divisor| dividend % divisor;
    let test_cases = [
        (
            "CREATE TABLE FloatDiv (
                dividend FLOAT DEFAULT MOD(30, 11),
                divisor FLOAT DEFAULT DIV(3, 2)
            )",
            Ok(Payload::Create),
        ),
        (
            "
            INSERT INTO 
                FloatDiv (dividend, divisor) 
            VALUES 
                (12.0, 3.0), (12.34, 56.78), (-12.3, 4.0)
            ",
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
                I64                      | F64;
                eval_div(12.0, 3.0)        eval_mod(12.0, 3.0);
                eval_div(12.34, 56.78)     eval_mod(12.34, 56.78);
                eval_div(-12.3, 4.0)       eval_mod(-12.3, 4.0)
            )),
        ),
        (
            "SELECT DIV(1.0, 0.0) AS quotient FROM FloatDiv",
            Err(EvaluateError::DivisorShouldNotBeZero.into()),
        ),
        (
            "SELECT DIV(1.0, 'dividend') AS quotient FROM FloatDiv",
            Err(EvaluateError::FunctionRequiresFloatOrIntegerValue("DIV".to_owned()).into()),
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
            "
            INSERT INTO 
                IntDiv (dividend, divisor) 
            VALUES 
                (12, 3), (12, 7), (12, 34), (-12, 7)
            ",
            Ok(Payload::Insert(4)),
        ),
        (
            "INSERT INTO IntDiv (dividend, divisor) VALUES (12, 2.0)",
            Ok(Payload::Insert(1)),
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
                I64                      | F64;
                eval_div(12_f64, 3_f64)    eval_mod(12_f64, 3_f64);
                eval_div(12_f64, 7_f64)    eval_mod(12_f64, 7_f64);
                eval_div(12_f64, 34_f64)   eval_mod(12_f64, 34_f64);
                eval_div(-12_f64, 7_f64)   eval_mod(-12_f64, 7_f64);
                eval_div(12_f64, 2_f64)   eval_mod(12_f64, 2_f64)
            )),
        ),
        (
            "SELECT DIV(1, 0) AS quotient FROM IntDiv",
            Err(EvaluateError::DivisorShouldNotBeZero.into()),
        ),
        (
            "CREATE TABLE MixDiv (dividend INTEGER NULL, divisor FLOAT NULL)",
            Ok(Payload::Create),
        ),
        (
            "
            INSERT INTO 
                MixDiv (dividend, divisor) 
            VALUES 
                (12, 3.0), (12, 34.0),
                (12, NULL), (NULL, 34.0), (NULL, NULL)
<<<<<<< HEAD
            "#,
            Ok(Payload::Insert(5)),
=======
            ",
            Ok(Payload::Insert(6)),
>>>>>>> main
        ),
        (
            "
            SELECT 
                DIV(dividend, divisor),
                MOD(dividend, divisor) 
            FROM MixDiv 
            ",
            Ok(select_with_null!(
                "DIV(dividend, divisor)"    | "MOD(dividend, divisor)";
                I64(eval_div(12_f64, 3.0))    F64(eval_mod(12_f64, 3.0));
                I64(eval_div(12_f64, 34.0))   F64(eval_mod(12_f64, 34.0));
                Null                          Null;
                Null                          Null;
                Null                          Null
            )),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
