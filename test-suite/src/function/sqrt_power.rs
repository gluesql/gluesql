use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, ValueError},
        prelude::Value::*,
    },
};

test_case!(sqrt, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT
                SQRT(4.0) as sqrt_1,
                SQRT(0.07) as sqrt_2
            ;",
            Ok(select!(
                sqrt_1 | sqrt_2;
                F64    | F64;
                2.0      0.07_f64.sqrt()
            )),
        ),
        (
            "SELECT SQRT(64) as sqrt_with_int",
            Ok(select!(
                sqrt_with_int
                F64;
                8.0
            )),
        ),
        (
            "SELECT SQRT(0) as sqrt_with_zero",
            Ok(select!(
                sqrt_with_zero
                F64;
                0.0
            )),
        ),
        (
            "SELECT SQRT('string') AS sqrt",
            Err(ValueError::SqrtOnNonNumeric(Str("string".to_owned())).into()),
        ),
        (
            "SELECT SQRT(NULL) AS sqrt",
            Ok(select_with_null!(sqrt; Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});

test_case!(power, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT
                POWER(2.0,4) as power_1,
                POWER(0.07,3) as power_2
            ;",
            Ok(select!(
                power_1 | power_2;
                F64     | F64;
                16.0      0.07_f64.powf(3.0)
            )),
        ),
        (
            "SELECT
                POWER(0,4) as power_with_zero,
                POWER(3,0) as power_to_zero
            ;",
            Ok(select!(
                power_with_zero | power_to_zero;
                F64             | F64;
                0.0               1.0
            )),
        ),
        (
            "SELECT POWER(32,3.0) as power_with_float",
            Ok(select!(
                power_with_float
                F64;
                f64::from(32).powf(3.0)
            )),
        ),
        (
            "SELECT POWER('string','string') AS power",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("POWER")).into()),
        ),
        (
            "SELECT POWER(2.0,'string') AS power",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("POWER")).into()),
        ),
        (
            "SELECT POWER('string',2.0) AS power",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("POWER")).into()),
        ),
        (
            "SELECT POWER(NULL,NULL) AS power",
            Ok(select_with_null!(power; Null)),
        ),
        (
            "SELECT POWER(2.0,NULL) AS power",
            Ok(select_with_null!(power; Null)),
        ),
        (
            "SELECT POWER(NULL,2.0) AS power",
            Ok(select_with_null!(power; Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
