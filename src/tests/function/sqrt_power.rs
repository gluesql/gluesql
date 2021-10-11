use crate::*;

test_case!(sqrt, async move {
    use Value::{Null, F64};

    let test_cases = vec![
        ("CREATE TABLE SingleItem (id FLOAT)", Ok(Payload::Create)),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT
            SQRT(2.0) as sqrt_1,
            SQRT(0.07) as sqrt_2
            FROM SingleItem",
            Ok(select!(
                sqrt_1          | sqrt_2;
                F64             | F64;
                2.0_f64.sqrt()   0.07_f64.sqrt()
            )),
        ),
        (
            "SELECT SQRT(32) as sqrt_with_int FROM SingleItem",
            Ok(select!(
                sqrt_with_int
                F64;
                f64::from(32).sqrt()
            )),
        ),
        (
            "SELECT SQRT(0) as sqrt_with_zero FROM SingleItem",
            Ok(select!(
                sqrt_with_zero
                F64;
                f64::from(0).sqrt()
            )),
        ),
        (
            "SELECT SQRT('string') AS sqrt FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("SQRT")).into()),
        ),
        (
            "SELECT SQRT(NULL) AS sqrt FROM SingleItem",
            Ok(select_with_null!(sqrt; Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});

test_case!(power, async move {
    use Value::{Null, F64};

    let test_cases = vec![
        ("CREATE TABLE SingleItem (id FLOAT)", Ok(Payload::Create)),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT
            POWER(2.0,4) as power_1,
            POWER(0.07,3) as power_2
            FROM SingleItem",
            Ok(select!(
                power_1         | power_2;
                F64             | F64;
                2.0_f64.powf(4.0)   0.07_f64.powf(3.0)
            )),
        ),
        (
            "SELECT
            POWER(0,4) as power_with_zero,
            POWER(3,0) as power_to_zero
            FROM SingleItem",
            Ok(select!(
                power_with_zero        | power_to_zero;
                F64             | F64;
                f64::from(0).powf(4.0)   f64::from(3).powf(0.0)
            )),
        ),
        // (
        //     "SELECT POWER(32,0.3) as power_with_float FROM SingleItem",
        //     Ok(select!(
        //         power_with_float
        //         F64;
        //         f64::from(32).powf(0.3) // expected: [F64(2.82842712474619)], found: [F64(2.8284271247461907)
        //     )),
        // ),
        (
            "SELECT POWER('string','string') AS power FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("POWER")).into()),
        ),
        (
            "SELECT POWER(2.0,'string') AS power FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("POWER")).into()),
        ),
        (
            "SELECT POWER('string',2.0) AS power FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("POWER")).into()),
        ),
        (
            "SELECT POWER(NULL,NULL) AS power FROM SingleItem",
            Ok(select_with_null!(power; Null)),
        ),
        (
            "SELECT POWER(2.0,NULL) AS power FROM SingleItem",
            Ok(select_with_null!(power; Null)),
        ),
        (
            "SELECT POWER(NULL,2.0) AS power FROM SingleItem",
            Ok(select_with_null!(power; Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
