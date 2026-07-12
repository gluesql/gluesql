use super::*;

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
        g.test(sql, expected);
    }
});
