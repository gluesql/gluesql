use crate::*;

test_case!(unary_operator, async move {
    use Value::*;

    let test_cases = vec![
        (
            "CREATE TABLE Test (v1 INT, v2 FLOAT, v3 TEXT, v4 INT, v5 INT)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO Test VALUES (10, 10.5, "hello", -5, 1000)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT * FROM Test",
            Ok(select!(
                v1  |   v2  |   v3  |               v4  |           v5
                I64 |   F64 |   Str |               I64 |           I64;
                10      10.5    "hello".to_owned()  i64::from(-5)   1000
            )),
        ),
        (
            "SELECT -v1 as v1, -v2 as v2, v3, -v4 as v4 FROM Test",
            Ok(select!(
                v1  |           v2  |                   v3  |                   v4
                I64 |           F64 |                   Str |                   I64;
                i64::from(-10)  f64::from(-10.5)  "hello".to_owned()    5
            )),
        ),
        (
            "SELECT -(-10) as v1, -(-10) as v2 FROM Test",
            Ok(select!(
                v1  |   v2
                I64 |   I64;
                10      10
            )),
        ),
        (
            "SELECT -v3 as v3 FROM Test",
            Err(ValueError::UnaryMinusOnNonNumeric.into()),
        ),
        (
            "SELECT -'errrr' as v1 FROM Test",
            Err(LiteralError::UnaryOperationOnNonNumeric.into()),
        ),
        (
            "SELECT +10 as v1, +(+10) as v2 FROM Test",
            Ok(select!(
                v1  |   v2
                I64 |   I64;
                10      10
            )),
        ),
        (
            "SELECT +v3 as v3 FROM Test",
            Err(ValueError::UnaryPlusOnNonNumeric.into()),
        ),
        (
            "SELECT +'errrr' as v1 FROM Test",
            Err(LiteralError::UnaryOperationOnNonNumeric.into()),
        ),
        (
            "SELECT v1! as v1 FROM Test",
            Ok(select!(
                v1
                I64;
                3628800
            )),
        ),
        (
            "SELECT 4! as v1 FROM Test",
            Ok(select!(
                v1
                I64;
                24
            )),
        ),
        (
            "SELECT v2! as v2 FROM Test",
            Err(ValueError::UnaryFactorialOnNonNumeric.into()),
        ),
        (
            "SELECT v3! as v1 FROM Test",
            Err(ValueError::UnaryFactorialOnNonNumeric.into()),
        ),
        (
            "SELECT v4! as v4 FROM Test",
            Err(ValueError::UnaryFactorialOnNegativeNumeric.into()),
        ),
        (
            "SELECT v5! as v5 FROM Test",
            Err(ValueError::UnaryFactorialOverflow.into()),
        ),
        (
            "SELECT (-5)! as v4 FROM Test",
            Err(LiteralError::UnaryFactorialOperationOnNegativeNumeric.into()),
        ),
        (
            "SELECT (5.5)! as v4 FROM Test",
            Err(LiteralError::UnreachableUnaryOperation.into()),
        ),
        (
            "SELECT 'errrr'! as v1 FROM Test",
            Err(LiteralError::UnaryOperationOnNonNumeric.into()),
        ),
        (
            "SELECT 1000! as v4 FROM Test",
            Err(LiteralError::UnaryFactorialOperationOverflow.into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
