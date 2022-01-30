use {
    crate::*,
    gluesql_core::{executor::AlterError, executor::Payload, prelude::Value::*, data::ValueError},
    rust_decimal::prelude::*,  
    rust_decimal::Decimal,
};

test_case!(decimal, async move {
    let test_cases = vec![
        (
            "CREATE TABLE DECIMAL_ITEM (decimal_field DECIMAL)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO DECIMAL_ITEM VALUES (1)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT decimal_field AS decimal_field FROM DECIMAL_ITEM"#,
            Ok(select!(
                decimal_field
                Decimal;
                1.into()
            )),
        ),
        (
            "CREATE TABLE ILLEGAL_DECIMAL (d1 DECIMAL(1,4))",
            Err(AlterError::UnsupportedDecimalScale("4".to_owned(), "1".to_owned()).into()),
        ),
        (
            "CREATE TABLE DECIMAL_PRECISION (d1 DECIMAL(5))",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO DECIMAL_PRECISION (d1) VALUES (12345)",
            Ok(Payload::Insert(1)),
        ),
        (
            "INSERT INTO DECIMAL_PRECISION (d1) VALUES (123456)",
            Err(ValueError::FailedToParseDecimal("123456".to_owned()).into()),
        ),
        (
            "CREATE TABLE DECIMAL_PRECISION_SCALE (d1 DECIMAL(5,2))",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO DECIMAL_PRECISION_SCALE (d1) VALUES (1234.56)",
            Err(ValueError::FailedToParseDecimal("1234.56".to_owned()).into()),
        ),
        (
            "INSERT INTO DECIMAL_PRECISION_SCALE (d1) VALUES (123.456)",
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT d1 AS d1 FROM DECIMAL_PRECISION_SCALE",
            Ok(select!(
                d1
                Decimal;
                rust_decimal::Decimal::from_str("123.46").unwrap()
            ))
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
