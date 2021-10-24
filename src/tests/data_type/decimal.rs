use {
    crate::*
};

test_case!(decimal, async move {
    use Value::Decimal;
    // use rust_decimal::Decimal;

    let test_cases = vec![
        ("CREATE TABLE DECIMAL_ITEM (decimal_field DECIMAL)", Ok(Payload::Create)),
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
            ))
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});