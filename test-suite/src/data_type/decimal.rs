use {
    crate::*,
    gluesql_core::{executor::AlterError, executor::Payload, prelude::Value::*},
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
            // Err(ValueError::IncompatibleDataType {
            //     data_type: DataType::Decimal(Some(1),Some(4)).clone(),
            //     value: Value::Decimal(0.into()).clone(),
            // }).into(),
        ),
        (
            "CREATE TABLE DECIMAL_EXTENDED (d1 DECIMAL(5), d2 DECIMAL(5,2))",
            Ok(Payload::Create),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
