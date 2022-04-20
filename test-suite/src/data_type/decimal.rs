use {
    crate::*,
    gluesql_core::{executor::Payload, prelude::Value::*},
    rust_decimal::prelude::Decimal as Dec,
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
                Dec::ONE
            )),
        ),
        (
            r#"SELECT decimal_field +1 as decimal_field FROM DECIMAL_ITEM"#,
            Ok(select!(
                decimal_field
                Decimal;
                Dec::TWO
            )),
        ),
        (
            r#"SELECT 1+ decimal_field  as decimal_field FROM DECIMAL_ITEM"#,
            Ok(select!(
                decimal_field
                Decimal;
                Dec::TWO
            )),
        ),
        (
            r#"SELECT decimal_field -1 as decimal_field FROM DECIMAL_ITEM"#,
            Ok(select!(
                decimal_field
                Decimal;
                Dec::ZERO
            )),
        ),
        (
            r#"SELECT 1- decimal_field  as decimal_field FROM DECIMAL_ITEM"#,
            Ok(select!(
                decimal_field
                Decimal;
                Dec::ZERO
            )),
        ),
        (
            r#"SELECT decimal_field * 2 as decimal_field FROM DECIMAL_ITEM"#,
            Ok(select!(
                decimal_field
                Decimal;
                Dec::TWO
            )),
        ),
        (
            r#"SELECT 2* decimal_field  as decimal_field FROM DECIMAL_ITEM"#,
            Ok(select!(
                decimal_field
                Decimal;
                Dec::TWO
            )),
        ),
        (
            r#"SELECT decimal_field/2 as decimal_field FROM DECIMAL_ITEM"#,
            Ok(select!(
                decimal_field
                Decimal;
                Dec::from_f64_retain(0.5f64).unwrap()
            )),
        ),
        (
            r#"SELECT 2/decimal_field  as decimal_field FROM DECIMAL_ITEM"#,
            Ok(select!(
                decimal_field
                Decimal;
                Dec::TWO
            )),
        ),
        (
            r#"SELECT 2%decimal_field  as decimal_field FROM DECIMAL_ITEM"#,
            Ok(select!(
                decimal_field
                Decimal;
                Dec::ZERO
            )),
        ),
        (
            r#"SELECT decimal_field % 2  as decimal_field FROM DECIMAL_ITEM"#,
            Ok(select!(
                decimal_field
                Decimal;
                Dec::ONE
            )),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
