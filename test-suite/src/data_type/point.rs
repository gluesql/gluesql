use {
    crate::*,
    bigdecimal::BigDecimal,
    gluesql_core::{
        ast::DataType,
        data::{Literal, ValueError},
        executor::Payload,
        prelude::Value::Point,
    },
    std::borrow::Cow,
};

test_case!(point, async move {
    let test_cases = [
        (
            "CREATE TABLE POINT (point_field POINT)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO POINT VALUES (0)"#,
            Err(ValueError::IncompatibleLiteralForDataType {
                data_type: DataType::Point,
                literal: format!("{:?}", Literal::Number(Cow::Owned(BigDecimal::from(0)))),
            }
            .into()),
        ),
        (
            r#"INSERT INTO POINT VALUES (POINT(0.3134, 0.156))"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT point_field AS point_field FROM POINT;"#,
            Ok(select!(
                point_field
                Point;
                gluesql_core::data::Point::new(0.3134, 0.156)
            )),
        ),
        (
            r#"UPDATE POINT SET point_field = 'POINT(2.0, 1.0)' WHERE point_field='POINT(2.0, 1.0)'"#,
            Ok(Payload::Update(1)),
        ),
        (
            r#"SELECT point_field AS point_field FROM POINT"#,
            Ok(select!(
                point_field
                Point;
                gluesql_core::data::Point::new(2.0, 1.0)
            )),
        ),
        (
            r#"DELETE FROM POINT WHERE point_field='POINT(2.0, 1.0)'"#,
            Ok(Payload::Delete(0)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
