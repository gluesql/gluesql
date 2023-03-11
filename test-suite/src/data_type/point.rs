use {
    crate::*,
    bigdecimal::BigDecimal,
    gluesql_core::{
        ast::DataType,
        data::{Literal, ValueError},
        executor::Payload,
        prelude::Value::Point,
        result::Error,
    },
    std::borrow::Cow,
};

test_case!(point, async move {
    let parse_point = |value: &str| {
        let v = value.replace("POINT(", "").replace(")", "");
        let mut split = v.split_whitespace();
        let x = split.next();
        let y = split.next();

        match (x, y) {
            (Some(x), Some(y)) => Ok((x.parse::<f64>().unwrap(), y.parse::<f64>().unwrap())),
            (_, _) => Err(Error::Value(ValueError::FailedToParsePoint(
                value.to_owned(),
            ))),
        }
    };

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
            r#"INSERT INTO POINT VALUES (X'1234')"#,
            Err(ValueError::FailedToParsePoint("1234".to_owned()).into()),
        ),
        (
            r#"INSERT INTO POINT VALUES ('NOT_POINT')"#,
            Err(ValueError::FailedToParsePoint("NOT_UUID".to_owned()).into()),
        ),
        (
            r#"INSERT INTO POINT VALUES (POINT(1.0 2.0))"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT point_field AS point_field FROM POINT;"#,
            Ok(select!(
                point_field
                Point;
                parse_point("POINT(1.0 2.0)").unwrap()
            )),
        ),
        (
            r#"UPDATE POINT SET point_field = 'POINT(2.0 1.0)' WHERE point_field='POINT(2.0 1.0)'"#,
            Ok(Payload::Update(1)),
        ),
        (
            r#"SELECT point_field AS point_field, COUNT(*) FROM POINT GROUP BY point_field"#,
            Ok(select!(
                point_field
                Point;
                parse_point("POINT(2.0 1.0)").unwrap()
            )),
        ),
        (
            r#"DELETE FROM POINT WHERE point_field='POINT(2.0 1.0)'"#,
            Ok(Payload::Delete(0)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
