use {
    crate::*,
    bigdecimal::BigDecimal,
    gluesql_core::{
        data::RowError,
        data::{Literal, ValueError},
        prelude::DataType,
        prelude::Value::*,
    },
    std::borrow::Cow,
};

test_case!(values, async move {
    let test_cases = vec![
        (
            "VALUES (1), (2), (3)",
            Ok(select!(
                column1;
                I64;
                1;
                2;
                3
            )),
        ),
        (
            "VALUES (1, 'a'), (2, 'b')",
            Ok(select!(
                column1 | column2;
                I64     | Str;
                1         "a".to_owned();
                2         "b".to_owned()
            )),
        ),
        (
            "VALUES (1), (2) limit 1",
            Ok(select!(
                column1;
                I64;
                1
            )),
        ),
        (
            "VALUES (1), (2) offset 1",
            Ok(select!(
                column1;
                I64;
                2
            )),
        ),
        (
            "VALUES (1, NULL), (2, NULL)",
            Ok(select_with_null!(
                column1 | column2;
                I64(1)    Null;
                I64(2)    Null
            )),
        ),
        (
            "VALUES (1), (2, 'b')",
            Err(RowError::NumberOfValuesDifferent.into()),
        ),
        (
            "VALUES (1, 'a'), (2)",
            Err(RowError::NumberOfValuesDifferent.into()),
        ),
        (
            "VALUES (1, 'a'), (2, 3)",
            Err(ValueError::IncompatibleLiteralForDataType {
                data_type: DataType::Text,
                literal: format!("{:?}", Literal::Number(Cow::Owned(BigDecimal::from(3)))),
            }
            .into()),
        ),
        (
            "VALUES (1, 'a'), ('b', 'c')",
            Err(ValueError::IncompatibleLiteralForDataType {
                data_type: DataType::Int,
                literal: format!("{:?}", Literal::Text(Cow::Owned("b".to_owned()))),
            }
            .into()),
        ),
        (
            "VALUES (1, NULL), (2, 'a'), (3, 4)",
            Err(ValueError::IncompatibleLiteralForDataType {
                data_type: DataType::Text,
                literal: format!("{:?}", Literal::Number(Cow::Owned(BigDecimal::from(4)))),
            }
            .into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
