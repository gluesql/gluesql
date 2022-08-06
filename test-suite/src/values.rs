use {
    crate::*,
    bigdecimal::BigDecimal,
    gluesql_core::{
        data::{Literal, RowError, ValueError},
        prelude::{DataType, Payload, Value::*},
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
        (
            "CREATE TABLE TableFromValues AS VALUES (1, 'a', True, Null, Null), (2, 'b', False, 3, Null)",
            Ok(Payload::Create),
        ),
        (
            "SELECT * FROM TableFromValues",
            Ok(select_with_null!(
                column1 | column2         | column3    | column4 | column5;
                I64(1)    Str("a".into())   Bool(true)   Null      Null   ;
                I64(2)    Str("b".into())   Bool(false)  I64(3)    Null
            )),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});

test_case!(type_match, async {
    type_match!(
        &[
            DataType::Int,
            DataType::Text,
            DataType::Boolean,
            DataType::Int,
            DataType::Text,
        ],
        "SELECT * FROM TableFromValues"
    );
});
