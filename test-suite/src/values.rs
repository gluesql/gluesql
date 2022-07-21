use std::borrow::Cow;

use bigdecimal::BigDecimal;
use gluesql_core::{
    data::{Literal, ValueError},
    prelude::DataType,
};

use {
    crate::*,
    gluesql_core::{data::RowError, prelude::Value::*},
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
            "VALUES (1), (2, 'b')",
            Err(RowError::NumberOfValuesDifferent.into()),
        ),
        (
            "VALUES (1, 'a'), (2)",
            Err(RowError::NumberOfValuesDifferent.into()),
        ),
        // (
        //     "VALUES (1, 'a'), (2, 3)",
        //     Err(RowError::ValuesTypeDifferent("Str".into(), "Int".into()).into()),
        // ),
        // (
        //     "VALUES (1, 'a'), ('b', 'c')",
        //     Err(RowError::ValuesTypeDifferent("Int".into(), "Str".into()).into()),
        // ),
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
            "VALUES (1, NULL)",
            Ok(select_with_null!(
                column1 | column2;
                I64(1)    Null
            )),
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
