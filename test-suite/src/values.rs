use {
    crate::*,
    gluesql_core::{data::RowError, prelude::Value::*},
};

test_case!(values, async move {
    let test_cases = vec![
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
    ];
    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
