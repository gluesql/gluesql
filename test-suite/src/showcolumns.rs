use crate::*;

test_case!(showcolumns, async move {
    use gluesql_core::{executor::ExecuteError, prelude::Value::*};

    run!(
        "
        CREATE TABLE mytable (
            id INTEGER,
            rate FLOAT,
            flag BOOLEAN,
            text TEXT,
            null_value TEXT NULL,
        );
    "
    );

    test!(
        Ok(select!(
            Field               | Type
            Str                 | Str;
            "id".to_owned()       "Int".to_owned();
            "rate".to_owned()     "Float".to_owned();
            "flag".to_owned()     "Boolean".to_owned();
            "text".to_owned()     "Text".to_owned();
            "null_value".to_owned()   "Text".to_owned()
        )),
        r#"Show columns from mytable"#
    );

    test!(
        Err(ExecuteError::TableNotFound("mytable1".to_owned()).into()),
        r#"Show columns from mytable1"#
    );
});
