use {
    crate::*,
    gluesql_core::{error::EvaluateError, prelude::Value},
};

test_case!(values, async move {
    run!("CREATE TABLE USER (id INTEGER, data MAP);");
    run!(
        r#"
            INSERT INTO USER VALUES 
            (1, '{"id": 1, "name": "alice"}'),
            (2, '{"name": "bob"}'),
            (3, '{}');
        "#
    );

    test!(
        name: "return all values from map",
        sql: r#"SELECT VALUES(data) as result FROM USER WHERE id=1"#,
        expected: { // result list is sorted by key of map
            Ok(select!(result; Value::List; [Value::I64(1), Value::Str("alice".to_owned())].to_vec()))
        }
    );
    test!(
        name: "return all values from map",
        sql: r#"SELECT VALUES(data) as result FROM USER WHERE id=2"#,
        expected: Ok(select!(result; Value::List; [Value::Str("bob".to_owned())].to_vec()))
    );
    test!(
        name: "return null from empty map",
        sql: r#"SELECT VALUES(data) as result FROM USER WHERE id=3"#,
        expected: Ok(select!(result; Value::List; [].to_vec()))
    );

    // Error
    test!(
        name: "return arguemnt type error",
        sql: r#"SELECT VALUES(id) FROM USER WHERE id=1"#,
        expected: Err(EvaluateError::MapTypeRequired.into())
    );
});
