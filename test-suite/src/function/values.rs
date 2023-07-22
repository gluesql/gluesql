use {
    crate::*,
    gluesql_core::{error::EvaluateError, prelude::Value},
};

test_case!(values, async move {
    run!("CREATE TABLE USER (id INTEGER, data MAP);");
    run!(
        r#"
            INSERT INTO USER VALUES 
            (1, '{"id": "1", "name": "alice"}'),
            (2, '{"name": "bob"}'),
            (3, '{}');
        "#
    );

    test!(
        name: "return all values from map by ascending order",
        sql: r#"SELECT SORT(VALUES(data), 'ASC') as result FROM USER WHERE id=1"#,
        expected: {
            Ok(select!(result; Value::List; vec![Value::Str("1".to_owned()), Value::Str("alice".to_owned())]))
        }
    );
    test!(
        name: "return all values from map by descending order",
        sql: r#"SELECT SORT(VALUES(data), 'DESC') as result FROM USER WHERE id=1"#,
        expected: {
            Ok(select!(result; Value::List; vec![Value::Str("alice".to_owned()), Value::Str("1".to_owned())]))
        }
    );
    test!(
        name: "return all values from map",
        sql: r#"SELECT VALUES(data) as result FROM USER WHERE id=2"#,
        expected: Ok(select!(result; Value::List; vec![Value::Str("bob".to_owned())]))
    );
    test!(
        name: "return null from empty map",
        sql: r#"SELECT VALUES(data) as result FROM USER WHERE id=3"#,
        expected: Ok(select!(result; Value::List; vec![]))
    );
    test!(
        name: "return arguemnt type error",
        sql: r#"SELECT VALUES(id) FROM USER WHERE id=1"#,
        expected: Err(EvaluateError::MapTypeRequired.into())
    );
});
