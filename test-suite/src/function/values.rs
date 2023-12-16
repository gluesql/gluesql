use {
    crate::*,
    gluesql_core::{error::EvaluateError, prelude::Value},
};

test_case!(values, {
    let g = get_tester!();

    g.run("CREATE TABLE USER (id INTEGER, data MAP);").await;
    g.run(
        r#"
            INSERT INTO USER VALUES 
            (1, '{"id": 1, "name": "alice", "is_male": false}'),
            (2, '{"name": "bob"}'),
            (3, '{}');
        "#,
    )
    .await;

    g.named_test(
         "return all values from map by descending order",
        r#"SELECT SORT(VALUES(data), 'DESC') as result FROM USER WHERE id=1"#,
        {
            Ok(select!(result; Value::List; vec![Value::I64(1), Value::Bool(false), Value::Str("alice".to_owned())]))
        }
    ).await;
    g.named_test(
         "return all values from map by ascending order",
        r#"SELECT SORT(VALUES(data), 'ASC') as result FROM USER WHERE id=1"#,
        {
            Ok(select!(result; Value::List; vec![Value::Str("alice".to_owned()), Value::Bool(false), Value::I64(1)]))
        }
    ).await;
    g.named_test(
        "return all values from map",
        r#"SELECT VALUES(data) as result FROM USER WHERE id=2"#,
        Ok(select!(result; Value::List; vec![Value::Str("bob".to_owned())])),
    )
    .await;
    g.named_test(
        "return null from empty map",
        r#"SELECT VALUES(data) as result FROM USER WHERE id=3"#,
        Ok(select!(result; Value::List; vec![])),
    )
    .await;
    g.named_test(
        "return argument type error",
        r#"SELECT VALUES(id) FROM USER WHERE id=1"#,
        Err(EvaluateError::MapTypeRequired.into()),
    )
    .await;
});
