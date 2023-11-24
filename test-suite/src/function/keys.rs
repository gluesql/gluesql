use {
    crate::*,
    gluesql_core::{error::EvaluateError, prelude::Value},
};

test_case!(keys, {
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
         "return all keys from map by ascending order",
        r#"SELECT SORT(KEYS(data), 'ASC') as result FROM USER WHERE id=1"#,
        {
            Ok(select!(result; Value::List; vec![Value::Str("id".to_owned()), Value::Str("is_male".to_owned()), Value::Str("name".to_owned())]))
        }
    ).await;
    g.named_test(
        "return one key from map",
        r#"SELECT KEYS(data) as result FROM USER WHERE id=2"#,
        Ok(select!(result; Value::List; vec![Value::Str("name".to_owned())])),
    )
    .await;
    g.named_test(
        "return null from empty map",
        r#"SELECT KEYS(data) as result FROM USER WHERE id=3"#,
        Ok(select!(result; Value::List; vec![])),
    )
    .await;
    g.named_test(
        "return argument type error",
        r#"SELECT KEYS(id) FROM USER WHERE id=1"#,
        Err(EvaluateError::MapTypeRequired.into()),
    )
    .await;
});
