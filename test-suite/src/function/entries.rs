use {
    crate::*,
    gluesql_core::{
        error::EvaluateError,
        prelude::{Payload, Value::*},
    },
};

test_case!(entries, {
    let g = get_tester!();

    g.named_test(
        "test entries function works while creating a table simultaneously",
        "CREATE TABLE Item (map MAP)",
        Ok(Payload::Create),
    )
    .await;
    g.named_test(
        "test if the sample string gets inserted to table",
        r#"INSERT INTO Item VALUES ('{"name":"GlueSQL"}')"#,
        Ok(Payload::Insert(1)),
    )
    .await;
    g.named_test(
        "check id the entries function works with the previously inserted string",
        "SELECT ENTRIES(map) AS test FROM Item",
        Ok(select!(
            "test";
            List;
            vec![
                List(vec![Str("name".to_owned()), Str("GlueSQL".to_owned())])
            ]
        )),
    )
    .await;
    g.named_test(
        "test ENTRIES function requires map value",
        "SELECT ENTRIES(1) FROM Item",
        Err(EvaluateError::FunctionRequiresMapValue("ENTRIES".to_owned()).into()),
    )
    .await;
});
