use {
    crate::*,
    gluesql_core::{executor::EvaluateError, prelude::Value::*},
};

test_case!(skip, {
    let g = get_tester!();

    g.run(
        "
            CREATE TABLE Test (
            id INTEGER,
            list LIST
            )",
    )
    .await;
    g.run("INSERT INTO Test (id, list) VALUES (1,'[1,2,3,4,5]')")
        .await;

    g.named_test(
        "skip function with normal usage",
        "SELECT SKIP(list, 2) as col1 FROM Test",
        Ok(select!(
            col1
            List;
            vec![I64(3), I64(4), I64(5)]
        )),
    )
    .await;
    g.named_test(
        "skip function with out of range index",
        "SELECT SKIP(list, 6) as col1 FROM Test",
        Ok(select!(
            col1
            List;
            [].to_vec()
        )),
    )
    .await;
    g.named_test(
        "skip function with null list",
        "SELECT SKIP(NULL, 2) as col1 FROM Test",
        Ok(select_with_null!(col1; Null)),
    )
    .await;
    g.named_test(
        "skip function with null size",
        "SELECT SKIP(list, NULL) as col1 FROM Test",
        Ok(select_with_null!(col1; Null)),
    )
    .await;
    g.named_test(
        "skip function with non integer parameter",
        "SELECT SKIP(list, 'd') as col1 FROM Test",
        Err(EvaluateError::FunctionRequiresIntegerValue("SKIP".to_owned()).into()),
    )
    .await;
    g.named_test(
        "skip function with non list",
        "SELECT SKIP(id, 2) as col1 FROM Test",
        Err(EvaluateError::ListTypeRequired.into()),
    )
    .await;
    g.named_test(
        "skip function with negative size",
        "SELECT SKIP(id, -2) as col1 FROM Test",
        Err(EvaluateError::FunctionRequiresUSizeValue("SKIP".to_owned()).into()),
    )
    .await;
});
