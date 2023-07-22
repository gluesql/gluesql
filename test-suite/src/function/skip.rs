use {
    crate::*,
    gluesql_core::{executor::EvaluateError, prelude::Value::*},
};
test_case!(skip, async move {
    run!(
        "
            CREATE TABLE Test (
            id INTEGER,
            list LIST
            )"
    );
    run!("INSERT INTO Test (id, list) VALUES (1,'[1,2,3,4,5]')");

    test! (
        name: "skip function with normal usage",
        sql : "SELECT SKIP(list, 2) as col1 FROM Test",
        expected : Ok(select!(
                            col1
                            List;
                            vec![I64(3), I64(4), I64(5)]
                        ))
    );
    test! (
        name: "skip function with out of range index",
        sql : "SELECT SKIP(list, 6) as col1 FROM Test",
        expected : Ok(select!(
                            col1
                            List;
                            [].to_vec()
                        ))
    );
    test! (
        name: "skip function with null list",
        sql : "SELECT SKIP(NULL, 2) as col1 FROM Test",
        expected : Ok(select_with_null!(col1; Null))
    );
    test! (
        name: "skip function with null size",
        sql : "SELECT SKIP(list, NULL) as col1 FROM Test",
        expected : Ok(select_with_null!(col1; Null))
    );
    test! (
        name: "skip function with non integer parameter",
        sql : "SELECT SKIP(list, 'd') as col1 FROM Test",
        expected : Err(EvaluateError::FunctionRequiresIntegerValue("SKIP".to_owned()).into())
    );
    test! (
        name: "skip function with non list",
        sql : "SELECT SKIP(id, 2) as col1 FROM Test",
        expected : Err(EvaluateError::ListTypeRequired.into())
    );
    test! (
        name: "skip function with negative size",
        sql : "SELECT SKIP(id, -2) as col1 FROM Test",
        expected : Err(EvaluateError::FunctionRequiresUSizeValue("SKIP".to_owned()).into())
    );
});