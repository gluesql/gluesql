use {
    crate::*,
    gluesql_core::{error::EvaluateError, prelude::Value::*},
};

test_case!(sort, async move {
    run!("CREATE TABLE Test1 (list LIST)");
    run!("INSERT INTO Test1 (list) VALUES ('[2, 1, 4, 3]')");

    test! (
        name: "sort the list by default order",
        sql : "SELECT SORT(list) AS list FROM Test1",
        expected : Ok(select!(list List; vec![I64(1), I64(2), I64(3), I64(4)]))
    );

    test! (
        name: "sort the list by ascending order",
        sql : "SELECT SORT(list, 'ASC') AS list FROM Test1",
        expected : Ok(select!(list List; vec![I64(1), I64(2), I64(3), I64(4)]))
    );

    test! (
        name: "sort the list by descending order",
        sql : "SELECT SORT(list, 'DESC') AS list FROM Test1",
        expected : Ok(select!(list List; vec![I64(4), I64(3), I64(2), I64(1)]))
    );

    test! (
        name: "sort the list by wrong order",
        sql : "SELECT SORT(list, 'WRONG') AS list FROM Test1",
        expected : Err(EvaluateError::InvalidSortOrder.into())
    );

    test! (
        name: "sort the list with not String typed order",
        sql : "SELECT SORT(list, 1) AS list FROM Test1",
        expected : Err(EvaluateError::InvalidSortOrder.into())
    );

    run!("CREATE TABLE Test2 (id INTEGER, list LIST)");
    run!("INSERT INTO Test2 (id, list) VALUES (1, '[2, \"1\", [\"a\", \"b\"], 3]')");

    test! (
        name: "sort non-LIST items",
        sql : "SELECT SORT(id) AS list FROM Test2",
        expected : Err(EvaluateError::ListTypeRequired.into())
    );

    test! (
        name: "sort the list with not comparable types",
        sql : "SELECT SORT(list) AS list FROM Test2",
        expected : Err(EvaluateError::InvalidSortType.into())
    );
});
