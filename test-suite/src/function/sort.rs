use {
    crate::*,
    gluesql_core::{error::EvaluateError, prelude::Value::*},
};

test_case!(sort, {
    let g = get_tester!();

    g.run("CREATE TABLE Test1 (list LIST)").await;
    g.run("INSERT INTO Test1 (list) VALUES ('[2, 1, 4, 3]')")
        .await;

    g.named_test(
        "sort the list by default order",
        "SELECT SORT(list) AS list FROM Test1",
        Ok(select!(list List; vec![I64(1), I64(2), I64(3), I64(4)])),
    )
    .await;

    g.named_test(
        "sort the list by ascending order",
        "SELECT SORT(list, 'ASC') AS list FROM Test1",
        Ok(select!(list List; vec![I64(1), I64(2), I64(3), I64(4)])),
    )
    .await;

    g.named_test(
        "sort the list by descending order",
        "SELECT SORT(list, 'DESC') AS list FROM Test1",
        Ok(select!(list List; vec![I64(4), I64(3), I64(2), I64(1)])),
    )
    .await;

    g.named_test(
        "sort the list by wrong order",
        "SELECT SORT(list, 'WRONG') AS list FROM Test1",
        Err(EvaluateError::InvalidSortOrder.into()),
    )
    .await;

    g.named_test(
        "sort the list with not String typed order",
        "SELECT SORT(list, 1) AS list FROM Test1",
        Err(EvaluateError::InvalidSortOrder.into()),
    )
    .await;

    g.run("CREATE TABLE Test2 (id INTEGER, list LIST)").await;
    g.run("INSERT INTO Test2 (id, list) VALUES (1, '[2, \"1\", [\"a\", \"b\"], 3]')")
        .await;

    g.named_test(
        "sort non-LIST items",
        "SELECT SORT(id) AS list FROM Test2",
        Err(EvaluateError::ListTypeRequired.into()),
    )
    .await;

    g.named_test(
        "sort the list with not comparable types",
        "SELECT SORT(list) AS list FROM Test2",
        Err(EvaluateError::InvalidSortType.into()),
    )
    .await;
});
