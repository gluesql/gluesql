use {crate::*, gluesql_core::prelude::Value::*};

test_case!(null, {
    let g = get_tester!();

    g.named_test(
        "NULL = NULL should return NULL",
        "SELECT NULL = NULL as res;",
        Ok(select_with_null!(res; Null)),
    )
    .await;

    g.named_test(
        "NULL IS NULL should return true",
        "SELECT NULL IS NULL as res;",
        Ok(select!(res Bool; true)),
    )
    .await;

    g.named_test(
        "NOT NULL should return NULL",
        "SELECT NOT NULL as res;",
        Ok(select_with_null!(res; Null)),
    )
    .await;

    g.named_test(
        "NULL > NULL should return NULL",
        "SELECT NULL > NULL as res;",
        Ok(select_with_null!(res; Null)),
    )
    .await;

    g.named_test(
        "NULL < NULL should return NULL",
        "SELECT NULL < NULL as res;",
        Ok(select_with_null!(res; Null)),
    )
    .await;

    g.named_test(
        "NULL >= NULL should return NULL",
        "SELECT NULL >= NULL as res;",
        Ok(select_with_null!(res; Null)),
    )
    .await;

    g.named_test(
        "NULL <= NULL should return NULL",
        "SELECT NULL <= NULL as res;",
        Ok(select_with_null!(res; Null)),
    )
    .await;

    g.named_test(
        "NULL <> NULL should return NULL",
        "SELECT NULL <> NULL as res;",
        Ok(select_with_null!(res; Null)),
    )
    .await;
});
