use {crate::*, gluesql_core::prelude::Value::*};

test_case!(between, {
    let g = get_tester!();

    g.named_test(
        "'1 BETWEEN 2 AND 3' should return false",
        "SELECT 1 BETWEEN 2 AND 3 as res;",
        Ok(select!(
           res
           Bool;
           false
        )),
    )
    .await;

    g.named_test(
        "'NULL BETWEEN ...' should return NULL",
        format!(
            "SELECT (NULL BETWEEN {} AND {}) IS NULL as res;",
            i128::MIN,
            i128::MAX
        )
        .as_str(),
        Ok(select!(
           res
           Bool;
           true
        )),
    )
    .await;
});
