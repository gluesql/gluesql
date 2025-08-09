use {crate::*, gluesql_core::prelude::Value::*};

test_case!(in_list, {
    let g = get_tester!();

    g.named_test(
        "'NULL IN (...)' should return 'NULL'",
        "SELECT NULL IN (1, 2, 3) as res",
        Ok(select_with_null!(
            res;
            Null
        )),
    )
    .await;

    g.named_test(
        "'NULL IN (...)' should return 'NULL' even if the list includes 'NULL'",
        "SELECT NULL IN (1, 2, 3, NULL) as res",
        Ok(select_with_null!(
            res;
            Null
        )),
    )
    .await;
});
