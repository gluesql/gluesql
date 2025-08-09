use {crate::*, gluesql_core::prelude::Value::*};

test_case!(null, {
    let g = get_tester!();

    g.named_test(
        "'NULL IS NULL' should return true",
        "SELECT NULL IS NULL as res;",
        Ok(select!(res; Bool; true)),
    )
    .await;

    // TODO: Add "|", "^", "&&" to testcase when it is implemented
    for binary_op in [
        "=", ">", "<", ">=", "<=", "<>", "&", "||", "<<", ">>", "+", "-", "*", "/", "%",
    ] {
        g.named_test(
            &format!("'NULL {binary_op} NULL' should return NULL"),
            &format!("SELECT NULL {binary_op} NULL as res;"),
            Ok(select_with_null!(res; Null)),
        )
        .await;
    }

    for unary_op in ["-", "+", "NOT"] {
        g.named_test(
            &format!("'{unary_op} NULL' should return NULL"),
            &format!("SELECT {unary_op} NULL as res;"),
            Ok(select_with_null!(res; Null)),
        )
        .await;
    }
});
