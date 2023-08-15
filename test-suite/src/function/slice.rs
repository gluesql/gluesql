use {
    crate::*,
    gluesql_core::{error::EvaluateError, prelude::Value::*},
};

test_case!(slice, {
    let g = get_tester!();

    g.run("CREATE TABLE Test (list LIST)").await;

    g.run("INSERT INTO Test VALUES ('[1,2,3,4]')").await;

    g.named_test(
        "slice start in index 0",
        "SELECT SLICE(list, 0, 2) AS value FROM Test;",
        Ok(select!(
            "value"
            List;
            vec![I64(1),I64(2)]
        )),
    )
    .await;

    g.named_test(
        "slice with size",
        "SELECT SLICE(list, 0, 4) AS value FROM Test;",
        Ok(select!(
            "value"
            List;
            vec![I64(1), I64(2),I64(3), I64(4)]
        )),
    )
    .await;

    g.named_test(
        "slice with size that pass over array size",
        "SELECT SLICE(list, 2, 5) AS value FROM Test;",
        Ok(select!(
            "value"
            List;
            vec![I64(3), I64(4)]
        )),
    )
    .await;
    g.named_test(
        "slice that over array size",
        "SELECT SLICE(list, 100, 5) AS value FROM Test;",
        Ok(select!(
            "value"
            List;
            vec![]
        )),
    )
    .await;

    g.named_test(
        "list value should be List Value",
        "SELECT SLICE(1, 2, 2) AS value FROM Test;",
        Err(EvaluateError::ListTypeRequired.into()),
    )
    .await;
    g.named_test(
        "start value should be Integer Value",
        "SELECT SLICE(list, 'b', 5) AS value FROM Test;",
        Err(EvaluateError::FunctionRequiresIntegerValue("SLICE".to_owned()).into()),
    )
    .await;
    g.named_test(
        "start value should be Positive USIZE Value",
        "SELECT SLICE(list, -1, 1) AS value FROM Test;",
        Ok(select!(
            "value"
            List;
            vec![I64(4)]
        )),
    )
    .await;
    g.named_test(
        "start value should be Positive USIZE Value",
        "SELECT SLICE(list, -2, 4) AS value FROM Test;",
        Ok(select!(
            "value"
            List;
            vec![I64(3), I64(4)]
        )),
    )
    .await;
    g.named_test(
        "start value should be Positive USIZE Value",
        "SELECT SLICE(list, 9999, 4) AS value FROM Test;",
        Ok(select!(
            "value"
            List;
            vec![]
        )),
    )
    .await;
    g.named_test(
        "start value should be Positive USIZE Value",
        "SELECT SLICE(list, 0, 1234) AS value FROM Test;",
        Ok(select!(
            "value"
            List;
            vec![I64(1), I64(2),I64(3), I64(4)]
        )),
    )
    .await;
    g.named_test(
        "if absoulte value of negative index over length of list, covert to index 0",
        "SELECT SLICE(list, -234, 4) AS value FROM Test;",
        Ok(select!(
            "value"
            List;
            vec![I64(1), I64(2),I64(3), I64(4)]
        )),
    )
    .await;
    g.named_test(
        "length value should be Integer Value",
        "SELECT SLICE(list, 2, 'a') AS value FROM Test;",
        Err(EvaluateError::FunctionRequiresIntegerValue("SLICE".to_owned()).into()),
    )
    .await;
});
