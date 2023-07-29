use {
    crate::*,
    gluesql_core::{error::EvaluateError, prelude::Value::*},
};

test_case!(slice, async move {
    run!("CREATE TABLE Test (list LIST)");
    run!("INSERT INTO Test VALUES ('[1,2,3,4]')");
    test! {
        name: "slice start in index 0",
        sql: "SELECT SLICE(list, 0, 2) AS value FROM Test;",
        expected: Ok(select!(
            "value"
            List;
            vec![I64(1),I64(2)]
        ))
    };
    test! {
        name: "slice with size",
        sql: "SELECT SLICE(list, 0, 4) AS value FROM Test;",
        expected: Ok(select!(
            "value"
            List;
            vec![I64(1), I64(2),I64(3), I64(4)]
        ))
    };
    test! {
        name: "slice with size that pass over array size",
        sql: "SELECT SLICE(list, 2, 5) AS value FROM Test;",
        expected: Ok(select!(
            "value"
            List;
            vec![I64(3), I64(4)]
        ))
    };
    test! {
        name: "slice that over array size",
        sql: "SELECT SLICE(list, 100, 5) AS value FROM Test;",
        expected: Ok(select!(
            "value"
            List;
            vec![]
        ))
    };
    test! {
        name: "list value should be List Value",
        sql: "SELECT SLICE(1, 2, 2) AS value FROM Test;",
        expected : Err(EvaluateError::ListTypeRequired.into())
    }
    test! {
        name: "start value should be Integer Value",
        sql: "SELECT SLICE(list, 'b', 5) AS value FROM Test;",
        expected : Err(EvaluateError::FunctionRequiresIntegerValue("SLICE".to_owned()).into())
    };
    test! {
        name: "start value should be Positive USIZE Value",
        sql: "SELECT SLICE(list, -1, 1) AS value FROM Test;",
        expected : Ok(select!(
            "value"
            List;
            vec![I64(4)]
        ))
    };
    test! {
        name: "start value should be Positive USIZE Value",
        sql: "SELECT SLICE(list, -2, 4) AS value FROM Test;",
        expected : Ok(select!(
            "value"
            List;
            vec![I64(3), I64(4)]
        ))
    };
    test! {
        name: "start value should be Positive USIZE Value",
        sql: "SELECT SLICE(list, 9999, 4) AS value FROM Test;",
        expected : Ok(select!(
            "value"
            List;
            vec![]
        ))
    };
    test! {
        name: "start value should be Positive USIZE Value",
        sql: "SELECT SLICE(list, 0, 1234) AS value FROM Test;",
        expected : Ok(select!(
            "value"
            List;
            vec![I64(1), I64(2),I64(3), I64(4)]
        ))
    };
    test! {
        name: "if absoulte value of negative index over length of list, covert to index 0",
        sql: "SELECT SLICE(list, -234, 4) AS value FROM Test;",
        expected : Ok(select!(
            "value"
            List;
            vec![I64(1), I64(2),I64(3), I64(4)]
        ))
    };
    test! {
        name: "length value should be Integer Value",
        sql: "SELECT SLICE(list, 2, 'a') AS value FROM Test;",
    expected : Err(EvaluateError::FunctionRequiresIntegerValue("SLICE".to_owned()).into())
    };
});
