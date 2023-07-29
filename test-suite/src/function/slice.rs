use {crate::*, gluesql_core::prelude::Value::*};

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
    // test! {
    //     name: "",
    //     sql: "SELECT SLICE(list, -1, 5) AS value FROM Test;",
    //     expected: Ok(select!(
    //         "value"
    //         List;
    //         vec![]
    //     ))
    // };
});
