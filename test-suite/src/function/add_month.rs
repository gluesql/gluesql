use {crate::*, gluesql_core::prelude::Value};

test_case!(add_month, async move {
    macro_rules! date {
        ($date: expr) => {
            $date.parse().unwrap()
        };
    }
    test! {
        name: "plus test on DATE TYPE",
        sql: "SELECT ADD_MONTH('2017-06-15',1) AS test;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-07-15")
        ))
    };
    test! {
        name: "minus test on general case",
        sql: "SELECT ADD_MONTH('2017-06-15',-1) AS test;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-05-15")
        ))
    };

    test! {
        name: "the last day of February test",
        sql: "SELECT ADD_MONTH('2017-01-31',1) AS test;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-02-28")
        ))
    };

    test! {
        name: "year change test",
        sql: "SELECT ADD_MONTH('2017-01-31',13) AS test;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2018-02-28")
        ))
    };

    test! {
        name: "leap year test",
        sql: "SELECT ADD_MONTH('2017-01-31',13) AS test;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2018-02-28")
        ))
    };
    test! {
        name: "zero test",
        sql: "SELECT ADD_MONTH('2017-01-31',0) AS test;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-01-31")
        ))
    };
});
