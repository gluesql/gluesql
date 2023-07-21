use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::{Payload, Value},
    },
};

test_case!(add_month, async move {
    macro_rules! date {
        ($date: expr) => {
            $date.parse().unwrap()
        };
    }
    test! {
        name: "check add_month function works with date type",
        sql: "SELECT ADD_MONTH(DATE '2017-06-15',1) AS test;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-06-15")
        ))
    };
    test! {
        name: "check add_month function works with to_date function",
        sql: "ADD_MONTH(TO_DATE('2017-06-15','%Y-%m-%d'),1) ;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-06-15")
        ))
    };
});
