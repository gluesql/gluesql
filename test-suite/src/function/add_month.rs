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
        name: "[DATE TYPE]check add_month function works as plus",
        sql: "SELECT ADD_MONTH(DATE '2017-06-15',1) AS test;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-07-15")
        ))
    };
    test! {
        name: "[TO_DATE FUNCTION]check add_month function works as plus",
        sql: "ADD_MONTH(TO_DATE('2017-06-15','%Y-%m-%d'),1) ;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-07-15")
        ))
    };
    test! {
        name: "[DATE TYPE]check add_month function works as minus",
        sql: "SELECT ADD_MONTH(DATE '2017-06-15',-1) AS test;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-05-15")
        ))
    };
    test! {
        name: "[TO_DATE FUNCTION]check add_month function works as minus",
        sql: "ADD_MONTH(TO_DATE('2017-06-15','%Y-%m-%d'),-1) ;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-05-15")
        ))
    };
});
