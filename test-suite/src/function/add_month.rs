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
        name: "[DATE TYPE]plus test on general case",
        sql: "SELECT ADD_MONTH(DATE '2017-06-15',1) AS test;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-07-15")
        ))
    };
    test! {
        name: "[TO_DATE FUNCTION]plus test on general case",
        sql: "ADD_MONTH(TO_DATE('2017-06-15','%Y-%m-%d'),1) ;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-07-15")
        ))
    };
    test! {
        name: "[DATE TYPE]minus test on general case",
        sql: "SELECT ADD_MONTH(DATE '2017-06-15',-1) AS test;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-05-15")
        ))
    };
    test! {
        name: "[TO_DATE FUNCTION]minus test on general case",
        sql: "ADD_MONTH(TO_DATE('2017-06-15','%Y-%m-%d'),-1) ;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-05-15")
        ))
    };

    test! {
        name: "[DATE TYPE]plus test on the last day of February",
        sql: "SELECT ADD_MONTH(DATE '2017-01-31',1) AS test;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-02-28")
        ))
    };
    test! {
        name: "[TO_DATE FUNCTION]plus test on the last day of February",
        sql: "ADD_MONTH(TO_DATE('2017-01-31','%Y-%m-%d'),1) ;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-07-28")
        ))
    };
    test! {
        name: "[DATE TYPE]minus test on the last day of February",
        sql: "SELECT ADD_MONTH(DATE '2017-06-30',-4) AS test;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-02-28")
        ))
    };
    test! {
        name: "[TO_DATE FUNCTION]minus test on the last day of February",
        sql: "ADD_MONTH(TO_DATE('2017-06-30','%Y-%m-%d'),-4) ;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-07-28")
        ))
    };

    test! {
        name: "[DATE TYPE]plus test in the case of year change",
        sql: "SELECT ADD_MONTH(DATE '2017-01-31',13) AS test;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-02-28")
        ))
    };
    test! {
        name: "[TO_DATE FUNCTION]plus test on the last day of February",
        sql: "ADD_MONTH(TO_DATE('2017-01-31','%Y-%m-%d'),13) ;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-07-28")
        ))
    };
    test! {
        name: "[DATE TYPE]minus test on the last day of February",
        sql: "SELECT ADD_MONTH(DATE '2017-06-30',-12) AS test;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-06-30")
        ))
    };
    test! {
        name: "[TO_DATE FUNCTION]minus test on the last day of February",
        sql: "ADD_MONTH(TO_DATE('2017-06-30','%Y-%m-%d'),-12) ;",
        expected: Ok(select!(
            "test"
            Value::Date;
            date!("2017-06-30")
        ))
    };
});
