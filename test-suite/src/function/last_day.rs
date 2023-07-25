use {crate::*, chrono::NaiveDate, gluesql_core::prelude::Value::*};

test_case!(last_day, async move {
    test! {
        name: "Should return the last day of the month that a given date belongs to",
        sql: "VALUES(LAST_DAY(date '2017-12-15'));",
        expected: Ok(select!(
            column1
            Date;
            NaiveDate::from_ymd_opt(2017, 12, 31).unwrap()
        ))
    };

    test! {
        name: "Should return the last day of the month that a given timestamp belongs to",
        sql: "VALUES(LAST_DAY(timestamp '2017-12-15 12:00:00'));",
        expected: Ok(select!(
            column1
            Date;
            NaiveDate::from_ymd_opt(2017, 12, 31).unwrap()
        ))
    };
});
