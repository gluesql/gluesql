use {
    crate::*, chrono::NaiveDate, gluesql_core::executor::EvaluateError,
    gluesql_core::prelude::Value::*,
};

test_case!(last_day, async move {
    run!(
        "CREATE TABLE LastDay (
            id INTEGER,
            date DATE,
            timestamp TIMESTAMP,
        );"
    );

    run!("INSERT INTO LastDay (id, date) VALUES (1, LAST_DAY(date '2017-12-15'));");
    test! {
        name: "Should insert the last day of the month that a given date belongs to",
        sql: "SELECT date FROM LastDay WHERE id = 1;",
        expected: Ok(select!(
            date;
            Date;
            NaiveDate::from_ymd_opt(2017, 12, 31).unwrap()
        ))
    };

    run!("INSERT INTO LastDay (id, date) VALUES (2, date '2017-01-01');");
    test! {
        name: "Should return the last day of the month that a retrieved date belongs to",
        sql: "SELECT LAST_DAY(date) as date FROM LastDay WHERE id = 2;",
        expected: Ok(select!(
            date;
            Date;
            NaiveDate::from_ymd_opt(2017, 1, 31).unwrap()
        ))
    };

    run!("INSERT INTO LastDay (id, date) VALUES (3, LAST_DAY(timestamp '2017-12-15 12:12:20'));");
    test! {
        name: "Should insert the last day of the month that a given timestamp belongs to",
        sql: "SELECT date FROM LastDay WHERE id = 3;",
        expected: Ok(select!(
            date;
            Date;
            NaiveDate::from_ymd_opt(2017, 12, 31).unwrap()
        ))
    };

    run!("INSERT INTO LastDay (id, timestamp) VALUES (4, timestamp '2017-01-01 12:12:20');");
    test! {
        name: "Should return the last day of the month that a retrieved timestamp belongs to",
        sql: "SELECT LAST_DAY(timestamp) as date FROM LastDay WHERE id = 4;",
        expected: Ok(select!(
            date;
            Date;
            NaiveDate::from_ymd_opt(2017, 1, 31).unwrap()
        ))
    };

    test! {
        name: "Should only give date or timestamp value to LAST_DAY function",
        sql: "VALUES (LAST_DAY('dfafsdf3243252454325342'));",
        expected: Err(EvaluateError::FunctionRequiresDateOrDateTimeValue("LAST_DAY".to_owned()).into())
    };
});
