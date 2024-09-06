use {
    crate::*,
    chrono::NaiveDate,
    gluesql_core::{executor::EvaluateError, prelude::Value::*},
};

test_case!(last_day, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE LastDay (
            id INTEGER,
            date DATE,
            timestamp TIMESTAMP
        );",
    )
    .await;

    g.run("INSERT INTO LastDay (id, date) VALUES (1, LAST_DAY(DATE '2017-12-15'));")
        .await;
    g.named_test(
        "Should insert the last day of the month that a given date belongs to",
        "SELECT date FROM LastDay WHERE id = 1;",
        Ok(select!(
            date;
            Date;
            NaiveDate::from_ymd_opt(2017, 12, 31).unwrap()
        )),
    )
    .await;

    g.run("INSERT INTO LastDay (id, date) VALUES (2, DATE '2017-01-01');")
        .await;
    g.named_test(
        "Should return the last day of the month that a retrieved date belongs to",
        "SELECT LAST_DAY(date) as date FROM LastDay WHERE id = 2;",
        Ok(select!(
            date;
            Date;
            NaiveDate::from_ymd_opt(2017, 1, 31).unwrap()
        )),
    )
    .await;

    g.run("INSERT INTO LastDay (id, date) VALUES (3, LAST_DAY(TIMESTAMP '2017-12-15 12:12:20'));")
        .await;
    g.named_test(
        "Should insert the last day of the month that a given timestamp belongs to",
        "SELECT date FROM LastDay WHERE id = 3;",
        Ok(select!(
            date;
            Date;
            NaiveDate::from_ymd_opt(2017, 12, 31).unwrap()
        )),
    )
    .await;

    g.run("INSERT INTO LastDay (id, timestamp) VALUES (4, TIMESTAMP '2017-01-01 12:12:20');")
        .await;
    g.named_test(
        "Should return the last day of the month that a retrieved timestamp belongs to",
        "SELECT LAST_DAY(timestamp) as date FROM LastDay WHERE id = 4;",
        Ok(select!(
            date;
            Date;
            NaiveDate::from_ymd_opt(2017, 1, 31).unwrap()
        )),
    )
    .await;

    g.named_test(
        "Should only give date or timestamp value to LAST_DAY function",
        "VALUES (LAST_DAY('dfafsdf3243252454325342'));",
        Err(EvaluateError::FunctionRequiresDateOrDateTimeValue("LAST_DAY".to_owned()).into()),
    )
    .await;
});
