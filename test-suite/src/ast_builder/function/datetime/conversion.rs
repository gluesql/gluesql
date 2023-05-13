use chrono::{NaiveDate, NaiveTime};

use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(conversion, async move {
    let glue = get_glue!();

    let actual = table("Visitor")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("name TEXT")
        .add_column("visit_date TEXT")
        .add_column("visit_time TEXT")
        .add_column("visit_time_stamp TEXT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    test(actual, expected);

    let actual = table("Visitor")
        .insert()
        .values(vec![
            "1, 'Bryanna', '2022-12-23', '13:05:26', '2022-12-23 13:05:26'",
            "2, 'Ash', '2023-04-01', '23:24:11', '2023-04-01 23:24:11'",
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(2));
    test(actual, expected);

    // Date
    let actual = table("Visitor")
        .select()
        .project("id")
        .project(col("visit_date"))
        .project(date("2022-03-03"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name                | visit_date
        I64 | Str                 | Date;
        1    "Bryanna".to_owned()   NaiveDate::from_ymd_opt(2022, 12, 23).unwrap();
        2    "Ash".to_owned()     NaiveDate::from_ymd_opt(2023, 4, 1).unwrap()
    ));
    test(actual, expected);

    // Time
    let actual = table("Visitor")
        .select()
        .project("id")
        .project(col("visit_time"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name                | visit_time
        I64 | Str                 | Time;
        1    "Bryanna".to_owned()   NaiveTime::from_hms_opt(13, 5, 26).unwrap();
        2    "Ash".to_owned()     NaiveTime::from_hms_opt(23, 24, 11).unwrap()
    ));
    test(actual, expected);

    // Timestamp
    let actual = table("Visitor")
        .select()
        .project("id")
        .project(col("visit_time_stamp"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name                 | visit_time_stamp
        I64 | Str                  | Timestamp;
        1    "Bryanna".to_owned()    NaiveDate::from_ymd_opt(2022, 12, 23).unwrap().and_hms_opt(13, 5, 26).unwrap();
        2    "Ash".to_owned()      NaiveDate::from_ymd_opt(2023, 4, 1).unwrap().and_hms_opt(23, 24, 11).unwrap()
    ));
    test(actual, expected);
});
