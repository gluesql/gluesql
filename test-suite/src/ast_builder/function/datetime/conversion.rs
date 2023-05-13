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
        .project("name")
        .project(col("visit_date").to_date("'%Y-%m-%d'"))
        .project(to_date("visit_date", "'%Y-%m-%d'"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name                | "TO_DATE(\"visit_date\", '%Y-%m-%d')"          | "TO_DATE(\"visit_date\", '%Y-%m-%d')"
        I64 | Str                 | Date                                           | Date;
        1    "Bryanna".to_owned()   NaiveDate::from_ymd_opt(2022, 12, 23).unwrap()   NaiveDate::from_ymd_opt(2022, 12, 23).unwrap();
        2    "Ash".to_owned()       NaiveDate::from_ymd_opt(2023, 4, 1).unwrap()     NaiveDate::from_ymd_opt(2023, 4, 1).unwrap()
    ));
    test(actual, expected);

    // Time
    let actual = table("Visitor")
        .select()
        .project("id")
        .project("name")
        .project(col("visit_time").to_time("'%H:%M:%S'"))
        .project(to_time("visit_time", "'%H:%M:%S'"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name                | "TO_TIME(\"visit_time\", '%H:%M:%S')"       | "TO_TIME(\"visit_time\", '%H:%M:%S')"
        I64 | Str                 | Time                                        | Time;
        1    "Bryanna".to_owned()   NaiveTime::from_hms_opt(13, 5, 26).unwrap()   NaiveTime::from_hms_opt(13, 5, 26).unwrap();
        2    "Ash".to_owned()       NaiveTime::from_hms_opt(23, 24, 11).unwrap()  NaiveTime::from_hms_opt(23, 24, 11).unwrap()
    ));
    test(actual, expected);

    // Timestamp
    let actual = table("Visitor")
        .select()
        .project("id")
        .project("name")
        .project(col("visit_time_stamp").to_timestamp("'%Y-%m-%d %H:%M:%S'"))
        .project(to_timestamp("visit_time_stamp", "'%Y-%m-%d %H:%M:%S'"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name                 | "TO_TIMESTAMP(\"visit_time_stamp\", '%Y-%m-%d %H:%M:%S')"                      | "TO_TIMESTAMP(\"visit_time_stamp\", '%Y-%m-%d %H:%M:%S')"
        I64 | Str                  | Timestamp                                                                      | Timestamp;
        1    "Bryanna".to_owned()    NaiveDate::from_ymd_opt(2022, 12, 23).unwrap().and_hms_opt(13, 5, 26).unwrap()   NaiveDate::from_ymd_opt(2022, 12, 23).unwrap().and_hms_opt(13, 5, 26).unwrap();
        2    "Ash".to_owned()        NaiveDate::from_ymd_opt(2023, 4, 1).unwrap().and_hms_opt(23, 24, 11).unwrap()    NaiveDate::from_ymd_opt(2023, 4, 1).unwrap().and_hms_opt(23, 24, 11).unwrap()
    ));
    test(actual, expected);
});
