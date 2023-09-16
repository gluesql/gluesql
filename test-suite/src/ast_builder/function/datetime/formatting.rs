use {
    crate::*,
    chrono::{NaiveDate, NaiveTime},
    gluesql_core::{
        ast_builder::{function as f, *},
        executor::Payload,
        prelude::Value::*,
    },
};
test_case!(formatting, {
    let glue = get_glue!();

    // create table -"Visitor"
    let actual = table("Visitor")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("name TEXT")
        .add_column("visit_date DATE")
        .add_column("visit_time TIME")
        .add_column("visit_timestamp TIMESTAMP")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Visitor");

    // insert
    let actual = table("Visitor")
        .insert()
        .values(vec![
            vec![
                num(1),
                text("Bryanna"),
                date("2017-06-15"),
                time("13:05:26"),
                timestamp("2015-09-05 23:56:04"),
            ],
            vec![
                num(2),
                text("Ash"),
                date("2023-04-01"),
                time("23:24:11"),
                timestamp("2023-04-01 23:24:11"),
            ],
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(2));
    assert_eq!(actual, expected, "insert - Visitor");

    // format date
    let actual = table("Visitor")
        .select()
        .project("name")
        .project("visit_date")
        .project(col("visit_date").format(text("%Y-%m")))
        .project(f::format(col("visit_date"), text("%m")))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        name                    | visit_date                                       | r#"FORMAT("visit_date", '%Y-%m')"#          | r#"FORMAT("visit_date", '%m')"#
        Str                     | Date                                             | Str                                        | Str;
        "Bryanna".to_owned()    NaiveDate::from_ymd_opt(2017, 6, 15).unwrap()     "2017-06".to_owned()                        "06".to_owned();
        "Ash".to_owned()        NaiveDate::from_ymd_opt(2023, 4, 1).unwrap()     "2023-04".to_owned()                        "04".to_owned()
    ));
    assert_eq!(actual, expected, "format date - Visitor");

    // format time
    let actual = table("Visitor")
        .select()
        .project("name")
        .project("visit_time")
        .project(col("visit_time").format(text("%H:%M:%S")))
        .project(f::format(col("visit_time"), text("%M:%S")))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        name                    | visit_time                                       | r#"FORMAT("visit_time", '%H:%M:%S')"#          | r#"FORMAT("visit_time", '%M:%S')"#
        Str                     | Time                                             | Str                                        | Str;
        "Bryanna".to_owned()    NaiveTime::from_hms_opt(13, 5, 26).unwrap()     "13:05:26".to_owned()                        "05:26".to_owned();
        "Ash".to_owned()        NaiveTime::from_hms_opt(23, 24, 11).unwrap()     "23:24:11".to_owned()                        "24:11".to_owned()
    ));
    assert_eq!(actual, expected, "format time - Visitor");

    // format timestamp
    let actual = table("Visitor")
        .select()
        .project("name")
        .project("visit_timestamp")
        .project(col("visit_timestamp").format(text("%Y-%m-%d %H:%M:%S")))
        .project(f::format(col("visit_timestamp"), text("%Y-%m-%d %H:%M:%S")))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        name                    | visit_timestamp                                                                   | r#"FORMAT("visit_timestamp", '%Y-%m-%d %H:%M:%S')"#           | r#"FORMAT("visit_timestamp", '%Y-%m-%d %H:%M:%S')"#
        Str                     | Timestamp                                                                         | Str                                                           | Str;
        "Bryanna".to_owned()    NaiveDate::from_ymd_opt(2015, 9, 5).unwrap().and_hms_opt(23, 56, 4).unwrap()     "2015-09-05 23:56:04".to_owned()                                 "2015-09-05 23:56:04".to_owned();
        "Ash".to_owned()        NaiveDate::from_ymd_opt(2023, 4, 1).unwrap().and_hms_opt(23, 24, 11).unwrap()     "2023-04-01 23:24:11".to_owned()                                 "2023-04-01 23:24:11".to_owned()
    ));
    assert_eq!(actual, expected, "format timestamp - Visitor");
});
