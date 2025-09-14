use gluesql::{
    FromGlueRow,
    core::{data::Value, executor::Payload, row_conversion::SelectExt},
};

#[derive(Debug, PartialEq, FromGlueRow)]
struct SString {
    v: String,
}

#[test]
fn date_to_string() {
    use chrono::NaiveDate;

    let date = NaiveDate::from_ymd_opt(2023, 4, 5).unwrap();
    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::Date(date)]],
    };

    let rows: Vec<SString> = payload.rows_as::<SString>().unwrap();
    assert_eq!(rows[0].v, "2023-04-05");
}

#[test]
fn time_to_string() {
    use chrono::NaiveTime;

    let time = NaiveTime::from_hms_opt(1, 2, 3).unwrap();
    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::Time(time)]],
    };

    let rows: Vec<SString> = payload.rows_as::<SString>().unwrap();
    assert_eq!(rows[0].v, "01:02:03");
}

#[test]
fn timestamp_to_string() {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

    let date = NaiveDate::from_ymd_opt(2023, 4, 5).unwrap();
    let time = NaiveTime::from_hms_opt(1, 2, 3).unwrap();
    let ts = NaiveDateTime::new(date, time);

    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::Timestamp(ts)]],
    };

    let rows: Vec<SString> = payload.rows_as::<SString>().unwrap();
    assert_eq!(rows[0].v, "2023-04-05T01:02:03Z");
}
