use {
    gluesql_core::{data::Value, executor::Payload, row_conversion::SelectExt},
    gluesql_macros::FromGlueRow,
};

#[derive(Debug, PartialEq, FromGlueRow)]
struct SString {
    v: String,
}

#[derive(Debug, PartialEq, FromGlueRow)]
struct SStringOpt {
    v: Option<String>,
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

#[test]
fn uuid_to_string() {
    let uuid_u128 = 0x936DA01F9ABD4D9D80C702AF85C822A8u128;
    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::Uuid(uuid_u128)]],
    };

    let rows: Vec<SString> = payload.rows_as::<SString>().unwrap();
    assert_eq!(rows[0].v, "936da01f-9abd-4d9d-80c7-02af85c822a8");
}

#[test]
fn uuid_to_option_string() {
    let uuid_u128 = 0x550E8400E29B41D4A716446655440000u128;
    let payload = Payload::Select {
        labels: vec!["v".into()],
        rows: vec![vec![Value::Uuid(uuid_u128)], vec![Value::Null]],
    };

    let rows: Vec<SStringOpt> = payload.rows_as::<SStringOpt>().unwrap();
    assert_eq!(
        rows[0].v.as_deref(),
        Some("550e8400-e29b-41d4-a716-446655440000")
    );
    assert_eq!(rows[1].v, None);
}
