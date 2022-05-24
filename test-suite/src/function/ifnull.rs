use crate::*;

test_case!(ifnull, async move {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use gluesql_core::{executor::Payload, prelude::Value::*};
    use rust_decimal::Decimal;
    let test_cases = vec![
        (
            r#"CREATE TABLE SingleItem (id integer null, int8 int(8) null, dec decimal null, 
                                        dt date null, mystring Text null,
                                        mybool Boolean null, myfloat float null,
                                        mytime time null, mytimestamp timestamp null)"#,
            Payload::Create,
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0, 1, 2, "2022-05-23", "this is a string", true, 3.15,
                          "01:02:03", "1970-01-01 00:00:00 -00:00")"#,
            Payload::Insert(1),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (null, null, null, null, null, null, null, null, null)"#,
            Payload::Insert(1),
        ),
        (
            r#"SELECT IFNULL(id, 1) AS myid, IFNULL(int8, 2) AS int8, IFNULL(dec, 3) 
            FROM SingleItem WHERE id IS NOT NULL"#,
            select!("myid" | "int8" | "IFNULL(dec, 3)"; I64 | I8 | Decimal; 0 1 Decimal::from(2i8)),
        ),
        (
            r#"SELECT ifnull(id, 1) AS ID, IFNULL(int8, 2) AS INT8, IFNULL(dec, 3) 
            FROM SingleItem WHERE id IS NULL"#,
            select!("ID" | "INT8" | "IFNULL(dec, 3)"; I64 | I64 | I64; 1  2 3),
        ),
        (
            r#"SELECT ifnull(dt, "2000-01-01") AS mydate, ifnull(mystring, "blah") AS name 
            FROM SingleItem WHERE id IS NOT NULL"#,
            select!("mydate" | "name"; Date | Str; NaiveDate::from_ymd(2022,5,23) "this is a string".to_string()),
        ),
        (
            r#"SELECT IFNULL(dt, "2000-01-01") AS mydate, IFNULL(mystring, "blah") AS name 
            FROM SingleItem where id is null"#,
            select!("mydate" | "name"; Str | Str; "2000-01-01".to_string() "blah".to_string()),
        ),
        (
            r#"SELECT IFNULL(mybool, "YES") AS mybool, IFNULL(myfloat, "NO") AS myfloat 
            FROM SingleItem WHERE id IS NOT NULL"#,
            select!("mybool" | "myfloat"; Bool | F64; true 3.15),
        ),
        (
            r#"SELECT IFNULL(mybool, "YES") AS mybool, IFNULL(myfloat, "NO") AS myfloat 
            FROM SingleItem WHERE id IS NULL"#,
            select!("mybool" | "myfloat"; Str | Str; "YES".to_string() "NO".to_string()),
        ),
        (
            r#"SELECT IFNULL(mytime, "YES") AS mybool, IFNULL(mytimestamp, "NO") AS myfloat 
            FROM SingleItem WHERE id IS NOT NULL"#,
            select!("mybool" | "myfloat"; Time | Timestamp; 
                    NaiveTime::from_hms(1, 2, 3) NaiveDateTime::from_timestamp(0, 0)),
        ),
        (
            r#"SELECT IFNULL(mytime, "YES") AS mybool, IFNULL(mytimestamp, "NO") AS myfloat 
            FROM SingleItem WHERE id IS NULL"#,
            select!("mybool" | "myfloat"; Str | Str; "YES".to_string() "NO".to_string()),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(Ok(expected), sql);
    }
});
