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
            r#"SELECT ifnull(id, 1) as ID, ifnull(int8, 2) as int8, ifnull(dec, 3) 
            FROM SingleItem where id is not null"#,
            select!("ID" | "int8" | "ifnull(dec, 3)"; I64 | I8 | Decimal; 0 1 Decimal::from(2i8)),
        ),
        (
            // notice that this example returns I64 I64 I64, where the previous example returned I64 I8 Decimal.
            // is this behavior desirable?  see https://dev.mysql.com/doc/refman/5.7/en/flow-control-functions.html#function_ifnull
            r#"SELECT ifnull(id, 1) as ID, ifnull(int8, 2) as int8, ifnull(dec, 3) 
            FROM SingleItem where id is null"#,
            select!("ID" | "int8" | "ifnull(dec, 3)"; I64 | I64 | I64; 1  2 3),
        ),
        (
            r#"SELECT ifnull(dt, "2000-01-01") as mydate, ifnull(mystring, "blah") as name 
            FROM SingleItem where id is not null"#,
            select!("mydate" | "name"; Date | Str; NaiveDate::from_ymd(2022,5,23) "this is a string".to_string()),
        ),
        (
            // notice that the returned data types are STR, and STR (and not DATE, STR, like the previous example
            // is this behavior desirable?  see https://dev.mysql.com/doc/refman/5.7/en/flow-control-functions.html#function_ifnull
            r#"SELECT ifnull(dt, "2000-01-01") as mydate, ifnull(mystring, "blah") as name 
            FROM SingleItem where id is null"#,
            select!("mydate" | "name"; Str | Str; "2000-01-01".to_string() "blah".to_string()),
        ),
        (
            r#"SELECT ifnull(mybool, "YES") as mybool, ifnull(myfloat, "NO") as myfloat 
            FROM SingleItem where id is not null"#,
            select!("mybool" | "myfloat"; Bool | F64; true 3.15),
        ),
        (
            r#"SELECT ifnull(mybool, "YES") as mybool, ifnull(myfloat, "NO") as myfloat 
            FROM SingleItem where id is null"#,
            select!("mybool" | "myfloat"; Str | Str; "YES".to_string() "NO".to_string()),
        ),
        (
            r#"SELECT ifnull(mytime, "YES") as mybool, ifnull(mytimestamp, "NO") as myfloat 
            FROM SingleItem where id is not null"#,
            select!("mybool" | "myfloat"; Time | Timestamp; 
                    NaiveTime::from_hms(1, 2, 3) NaiveDateTime::from_timestamp(0, 0)),
        ),
        (
            r#"SELECT ifnull(mytime, "YES") as mybool, ifnull(mytimestamp, "NO") as myfloat 
            FROM SingleItem where id is null"#,
            select!("mybool" | "myfloat"; Str | Str; "YES".to_string() "NO".to_string()),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(Ok(expected), sql);
    }
});
