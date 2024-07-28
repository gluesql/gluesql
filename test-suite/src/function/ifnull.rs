use {
    crate::*,
    chrono::{DateTime, NaiveDate, NaiveTime},
    gluesql_core::prelude::{Payload, Value::*},
    rust_decimal::Decimal,
};

test_case!(ifnull, {
    let g = get_tester!();

    let test_cases = [
        (
            "CREATE TABLE SingleItem (id integer null, int8 int8 null, dec decimal null,
                                        dt date null, mystring Text null,
                                        mybool Boolean null, myfloat float null,
                                        mytime time null, mytimestamp timestamp null)",
            Payload::Create,
        ),
        (
            "INSERT INTO SingleItem VALUES (0, 1, 2, '2022-05-23', 'this is a string', true, 3.15,
                          '01:02:03', '1970-01-01 00:00:00 -00:00')",
            Payload::Insert(1),
        ),
        (
            "INSERT INTO SingleItem VALUES (null, null, null, null, null, null, null, null, null)",
            Payload::Insert(1),
        ),
        (
            "SELECT IFNULL(id, 1) AS myid, IFNULL(int8, 2) AS int8, IFNULL(dec, 3)
            FROM SingleItem WHERE id IS NOT NULL",
            select!("myid" | "int8" | "IFNULL(dec, 3)"; I64 | I8 | Decimal; 0 1 Decimal::from(2i8)),
        ),
        (
            "SELECT ifnull(id, 1) AS ID, IFNULL(int8, 2) AS INT8, IFNULL(dec, 3)
            FROM SingleItem WHERE id IS NULL",
            select!("ID" | "INT8" | "IFNULL(dec, 3)"; I64 | I64 | I64; 1  2 3),
        ),
        (
            "SELECT ifnull(dt, '2000-01-01') AS mydate, ifnull(mystring, 'blah') AS name
            FROM SingleItem WHERE id IS NOT NULL",
            select!("mydate" | "name"; Date | Str; NaiveDate::from_ymd_opt(2022,5,23).unwrap() "this is a string".to_owned()),
        ),
        (
            "SELECT IFNULL(dt, '2000-01-01') AS mydate, IFNULL(mystring, 'blah') AS name
            FROM SingleItem where id is null",
            select!("mydate" | "name"; Str | Str; "2000-01-01".to_owned() "blah".to_owned()),
        ),
        (
            "SELECT IFNULL(mybool, 'YES') AS mybool, IFNULL(myfloat, 'NO') AS myfloat
            FROM SingleItem WHERE id IS NOT NULL",
            select!("mybool" | "myfloat"; Bool | F64; true 3.15),
        ),
        (
            "SELECT IFNULL(mybool, 'YES') AS mybool, IFNULL(myfloat, 'NO') AS myfloat
            FROM SingleItem WHERE id IS NULL",
            select!("mybool" | "myfloat"; Str | Str; "YES".to_owned() "NO".to_owned()),
        ),
        (
            "SELECT IFNULL(mytime, 'YES') AS mytime, IFNULL(mytimestamp, 'NO') AS mytimestamp
            FROM SingleItem WHERE id IS NOT NULL",
            select!("mytime" | "mytimestamp"; Time | Timestamp; 
                    NaiveTime::from_hms_opt(1, 2, 3).unwrap() DateTime::from_timestamp(0, 0).unwrap().naive_utc()),
        ),
        (
            "SELECT IFNULL(mytime, 'YES') AS mytime, IFNULL(mytimestamp, 'NO') AS mytimestamp
            FROM SingleItem WHERE id IS NULL",
            select!("mytime" | "mytimestamp"; Str | Str; "YES".to_owned() "NO".to_owned()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, Ok(expected)).await;
    }
});
