use {crate::*, gluesql_core::prelude::Value::*};

test_case!(date, async move {
    run!(
        r#"
CREATE TABLE DateLog (
    id INTEGER,
    date1 DATE,
    date2 DATE,
)"#
    );

    run!(
        r#"
INSERT INTO DateLog VALUES
    (1, "2020-06-11", "2021-03-01"),
    (2, "2020-09-30", "1989-01-01"),
    (3, "2021-05-01", "2021-05-01");
"#
    );

    macro_rules! date {
        ($date: expr) => {
            $date.parse().unwrap()
        };
    }

    test!(
        "SELECT id, date1, date2 FROM DateLog",
        Ok(select!(
            id  | date1               | date2
            I64 | Date                | Date;
            1     date!("2020-06-11")   date!("2021-03-01");
            2     date!("2020-09-30")   date!("1989-01-01");
            3     date!("2021-05-01")   date!("2021-05-01")
        ))
    );

    test!(
        "SELECT * FROM DateLog WHERE date1 > date2",
        Ok(select!(
            id  | date1               | date2
            I64 | Date                | Date;
            2     date!("2020-09-30")   date!("1989-01-01")
        ))
    );

    test!(
        "SELECT * FROM DateLog WHERE date1 <= date2",
        Ok(select!(
            id  | date1               | date2
            I64 | Date                | Date;
            1     date!("2020-06-11")   date!("2021-03-01");
            3     date!("2021-05-01")   date!("2021-05-01")
        ))
    );

    test!(
        r#"SELECT * FROM DateLog WHERE date1 = DATE "2020-06-11";"#,
        Ok(select!(
            id  | date1               | date2
            I64 | Date                | Date;
            1     date!("2020-06-11")   date!("2021-03-01")
        ))
    );

    test!(
        r#"SELECT * FROM DateLog WHERE date2 < "2000-01-01";"#,
        Ok(select!(
            id  | date1               | date2
            I64 | Date                | Date;
            2     date!("2020-09-30")   date!("1989-01-01")
        ))
    );

    test!(
        r#"SELECT * FROM DateLog WHERE "1999-01-03" < DATE "2000-01-01";"#,
        Ok(select!(
            id  | date1               | date2
            I64 | Date                | Date;
            1     date!("2020-06-11")   date!("2021-03-01");
            2     date!("2020-09-30")   date!("1989-01-01");
            3     date!("2021-05-01")   date!("2021-05-01")
        ))
    );

    let days = gluesql_core::data::Interval::days;
    let timestamp = |y, m, d| chrono::NaiveDate::from_ymd(y, m, d).and_hms(0, 0, 0);

    test!(
        r#"SELECT
            id,
            date1 - date2 AS date_sub,
            date1 - INTERVAL "1" DAY AS sub,
            date2 + INTERVAL "1" MONTH AS add
        FROM DateLog;"#,
        Ok(select!(
            id  | date_sub     | sub                    | add
            I64 | Interval     | Timestamp              | Timestamp;
            1     days(-263)     timestamp(2020, 6, 10)   timestamp(2021, 4, 1);
            2     days(11_595)   timestamp(2020, 9, 29)   timestamp(1989, 2, 1);
            3     days(0)        timestamp(2021, 4, 30)   timestamp(2021, 6, 1)
        ))
    );

    test!(
        r#"INSERT INTO DateLog VALUES (1, "12345-678", "2021-05-01")"#,
        Err(gluesql_core::data::ValueError::FailedToParseDate("12345-678".to_owned()).into())
    );
});
