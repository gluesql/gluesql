use crate::*;

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

    use Value::*;

    macro_rules! date {
        ($date: expr) => {
            $date.parse().unwrap()
        };
    }

    test!(
        Ok(select!(
            id  | date1               | date2
            I64 | Date                | Date;
            1     date!("2020-06-11")   date!("2021-03-01");
            2     date!("2020-09-30")   date!("1989-01-01");
            3     date!("2021-05-01")   date!("2021-05-01")
        )),
        "SELECT id, date1, date2 FROM DateLog"
    );

    test!(
        Ok(select!(
            id  | date1               | date2
            I64 | Date                | Date;
            2     date!("2020-09-30")   date!("1989-01-01")
        )),
        "SELECT * FROM DateLog WHERE date1 > date2"
    );

    test!(
        Ok(select!(
            id  | date1               | date2
            I64 | Date                | Date;
            1     date!("2020-06-11")   date!("2021-03-01");
            3     date!("2021-05-01")   date!("2021-05-01")
        )),
        "SELECT * FROM DateLog WHERE date1 <= date2"
    );

    test!(
        Ok(select!(
            id  | date1               | date2
            I64 | Date                | Date;
            1     date!("2020-06-11")   date!("2021-03-01")
        )),
        r#"SELECT * FROM DateLog WHERE date1 = DATE "2020-06-11";"#
    );

    test!(
        Ok(select!(
            id  | date1               | date2
            I64 | Date                | Date;
            2     date!("2020-09-30")   date!("1989-01-01")
        )),
        r#"SELECT * FROM DateLog WHERE date2 < "2000-01-01";"#
    );

    test!(
        Ok(select!(
            id  | date1               | date2
            I64 | Date                | Date;
            1     date!("2020-06-11")   date!("2021-03-01");
            2     date!("2020-09-30")   date!("1989-01-01");
            3     date!("2021-05-01")   date!("2021-05-01")
        )),
        r#"SELECT * FROM DateLog WHERE "1999-01-03" < DATE "2000-01-01";"#
    );

    test!(
        Err(ValueError::FailedToParseDate("12345-678".to_owned()).into()),
        r#"INSERT INTO DateLog VALUES (1, "12345-678", "2021-05-01")"#
    );
});
