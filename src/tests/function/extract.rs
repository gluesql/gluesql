use crate::*;

test_case!(extract, async move {
    use Value::*;

    let test_cases = vec![
        ("CREATE TABLE Item (number TEXT)", Ok(Payload::Create)),
        (r#"INSERT INTO Item VALUES ("1")"#, Ok(Payload::Insert(1))),
        (
            r#"SELECT EXTRACT(HOUR FROM TIMESTAMP '2016-12-31 13:30:15') as extract FROM Item"#,
            Ok(select!("extract" I64; 13)),
        ),
        (
            r#"SELECT EXTRACT(YEAR FROM TIMESTAMP '2016-12-31 13:30:15') as extract FROM Item"#,
            Ok(select!("extract" I64; 2016)),
        ),
        (
            r#"SELECT EXTRACT(MONTH FROM TIMESTAMP '2016-12-31 13:30:15') as extract FROM Item"#,
            Ok(select!("extract" I64; 12)),
        ),
        (
            r#"SELECT EXTRACT(DAY FROM TIMESTAMP '2016-12-31 13:30:15') as extract FROM Item"#,
            Ok(select!("extract" I64; 31)),
        ),
        (
            r#"SELECT EXTRACT(MINUTE FROM TIMESTAMP '2016-12-31 13:30:15') as extract FROM Item"#,
            Ok(select!("extract" I64; 30)),
        ),
        (
            r#"SELECT EXTRACT(SECOND FROM TIMESTAMP '2016-12-31 13:30:15') as extract FROM Item"#,
            Ok(select!("extract" I64; 15)),
        ),
        (
            r#"SELECT EXTRACT(SECOND FROM TIME '17:12:28') as extract FROM Item"#,
            Ok(select!("extract" I64; 28)),
        ),
        (
            r#"SELECT EXTRACT(DAY FROM DATE '2021-10-06') as extract FROM Item"#,
            Ok(select!("extract" I64; 6)),
        ),
        (
            r#"SELECT EXTRACT(DAY FROM INTERVAL '3 MONTHS 2 DAYS') as extract FROM Item"#,
            Ok(select!("extract" I64; 2)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
