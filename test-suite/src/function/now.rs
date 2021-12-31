use {
    crate::*,
    gluesql_core::prelude::{Payload, Value::*},
};

test_case!(now, async move {
    macro_rules! t {
        ($timestamp: expr) => {
            $timestamp.parse().unwrap()
        };
    }

    let test_cases = vec![
        ("CREATE TABLE Item (time TIMESTAMP)", Ok(Payload::Create)),
        (
            r#"INSERT INTO Item VALUES
                ("2021-10-13T06:42:40.364832862"),
                ("9999-12-31T23:59:40.364832862");"#,
            Ok(Payload::Insert(2)),
        ),
        (
            "SELECT time FROM Item WHERE time > NOW();",
            Ok(select!("time" Timestamp; t!("9999-12-31T23:59:40.364832862"))),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
