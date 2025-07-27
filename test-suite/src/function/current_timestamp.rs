use {
    crate::*,
    gluesql_core::prelude::{Payload, Value::*},
};

test_case!(current_timestamp, {
    let g = get_tester!();

    macro_rules! t {
        ($timestamp: expr) => {
            $timestamp.parse().unwrap()
        };
    }

    let test_cases = [
        (
            "CREATE TABLE Item (timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO Item VALUES
                ('2021-10-13T06:42:40.364832862'),
                ('9999-12-31T23:59:40.364832862');",
            Ok(Payload::Insert(2)),
        ),
        (
            "SELECT timestamp FROM Item WHERE timestamp > CURRENT_TIMESTAMP;",
            Ok(select!("timestamp" Timestamp; t!("9999-12-31T23:59:40.364832862"))),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
