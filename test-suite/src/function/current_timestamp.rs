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

    g.named_test(
        "table with CURRENT_TIMESTAMP default",
        "CREATE TABLE Item (timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        Ok(Payload::Create),
    )
    .await;

    g.named_test(
        "insert timestamp values",
        "INSERT INTO Item VALUES
            ('2021-10-13T06:42:40.364832862'),
            ('9999-12-31T23:59:40.364832862');",
        Ok(Payload::Insert(2)),
    )
    .await;

    g.named_test(
        "filter by CURRENT_TIMESTAMP",
        "SELECT timestamp FROM Item WHERE timestamp > CURRENT_TIMESTAMP;",
        Ok(select!("timestamp" Timestamp; t!("9999-12-31T23:59:40.364832862"))),
    )
    .await;
});
