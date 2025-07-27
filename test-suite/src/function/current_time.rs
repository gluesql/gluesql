use {
    crate::*,
    gluesql_core::prelude::{Payload, Value::*},
};

test_case!(current_time, {
    let g = get_tester!();

    g.named_test(
        "table with CURRENT_TIME default",
        "CREATE TABLE Item (time TIME DEFAULT CURRENT_TIME)",
        Ok(Payload::Create),
    )
    .await;

    g.named_test(
        "insert time values",
        "INSERT INTO Item VALUES
            ('06:42:40'),
            ('23:59:59');",
        Ok(Payload::Insert(2)),
    )
    .await;

    g.named_test(
        "CURRENT_TIME is not null",
        "SELECT CURRENT_TIME IS NOT NULL as is_not_null",
        Ok(select!("is_not_null" Bool; true)),
    )
    .await;

    g.named_test(
        "CURRENT_TIME in valid range",
        "SELECT CURRENT_TIME >= TIME '00:00:00' AND CURRENT_TIME <= TIME '23:59:59' as is_valid_range",
        Ok(select!("is_valid_range" Bool; true)),
    )
    .await;
});
