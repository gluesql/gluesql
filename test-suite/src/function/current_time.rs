use {
    crate::*,
    gluesql_core::prelude::{Payload, Value::*},
};

test_case!(current_time, {
    let g = get_tester!();

    let test_cases = [
        (
            "CREATE TABLE Item (time TIME DEFAULT CURRENT_TIME)",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO Item VALUES
                ('06:42:40'),
                ('23:59:59');",
            Ok(Payload::Insert(2)),
        ),
        (
            "SELECT CURRENT_TIME IS NOT NULL as is_not_null",
            Ok(select!("is_not_null" Bool; true)),
        ),
        (
            "SELECT CURRENT_TIME >= TIME '00:00:00' AND CURRENT_TIME <= TIME '23:59:59' as is_valid_range",
            Ok(select!("is_valid_range" Bool; true)),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
