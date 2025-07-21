use {
    crate::*,
    gluesql_core::prelude::{Payload, Value::*},
};

test_case!(current_datetime, {
    let g = get_tester!();

    let test_cases = [
        (
            "CREATE TABLE test_dates (id INT, event_date DATE)",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO test_dates VALUES (1, CURRENT_DATE)",
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT COUNT(*) as count FROM test_dates WHERE event_date = CURRENT_DATE",
            Ok(select!("count" I64; 1)),
        ),
        
        (
            "SELECT CURRENT_TIME > TIME '00:00:00' as is_after_midnight",
            Ok(select!("is_after_midnight" Bool; true)),
        ),
        
        (
            "SELECT CURRENT_TIMESTAMP > NOW() - INTERVAL '1' SECOND as within_second",
            Ok(select!("within_second" Bool; true)),
        ),
        
        (
            "SELECT ABS(EXTRACT(SECOND FROM (CURRENT_TIMESTAMP - NOW()))) < 1 as same_time",
            Ok(select!("same_time" Bool; true)),
        ),
        
        (
            "CREATE TABLE type_test (
                id INT,
                date_col DATE DEFAULT CURRENT_DATE,
                time_col TIME DEFAULT CURRENT_TIME, 
                timestamp_col TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO type_test (id) VALUES (1)",
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT COUNT(*) as count FROM type_test WHERE 
                date_col IS NOT NULL AND 
                time_col IS NOT NULL AND 
                timestamp_col IS NOT NULL",
            Ok(select!("count" I64; 1)),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});