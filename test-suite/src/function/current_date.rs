use {
    crate::*,
    gluesql_core::prelude::{Payload, Value::*},
};

test_case!(current_date, {
    let g = get_tester!();

    macro_rules! date {
        ($date: expr) => {
            $date.parse().unwrap()
        };
    }

    let test_cases = [
        (
            "CREATE TABLE Item (date DATE DEFAULT CURRENT_DATE)",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO Item VALUES
                ('2021-06-15'),
                ('9999-12-31');",
            Ok(Payload::Insert(2)),
        ),
        (
            "SELECT date FROM Item WHERE date > CURRENT_DATE;",
            Ok(select!("date" Date; date!("9999-12-31"))),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
