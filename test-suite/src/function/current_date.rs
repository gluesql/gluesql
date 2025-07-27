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

    g.named_test(
        "table with CURRENT_DATE default",
        "CREATE TABLE Item (date DATE DEFAULT CURRENT_DATE)",
        Ok(Payload::Create),
    )
    .await;

    g.named_test(
        "insert date values",
        "INSERT INTO Item VALUES
            ('2021-06-15'),
            ('9999-12-31');",
        Ok(Payload::Insert(2)),
    )
    .await;

    g.named_test(
        "filter by CURRENT_DATE",
        "SELECT date FROM Item WHERE date > CURRENT_DATE;",
        Ok(select!("date" Date; date!("9999-12-31"))),
    )
    .await;
});
