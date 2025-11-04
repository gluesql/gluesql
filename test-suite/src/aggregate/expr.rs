use {crate::*, gluesql_core::prelude::Value::*};

test_case!(expr, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE Item (
            id INTEGER,
            quantity INTEGER,
            age INTEGER NULL,
            total INTEGER
        );
    ",
    )
    .await;
    g.run(
        "
        INSERT INTO Item (id, quantity, age, total) VALUES
            (1, 10,   11, 1),
            (2,  0,   90, 2),
            (3,  9, NULL, 3),
            (4,  3,    3, 1),
            (5, 25, NULL, 1);
    ",
    )
    .await;

    g.named_test(
        "BETWEEN with aggregates",
        "SELECT SUM(quantity) BETWEEN MIN(quantity) AND MAX(quantity) AS test FROM Item;",
        Ok(select!("test" Bool; false)),
    )
    .await;

    g.named_test(
        "CASE comparing aggregates",
        "SELECT CASE SUM(quantity) WHEN MIN(quantity) THEN MAX(id) ELSE COUNT(id) END AS test FROM Item;",
        Ok(select!("test" I64; 5)),
    )
    .await;

    g.named_test(
        "CASE WHEN with aggregate condition",
        "SELECT CASE WHEN SUM(quantity) > 30 THEN MAX(id) ELSE MIN(id) END AS test FROM Item;",
        Ok(select!("test" I64; 5)),
    )
    .await;

    g.named_test(
        "aggregate nested in scalar function",
        "SELECT ABS(SUM(quantity) - MIN(quantity)) AS val FROM Item;",
        Ok(select!("val" I64; 47)),
    )
    .await;

    g.named_test(
        "functional expression inside aggregate",
        "SELECT SUM(ABS(quantity)) AS val FROM Item;",
        Ok(select!("val" I64; 47)),
    )
    .await;

    g.named_test(
        "aggregate mixed with nested case and functions",
        "
            SELECT
                ABS(SUM(quantity))
                + CASE
                    WHEN COUNT(*) > 3 THEN MAX(quantity)
                    ELSE MIN(quantity)
                END AS val
            FROM Item;
        ",
        Ok(select!("val" I64; 72)),
    )
    .await;
});
