use {crate::*, gluesql_core::prelude::Value::*};

test_case!(null_first_row_sum, {
    let g = get_tester!();

    g.run("CREATE TABLE T (val INTEGER NULL);").await;
    g.run("INSERT INTO T VALUES (NULL), (5), (3);").await;

    // SUM should ignore NULL values
    g.test(
        "SELECT SUM(val) FROM T",
        Ok(select!("SUM(val)"; I64; 8)),
    )
    .await;
});

test_case!(null_first_row_min, {
    let g = get_tester!();

    g.run("CREATE TABLE T (val INTEGER NULL);").await;
    g.run("INSERT INTO T VALUES (NULL), (5), (3);").await;

    // MIN should ignore NULL values, even when NULL is the first row
    g.test(
        "SELECT MIN(val) FROM T",
        Ok(select!("MIN(val)"; I64; 3)),
    )
    .await;
});

test_case!(null_first_row_max, {
    let g = get_tester!();

    g.run("CREATE TABLE T (val INTEGER NULL);").await;
    g.run("INSERT INTO T VALUES (NULL), (5), (3);").await;

    // MAX should ignore NULL values, even when NULL is the first row
    g.test(
        "SELECT MAX(val) FROM T",
        Ok(select!("MAX(val)"; I64; 5)),
    )
    .await;
});

test_case!(null_first_row_avg, {
    let g = get_tester!();

    g.run("CREATE TABLE T (val INTEGER NULL);").await;
    g.run("INSERT INTO T VALUES (NULL), (5), (3);").await;

    // AVG should ignore NULL values in both sum and count
    g.test(
        "SELECT AVG(val) FROM T",
        Ok(select!("AVG(val)"; F64; 4.0)),
    )
    .await;
});

test_case!(null_first_row_variance, {
    let g = get_tester!();

    g.run("CREATE TABLE T (val INTEGER NULL);").await;
    g.run("INSERT INTO T VALUES (NULL), (5), (3);").await;

    // VARIANCE should ignore NULL values
    g.test(
        "SELECT VARIANCE(val) FROM T",
        Ok(select!("VARIANCE(val)"; F64; 1.0)),
    )
    .await;
});

test_case!(null_first_row_stdev, {
    let g = get_tester!();

    g.run("CREATE TABLE T (val INTEGER NULL);").await;
    g.run("INSERT INTO T VALUES (NULL), (5), (3);").await;

    // STDEV should ignore NULL values
    g.test(
        "SELECT STDEV(val) FROM T",
        Ok(select!("STDEV(val)"; F64; 1.0)),
    )
    .await;
});

test_case!(all_null_aggregates, {
    let g = get_tester!();

    g.run("CREATE TABLE T (val INTEGER NULL);").await;
    g.run("INSERT INTO T VALUES (NULL), (NULL);").await;

    // All-NULL column should return NULL for non-COUNT aggregates
    let test_cases = [
        ("SELECT SUM(val) FROM T", select_with_null!("SUM(val)"; Null)),
        ("SELECT MIN(val) FROM T", select_with_null!("MIN(val)"; Null)),
        ("SELECT MAX(val) FROM T", select_with_null!("MAX(val)"; Null)),
        ("SELECT AVG(val) FROM T", select_with_null!("AVG(val)"; Null)),
        ("SELECT VARIANCE(val) FROM T", select_with_null!("VARIANCE(val)"; Null)),
        ("SELECT STDEV(val) FROM T", select_with_null!("STDEV(val)"; Null)),
        ("SELECT COUNT(val) FROM T", select!("COUNT(val)"; I64; 0)),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, Ok(expected)).await;
    }
});
