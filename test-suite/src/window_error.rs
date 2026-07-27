use crate::*;

test_case!(window_error, {
    let glue = get_glue!();

    glue.execute("CREATE TABLE T (id INTEGER, region TEXT, v INTEGER);")
        .unwrap();
    glue.execute("INSERT INTO T (id, region, v) VALUES (1, 'a', 10), (2, 'b', 20);")
        .unwrap();

    // A well-formed window query must work; the cases below must fail because
    // they are invalid uses, not because window functions are unsupported.
    assert!(
        glue.execute("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM T")
            .is_ok(),
        "a valid window query should succeed"
    );

    let cases = [
        (
            "SELECT id FROM T WHERE ROW_NUMBER() OVER () = 1",
            "window function in WHERE",
        ),
        (
            "SELECT id FROM T ORDER BY ROW_NUMBER() OVER ()",
            "window function in query-level ORDER BY",
        ),
        (
            "SELECT SUM(v) OVER () FROM T GROUP BY region",
            "window function combined with GROUP BY",
        ),
        (
            "SELECT DISTINCT ROW_NUMBER() OVER () FROM T",
            "window function combined with DISTINCT",
        ),
        (
            "SELECT SUM(ROW_NUMBER() OVER ()) FROM T",
            "window function nested in an aggregate",
        ),
        (
            "SELECT SUM(ROW_NUMBER() OVER ()) OVER () FROM T",
            "window function nested in a window function",
        ),
        (
            "SELECT LAG(v, -1) OVER (ORDER BY id) FROM T",
            "negative LAG offset",
        ),
        (
            "SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM T",
            "explicit window frame",
        ),
        (
            "SELECT SUM(DISTINCT v) OVER () FROM T",
            "DISTINCT inside a window aggregate",
        ),
        (
            "SELECT region, COUNT(*) FROM T GROUP BY region HAVING SUM(v) OVER () > 0",
            "window function in HAVING",
        ),
        (
            "SELECT v FROM T GROUP BY ROW_NUMBER() OVER ()",
            "window function in a GROUP BY expression",
        ),
        (
            "SELECT a.id FROM T a JOIN T b ON ROW_NUMBER() OVER () = b.id",
            "window function in a JOIN condition",
        ),
        (
            "SELECT LAG(v, 1.5) OVER (ORDER BY id) FROM T",
            "fractional LAG offset",
        ),
        (
            "SELECT LAG(v, id) OVER (ORDER BY id) FROM T",
            "non-literal LAG offset",
        ),
    ];

    for (sql, label) in cases {
        assert!(glue.execute(sql).is_err(), "expected an error: {label}");
    }
});
