use {
    crate::*,
    gluesql_core::{
        error::SelectError,
        plan::PlanError,
        prelude::{Payload, Value::*},
    },
};

test_case!(union, {
    let g = get_tester!();

    g.run("CREATE TABLE A (id INTEGER, name TEXT)").await;
    g.run("CREATE TABLE B (id INTEGER, name TEXT)").await;

    g.run("INSERT INTO A VALUES (1, 'Apple'), (2, 'Banana'), (3, 'Cherry')")
        .await;
    g.run("INSERT INTO B VALUES (2, 'Banana'), (3, 'Cherry'), (4, 'Date')")
        .await;

    // UNION deduplicates rows
    g.named_test(
        "UNION removes duplicates",
        "SELECT id, name FROM A UNION SELECT id, name FROM B ORDER BY id",
        Ok(select!(
            id   | name;
            I64  | Str;
            1      "Apple".to_owned();
            2      "Banana".to_owned();
            3      "Cherry".to_owned();
            4      "Date".to_owned()
        )),
    )
    .await;

    // UNION ALL keeps duplicates
    g.named_test(
        "UNION ALL keeps duplicate rows",
        "SELECT id, name FROM A UNION ALL SELECT id, name FROM B ORDER BY id",
        Ok(select!(
            id   | name;
            I64  | Str;
            1      "Apple".to_owned();
            2      "Banana".to_owned();
            2      "Banana".to_owned();
            3      "Cherry".to_owned();
            3      "Cherry".to_owned();
            4      "Date".to_owned()
        )),
    )
    .await;

    // UNION ALL as a derived subquery: the inner UNION ALL has no ORDER BY /
    // LIMIT / OFFSET so it takes the lazy-chain streaming path; the outer
    // ORDER BY makes the result deterministic for comparison.
    g.named_test(
        "UNION ALL as derived subquery streams lazily",
        "SELECT id, name FROM (SELECT id, name FROM A UNION ALL SELECT id, name FROM B) AS t ORDER BY id",
        Ok(select!(
            id   | name;
            I64  | Str;
            1      "Apple".to_owned();
            2      "Banana".to_owned();
            2      "Banana".to_owned();
            3      "Cherry".to_owned();
            3      "Cherry".to_owned();
            4      "Date".to_owned()
        )),
    )
    .await;

    // UNION with WHERE clause on each side
    g.named_test(
        "UNION with WHERE on each side",
        "SELECT id FROM A WHERE id < 3 UNION SELECT id FROM B WHERE id > 2 ORDER BY id",
        Ok(select!(id; I64; 1; 2; 3; 4)),
    )
    .await;

    // chained UNION (three queries)
    g.run("CREATE TABLE C (id INTEGER, name TEXT)").await;
    g.run("INSERT INTO C VALUES (5, 'Elderberry')").await;

    g.named_test(
        "three-way UNION",
        "SELECT id, name FROM A UNION SELECT id, name FROM B UNION SELECT id, name FROM C ORDER BY id",
        Ok(select!(
            id   | name;
            I64  | Str;
            1      "Apple".to_owned();
            2      "Banana".to_owned();
            3      "Cherry".to_owned();
            4      "Date".to_owned();
            5      "Elderberry".to_owned()
        )),
    )
    .await;

    // UNION with VALUES
    g.named_test(
        "UNION with VALUES",
        "SELECT id FROM A WHERE id = 1 UNION VALUES (10), (20) ORDER BY id",
        Ok(select!(id; I64; 1; 10; 20)),
    )
    .await;

    // UNION as a derived subquery (covers fetch.rs SetExpr::Union branch)
    g.named_test(
        "UNION as derived subquery",
        "SELECT id, name FROM (SELECT id, name FROM A UNION SELECT id, name FROM B) AS t ORDER BY id",
        Ok(select!(
            id   | name;
            I64  | Str;
            1      "Apple".to_owned();
            2      "Banana".to_owned();
            3      "Cherry".to_owned();
            4      "Date".to_owned()
        )),
    )
    .await;

    // CREATE TABLE AS UNION (covers alter/table.rs SetExpr::Union branch)
    g.named_test(
        "CREATE TABLE AS UNION query",
        "CREATE TABLE Merged AS SELECT id, name FROM A UNION SELECT id, name FROM B",
        Ok(Payload::Create),
    )
    .await;

    g.named_test(
        "SELECT from table created via UNION CTAS",
        "SELECT id, name FROM Merged ORDER BY id",
        Ok(select!(
            id   | name;
            I64  | Str;
            1      "Apple".to_owned();
            2      "Banana".to_owned();
            3      "Cherry".to_owned();
            4      "Date".to_owned()
        )),
    )
    .await;

    // UNION inside IN subquery (covers plan/expr/evaluable.rs SetExpr::Union branch)
    g.named_test(
        "UNION inside IN subquery",
        "SELECT id FROM A WHERE id IN (SELECT id FROM B UNION SELECT id FROM C) ORDER BY id",
        Ok(select!(id; I64; 2; 3)),
    )
    .await;

    // Column count mismatch returns an error (covers SelectError::UnionColumnCountMismatch)
    g.named_test(
        "UNION column count mismatch returns error",
        "SELECT id, name FROM A UNION SELECT id FROM B",
        Err(SelectError::UnionColumnCountMismatch { left: 2, right: 1 }.into()),
    )
    .await;

    // UNION with literal type mismatch is rejected at plan time
    g.named_test(
        "UNION literal type mismatch returns error",
        "SELECT 1, 'a' UNION SELECT 2, 3",
        Err(PlanError::UnionColumnTypeMismatch {
            index: 1,
            left: "TEXT".to_owned(),
            right: "INT".to_owned(),
        }
        .into()),
    )
    .await;
});
