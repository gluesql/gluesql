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

    // UNION ALL with LIMIT but no ORDER BY: rows are streamed lazily and
    // truncated without materialising the full result set.
    g.named_test(
        "UNION ALL with LIMIT streams lazily",
        "SELECT id FROM A UNION ALL SELECT id FROM B LIMIT 3",
        Ok(select!(id; I64; 1; 2; 3)),
    )
    .await;

    // UNION ALL with OFFSET but no ORDER BY.
    // A = (1,2,3), B = (2,3,4) → concat: 1,2,3,2,3,4; skip 4 → 3,4
    g.named_test(
        "UNION ALL with OFFSET streams lazily",
        "SELECT id FROM A UNION ALL SELECT id FROM B OFFSET 4",
        Ok(select!(id; I64; 3; 4)),
    )
    .await;

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

    g.named_test(
        "UNION with VALUES",
        "SELECT id FROM A WHERE id = 1 UNION VALUES (10), (20) ORDER BY id",
        Ok(select!(id; I64; 1; 10; 20)),
    )
    .await;

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

    g.named_test(
        "UNION inside IN subquery",
        "SELECT id FROM A WHERE id IN (SELECT id FROM B UNION SELECT id FROM C) ORDER BY id",
        Ok(select!(id; I64; 2; 3)),
    )
    .await;

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

    // A UNION with a type mismatch inside a derived table must also be caught
    // at plan time — the outer SELECT body is not a Union, so validate_set_expr
    // must recurse into the derived subquery.
    g.named_test(
        "nested UNION in derived table with type mismatch is rejected at plan time",
        "SELECT * FROM (SELECT 1, 'a' UNION SELECT 2, 3) AS t",
        Err(PlanError::UnionColumnTypeMismatch {
            index: 1,
            left: "TEXT".to_owned(),
            right: "INT".to_owned(),
        }
        .into()),
    )
    .await;

    // A UNION with matching types inside a derived table must still work.
    g.named_test(
        "nested UNION in derived table with matching types is accepted",
        "SELECT * FROM (SELECT 1, 2 UNION SELECT 3, 4) AS t ORDER BY 1",
        Ok(select!(
            "1"  | "2";
            I64  | I64;
            1      2;
            3      4
        )),
    )
    .await;

    // UNION ORDER BY positional index must resolve to the output column,
    // not evaluate the integer literal as a constant.
    g.named_test(
        "UNION ORDER BY positional index 1 ASC",
        "SELECT 3 UNION SELECT 1 ORDER BY 1 ASC",
        Ok(select!(
            "3";
            I64;
            1;
            3
        )),
    )
    .await;

    g.named_test(
        "UNION ORDER BY positional index 1 DESC",
        "SELECT 1 UNION SELECT 3 ORDER BY 1 DESC",
        Ok(select!(
            "1";
            I64;
            3;
            1
        )),
    )
    .await;

    g.named_test(
        "UNION ALL ORDER BY positional index 2 ASC",
        "SELECT 10, 'b' UNION ALL SELECT 5, 'a' ORDER BY 2 ASC",
        Ok(select!(
            "10" | "'b'";
            I64  | Str;
            5      "a".to_owned();
            10     "b".to_owned()
        )),
    )
    .await;

    g.named_test(
        "UNION ORDER BY positional index out of range returns error",
        "SELECT 1 UNION SELECT 2 ORDER BY 0",
        Err(gluesql_core::executor::SortError::ColumnIndexOutOfRange(0).into()),
    )
    .await;

    // UNION ALL + ORDER BY: the materialization path must sort before returning.
    g.named_test(
        "UNION ALL ORDER BY 1 ASC sorts correctly",
        "SELECT 3 UNION ALL SELECT 3 UNION ALL SELECT 1 ORDER BY 1 ASC",
        Ok(select!("3"; I64; 1; 3; 3)),
    )
    .await;

    g.named_test(
        "UNION ALL ORDER BY 1 DESC with LIMIT",
        "SELECT 1 UNION ALL SELECT 3 UNION ALL SELECT 2 ORDER BY 1 DESC LIMIT 2",
        Ok(select!("1"; I64; 3; 2)),
    )
    .await;

    // UNION DISTINCT without ORDER BY: lazy try_filter path.
    // With LIMIT the stream must stop as soon as enough unique rows are found
    // without materialising everything.
    g.named_test(
        "UNION DISTINCT without ORDER BY deduplicates lazily",
        "SELECT id FROM A UNION SELECT id FROM A ORDER BY id",
        Ok(select!(id; I64; 1; 2; 3)),
    )
    .await;

    g.named_test(
        "UNION DISTINCT without ORDER BY respects LIMIT lazily",
        "SELECT id FROM A UNION SELECT id FROM B LIMIT 2",
        Ok(select!(id; I64; 1; 2)),
    )
    .await;

    // UNION DISTINCT + ORDER BY: must materialise, deduplicate, then sort.
    g.named_test(
        "UNION DISTINCT with ORDER BY deduplicates before sorting",
        "SELECT 3 UNION SELECT 1 UNION SELECT 3 ORDER BY 1 ASC",
        Ok(select!("3"; I64; 1; 3)),
    )
    .await;

    g.named_test(
        "UNION DISTINCT with ORDER BY and LIMIT",
        "SELECT 3 UNION SELECT 1 UNION SELECT 2 ORDER BY 1 DESC LIMIT 2",
        Ok(select!("3"; I64; 3; 2)),
    )
    .await;
});
