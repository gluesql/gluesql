use {crate::*, gluesql_core::prelude::Value::*};

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
});
