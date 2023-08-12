use {
    crate::*,
    gluesql_core::{error::FetchError, prelude::Value::*},
};

test_case!(create_drop_table, {
    let g = get_tester!();

    // CREATE && ROLLBACK
    g.run("BEGIN;").await;
    g.run("CREATE TABLE Test (id INTEGER);").await;
    g.run("INSERT INTO Test VALUES (1);").await;
    g.test("SELECT * FROM Test;", Ok(select!(id I64; 1))).await;
    g.run("ROLLBACK;").await;
    g.test(
        "SELECT * FROM Test;",
        Err(FetchError::TableNotFound("Test".to_owned()).into()),
    )
    .await;

    // CREATE && COMMIT
    g.run("BEGIN;").await;
    g.run("CREATE TABLE Test (id INTEGER);").await;
    g.run("INSERT INTO Test VALUES (3);").await;
    g.run("COMMIT;").await;
    g.test("SELECT * FROM Test;", Ok(select!(id I64; 3))).await;

    // DROP && ROLLBACK
    g.run("BEGIN;").await;
    g.run("DROP TABLE Test;").await;
    g.test(
        "SELECT * FROM Test;",
        Err(FetchError::TableNotFound("Test".to_owned()).into()),
    )
    .await;
    g.run("ROLLBACK;").await;
    g.test("SELECT * FROM Test;", Ok(select!(id I64; 3))).await;

    // DROP && COMMIT
    g.run("BEGIN;").await;
    g.run("DROP TABLE Test;").await;
    g.run("COMMIT;").await;
    g.test(
        "SELECT * FROM Test;",
        Err(FetchError::TableNotFound("Test".to_owned()).into()),
    )
    .await;
});
