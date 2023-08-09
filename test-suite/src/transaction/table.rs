use {
    crate::*,
    gluesql_core::{error::FetchError, prelude::Value::*},
};

test_case!(create_drop_table, async move {
    let g = get_tester!();

    // CREATE && ROLLBACK
    g.run("BEGIN;").await.unwrap();
    g.run("CREATE TABLE Test (id INTEGER);").await.unwrap();
    g.run("INSERT INTO Test VALUES (1);").await.unwrap();
    g.test("SELECT * FROM Test;", Ok(select!(id I64; 1))).await;
    g.run("ROLLBACK;").await.unwrap();
    g.test(
        "SELECT * FROM Test;",
        Err(FetchError::TableNotFound("Test".to_owned()).into()),
    )
    .await;

    // CREATE && COMMIT
    g.run("BEGIN;").await.unwrap();
    g.run("CREATE TABLE Test (id INTEGER);").await.unwrap();
    g.run("INSERT INTO Test VALUES (3);").await.unwrap();
    g.run("COMMIT;").await.unwrap();
    g.test("SELECT * FROM Test;", Ok(select!(id I64; 3))).await;

    // DROP && ROLLBACK
    g.run("BEGIN;").await.unwrap();
    g.run("DROP TABLE Test;").await.unwrap();
    g.test(
        "SELECT * FROM Test;",
        Err(FetchError::TableNotFound("Test".to_owned()).into()),
    )
    .await;
    g.run("ROLLBACK;").await.unwrap();
    g.test("SELECT * FROM Test;", Ok(select!(id I64; 3))).await;

    // DROP && COMMIT
    g.run("BEGIN;").await.unwrap();
    g.run("DROP TABLE Test;").await.unwrap();
    g.run("COMMIT;").await.unwrap();
    g.test(
        "SELECT * FROM Test;",
        Err(FetchError::TableNotFound("Test".to_owned()).into()),
    )
    .await;
});
