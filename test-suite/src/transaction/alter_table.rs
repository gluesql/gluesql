use {
    crate::*,
    gluesql_core::{error::FetchError, prelude::Value::*},
};

test_case!(alter_table_rename_table, {
    let g = get_tester!();

    for query in [
        "CREATE TABLE RenameTable (id INTEGER);",
        "INSERT INTO RenameTable VALUES (1);",
        "BEGIN;",
        "ALTER TABLE RenameTable RENAME TO NewName;",
    ] {
        g.run(query).await;
    }

    g.test(
        "SELECT * FROM RenameTable",
        Err(FetchError::TableNotFound("RenameTable".to_owned()).into()),
    )
    .await;
    g.test("SELECT * FROM NewName", Ok(select!(id I64; 1)))
        .await;

    g.run("ROLLBACK;").await;

    g.test(
        "SELECT * FROM NewName",
        Err(FetchError::TableNotFound("NewName".to_owned()).into()),
    )
    .await;
    g.test("SELECT * FROM RenameTable", Ok(select!(id I64; 1)))
        .await;
});

test_case!(alter_table_rename_column, {
    let g = get_tester!();

    g.run("CREATE TABLE RenameCol (id INTEGER);").await;
    g.run("INSERT INTO RenameCol VALUES (1);").await;

    // ROLLBACK
    g.run("BEGIN;").await;
    g.run("ALTER TABLE RenameCol RENAME COLUMN id TO new_id;")
        .await;
    g.test("SELECT * FROM RenameCol", Ok(select!(new_id I64; 1)))
        .await;
    g.run("ROLLBACK;").await;
    g.test("SELECT * FROM RenameCol", Ok(select!(id I64; 1)))
        .await;

    // COMMIT
    g.run("BEGIN;").await;
    g.run("ALTER TABLE RenameCol RENAME COLUMN id TO new_id;")
        .await;
    g.run("COMMIT;").await;
    g.test("SELECT * FROM RenameCol", Ok(select!(new_id I64; 1)))
        .await;
});

test_case!(alter_table_add_column, {
    let g = get_tester!();

    g.run("CREATE TABLE AddCol (id INTEGER);").await;
    g.run("INSERT INTO AddCol VALUES (1);").await;

    // ROLLBACK
    g.run("BEGIN;").await;
    g.run("ALTER TABLE AddCol ADD COLUMN new_col INTEGER DEFAULT 3;")
        .await;
    g.test(
        "SELECT * FROM AddCol",
        Ok(select!(
            id  | new_col
            I64 | I64;
            1     3
        )),
    )
    .await;
    g.run("ROLLBACK;").await;
    g.test("SELECT * FROM AddCol", Ok(select!(id I64; 1))).await;

    // COMMIT
    g.run("BEGIN;").await;
    g.run("ALTER TABLE AddCol ADD COLUMN new_col INTEGER DEFAULT 3;")
        .await;
    g.run("COMMIT;").await;
    g.test(
        "SELECT * FROM AddCol",
        Ok(select!(
            id  | new_col
            I64 | I64;
            1     3
        )),
    )
    .await;
});

test_case!(alter_table_drop_column, {
    let g = get_tester!();

    g.run("CREATE TABLE DropCol (id INTEGER, num INTEGER);")
        .await;
    g.run("INSERT INTO DropCol VALUES (1, 2);").await;

    // ROLLBACK
    g.run("BEGIN;").await;
    g.run("ALTER TABLE DropCol DROP COLUMN num;").await;
    g.test("SELECT * FROM DropCol", Ok(select!(id I64; 1)))
        .await;
    g.run("ROLLBACK;").await;
    g.test(
        "SELECT * FROM DropCol",
        Ok(select!(
            id  | num
            I64 | I64;
            1     2
        )),
    )
    .await;

    // COMMIT
    g.run("BEGIN;").await;
    g.run("ALTER TABLE DropCol DROP COLUMN num;").await;
    g.run("COMMIT;").await;
    g.test("SELECT * FROM DropCol", Ok(select!(id I64; 1)))
        .await;
});
