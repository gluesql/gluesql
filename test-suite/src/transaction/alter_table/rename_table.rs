use super::*;

test_case!(rename_table, {
    let g = get_tester!();

    for query in [
        "CREATE TABLE RenameTable (id INTEGER);",
        "INSERT INTO RenameTable VALUES (1);",
        "BEGIN;",
        "ALTER TABLE RenameTable RENAME TO NewName;",
    ] {
        g.run(query);
    }

    g.test(
        "SELECT * FROM RenameTable",
        Err(FetchError::TableNotFound("RenameTable".to_owned()).into()),
    );
    g.test("SELECT * FROM NewName", Ok(select!(id I64; 1)));

    g.run("ROLLBACK;");

    g.test(
        "SELECT * FROM NewName",
        Err(FetchError::TableNotFound("NewName".to_owned()).into()),
    );
    g.test("SELECT * FROM RenameTable", Ok(select!(id I64; 1)));
});
