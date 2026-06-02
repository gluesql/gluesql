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

test_case!(alter_table_rename_column, {
    let g = get_tester!();

    g.run("CREATE TABLE RenameCol (id INTEGER);");
    g.run("INSERT INTO RenameCol VALUES (1);");

    // ROLLBACK
    g.run("BEGIN;");
    g.run("ALTER TABLE RenameCol RENAME COLUMN id TO new_id;");
    g.test("SELECT * FROM RenameCol", Ok(select!(new_id I64; 1)));
    g.run("ROLLBACK;");
    g.test("SELECT * FROM RenameCol", Ok(select!(id I64; 1)));

    // COMMIT
    g.run("BEGIN;");
    g.run("ALTER TABLE RenameCol RENAME COLUMN id TO new_id;");
    g.run("COMMIT;");
    g.test("SELECT * FROM RenameCol", Ok(select!(new_id I64; 1)));
});

test_case!(alter_table_add_column, {
    let g = get_tester!();

    g.run("CREATE TABLE AddCol (id INTEGER);");
    g.run("INSERT INTO AddCol VALUES (1);");

    // ROLLBACK
    g.run("BEGIN;");
    g.run("ALTER TABLE AddCol ADD COLUMN new_col INTEGER DEFAULT 3;");
    g.test(
        "SELECT * FROM AddCol",
        Ok(select!(
            id  | new_col
            I64 | I64;
            1     3
        )),
    );
    g.run("ROLLBACK;");
    g.test("SELECT * FROM AddCol", Ok(select!(id I64; 1)));

    // COMMIT
    g.run("BEGIN;");
    g.run("ALTER TABLE AddCol ADD COLUMN new_col INTEGER DEFAULT 3;");
    g.run("COMMIT;");
    g.test(
        "SELECT * FROM AddCol",
        Ok(select!(
            id  | new_col
            I64 | I64;
            1     3
        )),
    );
});

test_case!(alter_table_drop_column, {
    let g = get_tester!();

    g.run("CREATE TABLE DropCol (id INTEGER, num INTEGER);");
    g.run("INSERT INTO DropCol VALUES (1, 2);");

    // ROLLBACK
    g.run("BEGIN;");
    g.run("ALTER TABLE DropCol DROP COLUMN num;");
    g.test("SELECT * FROM DropCol", Ok(select!(id I64; 1)));
    g.run("ROLLBACK;");
    g.test(
        "SELECT * FROM DropCol",
        Ok(select!(
            id  | num
            I64 | I64;
            1     2
        )),
    );

    // COMMIT
    g.run("BEGIN;");
    g.run("ALTER TABLE DropCol DROP COLUMN num;");
    g.run("COMMIT;");
    g.test("SELECT * FROM DropCol", Ok(select!(id I64; 1)));
});
