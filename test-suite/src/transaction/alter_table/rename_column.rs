use super::*;

test_case!(rename_column, {
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
