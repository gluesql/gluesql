use super::*;

test_case!(drop_column, {
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
