use super::*;

test_case!(add_column, {
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
