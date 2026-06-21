use {
    crate::*,
    gluesql_core::{error::FetchError, prelude::Value::*},
};

test_case!(create_drop_table, {
    let g = get_tester!();

    // CREATE && ROLLBACK
    g.run("BEGIN;");
    g.run("CREATE TABLE Test (id INTEGER);");
    g.run("INSERT INTO Test VALUES (1);");
    g.test("SELECT * FROM Test;", Ok(select!(id I64; 1)));
    g.run("ROLLBACK;");
    g.test(
        "SELECT * FROM Test;",
        Err(FetchError::TableNotFound("Test".to_owned()).into()),
    );

    // CREATE && COMMIT
    g.run("BEGIN;");
    g.run("CREATE TABLE Test (id INTEGER);");
    g.run("INSERT INTO Test VALUES (3);");
    g.run("COMMIT;");
    g.test("SELECT * FROM Test;", Ok(select!(id I64; 3)));

    // DROP && ROLLBACK
    g.run("BEGIN;");
    g.run("DROP TABLE Test;");
    g.test(
        "SELECT * FROM Test;",
        Err(FetchError::TableNotFound("Test".to_owned()).into()),
    );
    g.run("ROLLBACK;");
    g.test("SELECT * FROM Test;", Ok(select!(id I64; 3)));

    // DROP && COMMIT
    g.run("BEGIN;");
    g.run("DROP TABLE Test;");
    g.run("COMMIT;");
    g.test(
        "SELECT * FROM Test;",
        Err(FetchError::TableNotFound("Test".to_owned()).into()),
    );
});
