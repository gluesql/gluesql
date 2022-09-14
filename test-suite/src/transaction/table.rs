use {
    crate::*,
    gluesql_core::{executor::FetchError, prelude::Value::*},
};

test_case!(create_drop_table, async move {
    // CREATE && ROLLBACK
    run!("BEGIN;");
    run!("CREATE TABLE Test (id INTEGER);");
    run!("INSERT INTO Test VALUES (1);");
    test!("SELECT * FROM Test;", Ok(select!(id I64; 1)));
    run!("ROLLBACK;");
    test!(
        "SELECT * FROM Test;",
        Err(FetchError::TableNotFound("Test".to_owned()).into())
    );

    // CREATE && COMMIT
    run!("BEGIN;");
    run!("CREATE TABLE Test (id INTEGER);");
    run!("INSERT INTO Test VALUES (3);");
    run!("COMMIT;");
    test!("SELECT * FROM Test;", Ok(select!(id I64; 3)));

    // DROP && ROLLBACK
    run!("BEGIN;");
    run!("DROP TABLE Test;");
    test!(
        "SELECT * FROM Test;",
        Err(FetchError::TableNotFound("Test".to_owned()).into())
    );
    run!("ROLLBACK;");
    test!("SELECT * FROM Test;", Ok(select!(id I64; 3)));

    // DROP && COMMIT
    run!("BEGIN;");
    run!("DROP TABLE Test;");
    run!("COMMIT;");
    test!(
        "SELECT * FROM Test;",
        Err(FetchError::TableNotFound("Test".to_owned()).into())
    );
});
