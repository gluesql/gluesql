use {
    crate::*,
    gluesql_core::{executor::FetchError, prelude::Value::*},
};

test_case!(create_drop_table, async move {
    // CREATE && ROLLBACK
    run!("BEGIN;");
    run!("CREATE TABLE Test (id INTEGER);");
    run!("INSERT INTO Test VALUES (1);");
    test!(Ok(select!(id I64; 1)), "SELECT * FROM Test;");
    run!("ROLLBACK;");
    test!(
        Err(FetchError::TableNotFound("Test".to_owned()).into()),
        "SELECT * FROM Test;"
    );

    // CREATE && COMMIT
    run!("BEGIN;");
    run!("CREATE TABLE Test (id INTEGER);");
    run!("INSERT INTO Test VALUES (3);");
    run!("COMMIT;");
    test!(Ok(select!(id I64; 3)), "SELECT * FROM Test;");

    // DROP && ROLLBACK
    run!("BEGIN;");
    run!("DROP TABLE Test;");
    test!(
        Err(FetchError::TableNotFound("Test".to_owned()).into()),
        "SELECT * FROM Test;"
    );
    run!("ROLLBACK;");
    test!(Ok(select!(id I64; 3)), "SELECT * FROM Test;");

    // DROP && COMMIT
    run!("BEGIN;");
    run!("DROP TABLE Test;");
    run!("COMMIT;");
    test!(
        Err(FetchError::TableNotFound("Test".to_owned()).into()),
        "SELECT * FROM Test;"
    );
});
