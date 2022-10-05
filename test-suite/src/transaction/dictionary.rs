#![cfg(all(feature = "transaction"))]

use {crate::*, gluesql_core::prelude::*};

test_case!(dictionary, async move {
    let tables = |v: Vec<&str>| {
        Ok(Payload::ShowVariable(PayloadVariable::Tables(
            v.into_iter().map(ToOwned::to_owned).collect(),
        )))
    };

    run!("CREATE TABLE Garlic (id INTEGER);");
    test!("SHOW TABLES;", tables(vec!["Garlic"]));

    run!("BEGIN;");
    test!("SHOW TABLES;", tables(vec!["Garlic"]));

    run!("CREATE TABLE Noodle (id INTEGER);");
    test!("SHOW TABLES;", tables(vec!["Garlic", "Noodle"]));

    run!("ROLLBACK;");
    test!("SHOW TABLES;", tables(vec!["Garlic"]));

    run!("BEGIN;");
    run!("CREATE TABLE Apple (id INTEGER);");
    run!("CREATE TABLE Rice (id INTEGER);");
    test!("SHOW TABLES;", tables(vec!["Apple", "Garlic", "Rice"]));

    run!("COMMIT;");
    test!("SHOW TABLES;", tables(vec!["Apple", "Garlic", "Rice"]));
});
