#![cfg(all(feature = "transaction", feature = "metadata"))]

use {crate::*, gluesql_core::prelude::*};

test_case!(metadata, async move {
    let tables = |v: Vec<&str>| {
        Ok(Payload::ShowVariable(PayloadVariable::Tables(
            v.into_iter().map(ToOwned::to_owned).collect(),
        )))
    };

    run!("CREATE TABLE Garlic (id INTEGER);");
    test!(tables(vec!["Garlic"]), "SHOW TABLES;");

    run!("BEGIN;");
    test!(tables(vec!["Garlic"]), "SHOW TABLES;");

    run!("CREATE TABLE Noodle (id INTEGER);");
    test!(tables(vec!["Garlic", "Noodle"]), "SHOW TABLES;");

    run!("ROLLBACK;");
    test!(tables(vec!["Garlic"]), "SHOW TABLES;");

    run!("BEGIN;");
    run!("CREATE TABLE Apple (id INTEGER);");
    run!("CREATE TABLE Rice (id INTEGER);");
    test!(tables(vec!["Apple", "Garlic", "Rice"]), "SHOW TABLES;");

    run!("COMMIT;");
    test!(tables(vec!["Apple", "Garlic", "Rice"]), "SHOW TABLES;");
});
