use {crate::*, gluesql_core::prelude::*};

test_case!(dictionary, {
    let g = get_tester!();

    let tables = |v: Vec<&str>| {
        Ok(Payload::ShowVariable(PayloadVariable::Tables(
            v.into_iter().map(ToOwned::to_owned).collect(),
        )))
    };

    g.run("CREATE TABLE Garlic (id INTEGER);").await;
    g.test("SHOW TABLES;", tables(vec!["Garlic"])).await;

    g.run("BEGIN;").await;
    g.test("SHOW TABLES;", tables(vec!["Garlic"])).await;

    g.run("CREATE TABLE Noodle (id INTEGER);").await;
    g.test("SHOW TABLES;", tables(vec!["Garlic", "Noodle"]))
        .await;

    g.run("ROLLBACK;").await;
    g.test("SHOW TABLES;", tables(vec!["Garlic"])).await;

    g.run("BEGIN;").await;
    g.run("CREATE TABLE Apple (id INTEGER);").await;
    g.run("CREATE TABLE Rice (id INTEGER);").await;
    g.test("SHOW TABLES;", tables(vec!["Apple", "Garlic", "Rice"]))
        .await;

    g.run("COMMIT;").await;
    g.test("SHOW TABLES;", tables(vec!["Apple", "Garlic", "Rice"]))
        .await;
});
