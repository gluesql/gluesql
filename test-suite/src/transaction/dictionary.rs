use {crate::*, gluesql_core::prelude::*};

test_case!(dictionary, {
    let g = get_tester!();

    let tables = |v: Vec<&str>| {
        Ok(Payload::ShowVariable(PayloadVariable::Tables(
            v.into_iter().map(ToOwned::to_owned).collect(),
        )))
    };

    g.run("CREATE TABLE Garlic (id INTEGER);");
    g.test("SHOW TABLES;", tables(vec!["Garlic"]));

    g.run("BEGIN;");
    g.test("SHOW TABLES;", tables(vec!["Garlic"]));

    g.run("CREATE TABLE Noodle (id INTEGER);");
    g.test("SHOW TABLES;", tables(vec!["Garlic", "Noodle"]));

    g.run("ROLLBACK;");
    g.test("SHOW TABLES;", tables(vec!["Garlic"]));

    g.run("BEGIN;");
    g.run("CREATE TABLE Apple (id INTEGER);");
    g.run("CREATE TABLE Rice (id INTEGER);");
    g.test("SHOW TABLES;", tables(vec!["Apple", "Garlic", "Rice"]));

    g.run("COMMIT;");
    g.test("SHOW TABLES;", tables(vec!["Apple", "Garlic", "Rice"]));
});
