use super::*;

test_case!(table, {
    let g = get_tester!();

    g.run("DROP TABLE IF EXISTS Test;");
    g.run("CREATE TABLE Test (id INTEGER);");
    g.run("INSERT INTO Test VALUES (1), (2);");
    g.run("CREATE INDEX idx_id ON Test (id)");
    g.test_idx(
        "SELECT * FROM Test WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(idx_id, Eq, "1"),
    );

    g.run("DROP TABLE Test;");
    g.test(
        "SELECT * FROM Test;",
        Err(FetchError::TableNotFound("Test".to_owned()).into()),
    );

    g.run("CREATE TABLE Test (id INTEGER);");
    g.run("INSERT INTO Test VALUES (3), (4);");
    g.test_idx(
        "SELECT * FROM Test WHERE id = 3",
        Ok(select!(id I64; 3)),
        idx!(),
    );

    g.run("CREATE INDEX idx_id ON Test (id)");
    g.test_idx(
        "SELECT * FROM Test WHERE id < 10",
        Ok(select!(id I64; 3; 4)),
        idx!(idx_id, Lt, "10"),
    );

    g.test(
        "DROP INDEX Test",
        Err(TranslateError::InvalidParamsInDropIndex.into()),
    );
    g.test(
        "DROP INDEX Test.idx_id.IndexC",
        Err(TranslateError::InvalidParamsInDropIndex.into()),
    );
});
