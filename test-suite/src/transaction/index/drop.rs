use super::*;

test_case!(drop, {
    let g = get_tester!();

    g.run("CREATE TABLE IdxDrop (id INTEGER);");
    g.run("INSERT INTO IdxDrop VALUES (1);");
    g.run("CREATE INDEX idx_id ON IdxDrop (id);");

    // ROLLBACK
    g.run("BEGIN;");
    g.run("DROP INDEX IdxDrop.idx_id;");
    g.test_idx(
        "SELECT id FROM IdxDrop WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(),
    );
    g.run("ROLLBACK;");
    g.test_idx(
        "SELECT id FROM IdxDrop WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(idx_id, Eq, "1"),
    );

    // COMMIT;
    g.run("BEGIN;");
    g.run("DROP INDEX IdxDrop.idx_id;");
    g.test_idx(
        "SELECT id FROM IdxDrop WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(),
    );
    g.run("COMMIT;");
    g.test_idx(
        "SELECT id FROM IdxDrop WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(),
    );
});
