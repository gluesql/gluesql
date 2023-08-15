use {
    crate::*,
    gluesql_core::{ast::IndexOperator::*, prelude::Value::*},
};

test_case!(index_create, {
    let g = get_tester!();

    g.run("CREATE TABLE IdxCreate (id INTEGER);").await;
    g.run("INSERT INTO IdxCreate VALUES (1);").await;

    // ROLLBACK
    g.run("BEGIN;").await;
    g.run("CREATE INDEX idx_id ON IdxCreate (id);").await;
    g.test_idx(
        "SELECT id FROM IdxCreate WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(idx_id, Eq, "1"),
    )
    .await;
    g.run("ROLLBACK;").await;
    g.test_idx(
        "SELECT id FROM IdxCreate WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(),
    )
    .await;

    // COMMIT;
    g.run("BEGIN;").await;
    g.run("CREATE INDEX idx_id ON IdxCreate (id);").await;
    g.test_idx(
        "SELECT id FROM IdxCreate WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(idx_id, Eq, "1"),
    )
    .await;
    g.run("COMMIT;").await;
    g.test_idx(
        "SELECT id FROM IdxCreate WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(idx_id, Eq, "1"),
    )
    .await;

    g.run("DELETE FROM IdxCreate;").await;
    g.run("INSERT INTO IdxCreate VALUES (3);").await;

    // CREATE MORE
    g.run("BEGIN;").await;
    g.run("CREATE INDEX idx_id2 ON IdxCreate (id * 2);").await;
    g.test_idx(
        "SELECT id FROM IdxCreate WHERE id = 3",
        Ok(select!(id I64; 3)),
        idx!(idx_id, Eq, "3"),
    )
    .await;
    g.test_idx(
        "SELECT id FROM IdxCreate WHERE id * 2 = 6",
        Ok(select!(id I64; 3)),
        idx!(idx_id2, Eq, "6"),
    )
    .await;
    g.run("ROLLBACK;").await;

    g.test_idx(
        "SELECT id FROM IdxCreate WHERE id = 3",
        Ok(select!(id I64; 3)),
        idx!(idx_id, Eq, "3"),
    )
    .await;
    g.test_idx(
        "SELECT id FROM IdxCreate WHERE id * 2 = 6",
        Ok(select!(id I64; 3)),
        idx!(),
    )
    .await;
});

test_case!(index_drop, {
    let g = get_tester!();

    g.run("CREATE TABLE IdxDrop (id INTEGER);").await;
    g.run("INSERT INTO IdxDrop VALUES (1);").await;
    g.run("CREATE INDEX idx_id ON IdxDrop (id);").await;

    // ROLLBACK
    g.run("BEGIN;").await;
    g.run("DROP INDEX IdxDrop.idx_id;").await;
    g.test_idx(
        "SELECT id FROM IdxDrop WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(),
    )
    .await;
    g.run("ROLLBACK;").await;
    g.test_idx(
        "SELECT id FROM IdxDrop WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(idx_id, Eq, "1"),
    )
    .await;

    // COMMIT;
    g.run("BEGIN;").await;
    g.run("DROP INDEX IdxDrop.idx_id;").await;
    g.test_idx(
        "SELECT id FROM IdxDrop WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(),
    )
    .await;
    g.run("COMMIT;").await;
    g.test_idx(
        "SELECT id FROM IdxDrop WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(),
    )
    .await;
});
