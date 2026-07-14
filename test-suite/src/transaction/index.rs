use {
    crate::*,
    gluesql_core::{ast::IndexOperator::*, prelude::Value::*},
};

test_case!(index_create, {
    let g = get_tester!();

    g.run("CREATE TABLE IdxCreate (id INTEGER);");
    g.run("INSERT INTO IdxCreate VALUES (1);");

    // ROLLBACK
    g.run("BEGIN;");
    g.run("CREATE INDEX idx_id ON IdxCreate (id);");
    g.test_idx(
        "SELECT id FROM IdxCreate WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(idx_id, Eq, "1"),
    );
    g.run("ROLLBACK;");
    g.test_idx(
        "SELECT id FROM IdxCreate WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(),
    );

    // COMMIT;
    g.run("BEGIN;");
    g.run("CREATE INDEX idx_id ON IdxCreate (id);");
    g.test_idx(
        "SELECT id FROM IdxCreate WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(idx_id, Eq, "1"),
    );
    g.run("COMMIT;");
    g.test_idx(
        "SELECT id FROM IdxCreate WHERE id = 1",
        Ok(select!(id I64; 1)),
        idx!(idx_id, Eq, "1"),
    );

    g.run("DELETE FROM IdxCreate;");
    g.run("INSERT INTO IdxCreate VALUES (3);");

    // CREATE MORE
    g.run("BEGIN;");
    g.run("CREATE INDEX idx_id2 ON IdxCreate (id * 2);");
    g.test_idx(
        "SELECT id FROM IdxCreate WHERE id = 3",
        Ok(select!(id I64; 3)),
        idx!(idx_id, Eq, "3"),
    );
    g.test_idx(
        "SELECT id FROM IdxCreate WHERE id * 2 = 6",
        Ok(select!(id I64; 3)),
        idx!(idx_id2, Eq, "6"),
    );
    g.run("ROLLBACK;");

    g.test_idx(
        "SELECT id FROM IdxCreate WHERE id = 3",
        Ok(select!(id I64; 3)),
        idx!(idx_id, Eq, "3"),
    );
    g.test_idx(
        "SELECT id FROM IdxCreate WHERE id * 2 = 6",
        Ok(select!(id I64; 3)),
        idx!(),
    );
});

test_case!(index_drop, {
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

test_case!(index_dml, {
    let g = get_tester!();

    g.run("CREATE TABLE IdxDml (id INTEGER, value INTEGER);");
    g.run("INSERT INTO IdxDml VALUES (1, 10), (2, 20);");
    g.run("CREATE INDEX idx_value ON IdxDml (value);");

    // ROLLBACK
    g.run("BEGIN;");
    g.run("INSERT INTO IdxDml VALUES (3, 30);");
    g.run("UPDATE IdxDml SET value = 11 WHERE id = 1;");
    g.run("DELETE FROM IdxDml WHERE id = 2;");
    g.test_idx(
        "SELECT id, value FROM IdxDml WHERE value = 10",
        Ok(select!(id | value)),
        idx!(idx_value, Eq, "10"),
    );
    g.test_idx(
        "SELECT id, value FROM IdxDml WHERE value = 11",
        Ok(select!(id | value I64 | I64; 1 11)),
        idx!(idx_value, Eq, "11"),
    );
    g.test_idx(
        "SELECT id, value FROM IdxDml WHERE value = 20",
        Ok(select!(id | value)),
        idx!(idx_value, Eq, "20"),
    );
    g.test_idx(
        "SELECT id, value FROM IdxDml WHERE value = 30",
        Ok(select!(id | value I64 | I64; 3 30)),
        idx!(idx_value, Eq, "30"),
    );
    g.run("ROLLBACK;");
    g.test_idx(
        "SELECT id, value FROM IdxDml WHERE value = 10",
        Ok(select!(id | value I64 | I64; 1 10)),
        idx!(idx_value, Eq, "10"),
    );
    g.test_idx(
        "SELECT id, value FROM IdxDml WHERE value = 20",
        Ok(select!(id | value I64 | I64; 2 20)),
        idx!(idx_value, Eq, "20"),
    );
    g.test_idx(
        "SELECT id, value FROM IdxDml WHERE value = 11",
        Ok(select!(id | value)),
        idx!(idx_value, Eq, "11"),
    );
    g.test_idx(
        "SELECT id, value FROM IdxDml WHERE value = 30",
        Ok(select!(id | value)),
        idx!(idx_value, Eq, "30"),
    );

    // COMMIT
    g.run("BEGIN;");
    g.run("INSERT INTO IdxDml VALUES (4, 40);");
    g.run("UPDATE IdxDml SET value = 12 WHERE id = 1;");
    g.run("DELETE FROM IdxDml WHERE id = 2;");
    g.run("COMMIT;");
    g.test_idx(
        "SELECT id, value FROM IdxDml WHERE value = 10",
        Ok(select!(id | value)),
        idx!(idx_value, Eq, "10"),
    );
    g.test_idx(
        "SELECT id, value FROM IdxDml WHERE value = 12",
        Ok(select!(id | value I64 | I64; 1 12)),
        idx!(idx_value, Eq, "12"),
    );
    g.test_idx(
        "SELECT id, value FROM IdxDml WHERE value = 20",
        Ok(select!(id | value)),
        idx!(idx_value, Eq, "20"),
    );
    g.test_idx(
        "SELECT id, value FROM IdxDml WHERE value = 40",
        Ok(select!(id | value I64 | I64; 4 40)),
        idx!(idx_value, Eq, "40"),
    );
});
