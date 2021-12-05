#![cfg(all(feature = "transaction", feature = "index"))]

use {crate::*, ast::IndexOperator::*, prelude::Value::*};

test_case!(index_create, async move {
    run!("CREATE TABLE IdxCreate (id INTEGER);");
    run!("INSERT INTO IdxCreate VALUES (1);");

    // ROLLBACK
    run!("BEGIN;");
    run!("CREATE INDEX idx_id ON IdxCreate (id);");
    test_idx!(
        Ok(select!(id I64; 1)),
        idx!(idx_id, Eq, "1"),
        "SELECT id FROM IdxCreate WHERE id = 1"
    );
    run!("ROLLBACK;");
    test_idx!(
        Ok(select!(id I64; 1)),
        idx!(),
        "SELECT id FROM IdxCreate WHERE id = 1"
    );

    // COMMIT;
    run!("BEGIN;");
    run!("CREATE INDEX idx_id ON IdxCreate (id);");
    test_idx!(
        Ok(select!(id I64; 1)),
        idx!(idx_id, Eq, "1"),
        "SELECT id FROM IdxCreate WHERE id = 1"
    );
    run!("COMMIT;");
    test_idx!(
        Ok(select!(id I64; 1)),
        idx!(idx_id, Eq, "1"),
        "SELECT id FROM IdxCreate WHERE id = 1"
    );

    run!("DELETE FROM IdxCreate;");
    run!("INSERT INTO IdxCreate VALUES (3);");

    // CREATE MORE
    run!("BEGIN;");
    run!("CREATE INDEX idx_id2 ON IdxCreate (id * 2);");
    test_idx!(
        Ok(select!(id I64; 3)),
        idx!(idx_id, Eq, "3"),
        "SELECT id FROM IdxCreate WHERE id = 3"
    );
    test_idx!(
        Ok(select!(id I64; 3)),
        idx!(idx_id2, Eq, "6"),
        "SELECT id FROM IdxCreate WHERE id * 2 = 6"
    );
    run!("ROLLBACK;");

    test_idx!(
        Ok(select!(id I64; 3)),
        idx!(idx_id, Eq, "3"),
        "SELECT id FROM IdxCreate WHERE id = 3"
    );
    test_idx!(
        Ok(select!(id I64; 3)),
        idx!(),
        "SELECT id FROM IdxCreate WHERE id * 2 = 6"
    );
});

test_case!(index_drop, async move {
    run!("CREATE TABLE IdxDrop (id INTEGER);");
    run!("INSERT INTO IdxDrop VALUES (1);");
    run!("CREATE INDEX idx_id ON IdxDrop (id);");

    // ROLLBACK
    run!("BEGIN;");
    run!("DROP INDEX IdxDrop.idx_id;");
    test_idx!(
        Ok(select!(id I64; 1)),
        idx!(),
        "SELECT id FROM IdxDrop WHERE id = 1"
    );
    run!("ROLLBACK;");
    test_idx!(
        Ok(select!(id I64; 1)),
        idx!(idx_id, Eq, "1"),
        "SELECT id FROM IdxDrop WHERE id = 1"
    );

    // COMMIT;
    run!("BEGIN;");
    run!("DROP INDEX IdxDrop.idx_id;");
    test_idx!(
        Ok(select!(id I64; 1)),
        idx!(),
        "SELECT id FROM IdxDrop WHERE id = 1"
    );
    run!("COMMIT;");
    test_idx!(
        Ok(select!(id I64; 1)),
        idx!(),
        "SELECT id FROM IdxDrop WHERE id = 1"
    );
});
