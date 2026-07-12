use super::*;

test_case!(multi, {
    let g = get_tester!();

    g.run(
        "
CREATE TABLE Multi (
    id INTEGER,
    num INTEGER
)",
    );

    g.run(
        "
        INSERT INTO Multi VALUES
            (3, 50), (3, 10), (3, 40), (3, 30), (3, 20),
            (4, 10), (4, 30), (4, 20), (4, 40), (4, 50),
            (2, 20), (2, 10), (2, 30), (2, 40), (2, 50),
            (5, 40), (5, 50), (5, 10), (5, 20), (5, 30),
            (1, 30), (1, 40), (1, 20), (1, 50), (1, 10);
    ",
    );

    g.test(
        "CREATE INDEX idx_id_num ON Multi (id + num DESC)",
        Ok(Payload::CreateIndex),
    );

    g.test_idx(
        "SELECT * FROM Multi ORDER BY id ASC, num ASC",
        Ok(select!(id | num I64 | I64;
            1 10; 1 20; 1 30; 1 40; 1 50;
            2 10; 2 20; 2 30; 2 40; 2 50;
            3 10; 3 20; 3 30; 3 40; 3 50;
            4 10; 4 20; 4 30; 4 40; 4 50;
            5 10; 5 20; 5 30; 5 40; 5 50
        )),
        idx!(),
    );

    g.test(
        "CREATE INDEX idx_num ON Multi (num ASC)",
        Ok(Payload::CreateIndex),
    );

    g.test_idx(
        "SELECT * FROM Multi ORDER BY id ASC, num ASC",
        Ok(select!(id | num I64 | I64;
            1 10; 1 20; 1 30; 1 40; 1 50;
            2 10; 2 20; 2 30; 2 40; 2 50;
            3 10; 3 20; 3 30; 3 40; 3 50;
            4 10; 4 20; 4 30; 4 40; 4 50;
            5 10; 5 20; 5 30; 5 40; 5 50
        )),
        idx!(idx_num, ASC),
    );

    g.test_idx(
        "SELECT * FROM Multi ORDER BY num ASC, id ASC",
        Ok(select!(id | num I64 | I64;
            1 10; 2 10; 3 10; 4 10; 5 10;
            1 20; 2 20; 3 20; 4 20; 5 20;
            1 30; 2 30; 3 30; 4 30; 5 30;
            1 40; 2 40; 3 40; 4 40; 5 40;
            1 50; 2 50; 3 50; 4 50; 5 50
        )),
        idx!(),
    );

    g.test_idx(
        "SELECT * FROM Multi ORDER BY id DESC, id + num DESC",
        Ok(select!(id | num I64 | I64;
            5 50; 5 40; 5 30; 5 20; 5 10;
            4 50; 4 40; 4 30; 4 20; 4 10;
            3 50; 3 40; 3 30; 3 20; 3 10;
            2 50; 2 40; 2 30; 2 20; 2 10;
            1 50; 1 40; 1 30; 1 20; 1 10
        )),
        idx!(idx_id_num, DESC),
    );

    g.test_idx(
        "SELECT * FROM Multi ORDER BY id ASC, id + num DESC",
        Ok(select!(id | num I64 | I64;
            1 50; 1 40; 1 30; 1 20; 1 10;
            2 50; 2 40; 2 30; 2 20; 2 10;
            3 50; 3 40; 3 30; 3 20; 3 10;
            4 50; 4 40; 4 30; 4 20; 4 10;
            5 50; 5 40; 5 30; 5 20; 5 10
        )),
        idx!(idx_id_num, DESC),
    );
});
