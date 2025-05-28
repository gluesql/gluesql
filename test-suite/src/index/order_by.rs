use {crate::*, Value::*, gluesql_core::prelude::*};

test_case!(order_by, {
    let g = get_tester!();

    g.run(
        "
CREATE TABLE Test (
    id INTEGER,
    num INTEGER NULL,
    name TEXT
)",
    )
    .await;
    g.run(
        "
        INSERT INTO Test (id, num, name)
        VALUES
            (1, 2,    'Hello'),
            (1, 9,    'Wild'),
            (3, NULL, 'World'),
            (4, 7,    'Monday');
   ",
    )
    .await;

    g.test(
        "CREATE INDEX idx_name ON Test (name)",
        Ok(Payload::CreateIndex),
    )
    .await;
    g.test(
        "CREATE INDEX idx_id_num_asc ON Test (id + num ASC)",
        Ok(Payload::CreateIndex),
    )
    .await;
    g.test(
        "CREATE INDEX idx_num_desc ON Test (num DESC)",
        Ok(Payload::CreateIndex),
    )
    .await;

    macro_rules! s {
        ($v: literal) => {
            Str($v.to_owned())
        };
    }

    g.test_idx(
        "SELECT * FROM Test ORDER BY name",
        Ok(select_with_null!(
            id     | num    | name;
            I64(1)   I64(2)   s!("Hello");
            I64(4)   I64(7)   s!("Monday");
            I64(1)   I64(9)   s!("Wild");
            I64(3)   Null     s!("World")
        )),
        idx!(idx_name),
    )
    .await;

    g.test_idx(
        "SELECT * FROM Test ORDER BY id + num",
        Ok(select_with_null!(
            id     | num    | name;
            I64(1)   I64(2)   s!("Hello");
            I64(1)   I64(9)   s!("Wild");
            I64(4)   I64(7)   s!("Monday");
            I64(3)   Null     s!("World")
        )),
        idx!(idx_id_num_asc),
    )
    .await;

    g.test_idx(
        "SELECT * FROM Test ORDER BY id + num ASC",
        Ok(select_with_null!(
            id     | num    | name;
            I64(1)   I64(2)   s!("Hello");
            I64(1)   I64(9)   s!("Wild");
            I64(4)   I64(7)   s!("Monday");
            I64(3)   Null     s!("World")
        )),
        idx!(idx_id_num_asc, ASC),
    )
    .await;

    g.test_idx(
        "SELECT * FROM Test where id < 4 ORDER BY num DESC",
        Ok(select_with_null!(
            id     | num    | name;
            I64(3)   Null     s!("World");
            I64(1)   I64(9)   s!("Wild");
            I64(1)   I64(2)   s!("Hello")
        )),
        idx!(idx_num_desc, DESC),
    )
    .await;
});

test_case!(order_by_multi, {
    let g = get_tester!();

    g.run(
        "
CREATE TABLE Multi (
    id INTEGER,
    num INTEGER
)",
    )
    .await;

    g.run(
        "
        INSERT INTO Multi VALUES
            (3, 50), (3, 10), (3, 40), (3, 30), (3, 20),
            (4, 10), (4, 30), (4, 20), (4, 40), (4, 50),
            (2, 20), (2, 10), (2, 30), (2, 40), (2, 50),
            (5, 40), (5, 50), (5, 10), (5, 20), (5, 30),
            (1, 30), (1, 40), (1, 20), (1, 50), (1, 10);
    ",
    )
    .await;

    g.test(
        "CREATE INDEX idx_id_num ON Multi (id + num DESC)",
        Ok(Payload::CreateIndex),
    )
    .await;

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
    )
    .await;

    g.test(
        "CREATE INDEX idx_num ON Multi (num ASC)",
        Ok(Payload::CreateIndex),
    )
    .await;

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
    )
    .await;

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
    )
    .await;

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
    )
    .await;

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
    )
    .await;
});
