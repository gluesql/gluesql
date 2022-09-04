use {crate::*, gluesql_core::prelude::*, Value::*};

test_case!(order_by, async move {
    run!(
        r#"
CREATE TABLE Test (
    id INTEGER,
    num INTEGER NULL,
    name TEXT,
)"#
    );
    run!(
        r#"
        INSERT INTO Test (id, num, name)
        VALUES
            (1, 2,    "Hello"),
            (1, 9,    "Wild"),
            (3, NULL, "World"),
            (4, 7,    "Monday");
    "#
    );

    test!(
        "CREATE INDEX idx_name ON Test (name)",
        Ok(Payload::CreateIndex)
    );
    test!(
        "CREATE INDEX idx_id_num_asc ON Test (id + num ASC)",
        Ok(Payload::CreateIndex)
    );
    test!(
        "CREATE INDEX idx_num_desc ON Test (num DESC)",
        Ok(Payload::CreateIndex)
    );

    macro_rules! s {
        ($v: literal) => {
            Str($v.to_owned())
        };
    }

    test_idx!(
        Ok(select_with_null!(
            id     | num    | name;
            I64(1)   I64(2)   s!("Hello");
            I64(4)   I64(7)   s!("Monday");
            I64(1)   I64(9)   s!("Wild");
            I64(3)   Null     s!("World")
        )),
        idx!(idx_name),
        "SELECT * FROM Test ORDER BY name"
    );

    test_idx!(
        Ok(select_with_null!(
            id     | num    | name;
            I64(1)   I64(2)   s!("Hello");
            I64(1)   I64(9)   s!("Wild");
            I64(4)   I64(7)   s!("Monday");
            I64(3)   Null     s!("World")
        )),
        idx!(idx_id_num_asc),
        "SELECT * FROM Test ORDER BY id + num"
    );

    test_idx!(
        Ok(select_with_null!(
            id     | num    | name;
            I64(1)   I64(2)   s!("Hello");
            I64(1)   I64(9)   s!("Wild");
            I64(4)   I64(7)   s!("Monday");
            I64(3)   Null     s!("World")
        )),
        idx!(idx_id_num_asc, ASC),
        "SELECT * FROM Test ORDER BY id + num ASC"
    );

    test_idx!(
        Ok(select_with_null!(
            id     | num    | name;
            I64(3)   Null     s!("World");
            I64(1)   I64(9)   s!("Wild");
            I64(1)   I64(2)   s!("Hello")
        )),
        idx!(idx_num_desc, DESC),
        "SELECT * FROM Test where id < 4 ORDER BY num DESC"
    );
});

test_case!(order_by_multi, async move {
    run!(
        r#"
CREATE TABLE Multi (
    id INTEGER,
    num INTEGER
)"#
    );

    run!(
        "
        INSERT INTO Multi VALUES
            (3, 50), (3, 10), (3, 40), (3, 30), (3, 20),
            (4, 10), (4, 30), (4, 20), (4, 40), (4, 50),
            (2, 20), (2, 10), (2, 30), (2, 40), (2, 50),
            (5, 40), (5, 50), (5, 10), (5, 20), (5, 30),
            (1, 30), (1, 40), (1, 20), (1, 50), (1, 10);
    "
    );

    test!(
        "CREATE INDEX idx_id_num ON Multi (id + num DESC)",
        Ok(Payload::CreateIndex)
    );

    test_idx!(
        Ok(select!(id | num I64 | I64;
            1 10; 1 20; 1 30; 1 40; 1 50;
            2 10; 2 20; 2 30; 2 40; 2 50;
            3 10; 3 20; 3 30; 3 40; 3 50;
            4 10; 4 20; 4 30; 4 40; 4 50;
            5 10; 5 20; 5 30; 5 40; 5 50
        )),
        idx!(),
        "SELECT * FROM Multi ORDER BY id ASC, num ASC"
    );

    test!(
        "CREATE INDEX idx_num ON Multi (num ASC)",
        Ok(Payload::CreateIndex)
    );

    test_idx!(
        Ok(select!(id | num I64 | I64;
            1 10; 1 20; 1 30; 1 40; 1 50;
            2 10; 2 20; 2 30; 2 40; 2 50;
            3 10; 3 20; 3 30; 3 40; 3 50;
            4 10; 4 20; 4 30; 4 40; 4 50;
            5 10; 5 20; 5 30; 5 40; 5 50
        )),
        idx!(idx_num, ASC),
        "SELECT * FROM Multi ORDER BY id ASC, num ASC"
    );

    test_idx!(
        Ok(select!(id | num I64 | I64;
            1 10; 2 10; 3 10; 4 10; 5 10;
            1 20; 2 20; 3 20; 4 20; 5 20;
            1 30; 2 30; 3 30; 4 30; 5 30;
            1 40; 2 40; 3 40; 4 40; 5 40;
            1 50; 2 50; 3 50; 4 50; 5 50
        )),
        idx!(),
        "SELECT * FROM Multi ORDER BY num ASC, id ASC"
    );

    test_idx!(
        Ok(select!(id | num I64 | I64;
            5 50; 5 40; 5 30; 5 20; 5 10;
            4 50; 4 40; 4 30; 4 20; 4 10;
            3 50; 3 40; 3 30; 3 20; 3 10;
            2 50; 2 40; 2 30; 2 20; 2 10;
            1 50; 1 40; 1 30; 1 20; 1 10
        )),
        idx!(idx_id_num, DESC),
        "SELECT * FROM Multi ORDER BY id DESC, id + num DESC"
    );

    test_idx!(
        Ok(select!(id | num I64 | I64;
            1 50; 1 40; 1 30; 1 20; 1 10;
            2 50; 2 40; 2 30; 2 20; 2 10;
            3 50; 3 40; 3 30; 3 20; 3 10;
            4 50; 4 40; 4 30; 4 20; 4 10;
            5 50; 5 40; 5 30; 5 20; 5 10
        )),
        idx!(idx_id_num, DESC),
        "SELECT * FROM Multi ORDER BY id ASC, id + num DESC"
    );
});
