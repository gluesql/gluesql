use {crate::*, Value::*, gluesql_core::prelude::*};

pub mod multi;

test_case!(order_by, {
    let g = get_tester!();

    g.run(
        "
CREATE TABLE Test (
    id INTEGER,
    num INTEGER NULL,
    name TEXT
)",
    );
    g.run(
        "
        INSERT INTO Test (id, num, name)
        VALUES
            (1, 2,    'Hello'),
            (1, 9,    'Wild'),
            (3, NULL, 'World'),
            (4, 7,    'Monday');
   ",
    );

    g.test(
        "CREATE INDEX idx_name ON Test (name)",
        Ok(Payload::CreateIndex),
    );
    g.test(
        "CREATE INDEX idx_id_num_asc ON Test (id + num ASC)",
        Ok(Payload::CreateIndex),
    );
    g.test(
        "CREATE INDEX idx_num_desc ON Test (num DESC)",
        Ok(Payload::CreateIndex),
    );

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
    );

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
    );

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
    );

    g.test_idx(
        "SELECT * FROM Test where id < 4 ORDER BY num DESC",
        Ok(select_with_null!(
            id     | num    | name;
            I64(3)   Null     s!("World");
            I64(1)   I64(9)   s!("Wild");
            I64(1)   I64(2)   s!("Hello")
        )),
        idx!(idx_num_desc, DESC),
    );
});
