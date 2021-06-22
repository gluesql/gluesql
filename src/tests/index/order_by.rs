use crate::*;

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
        Ok(Payload::CreateIndex),
        "CREATE INDEX idx_name ON Test (name)"
    );
    test!(
        Ok(Payload::CreateIndex),
        "CREATE INDEX idx_id_num_asc ON Test (id + num ASC)"
    );
    test!(
        Ok(Payload::CreateIndex),
        "CREATE INDEX idx_num_desc ON Test (num DESC)"
    );

    use Value::*;

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
