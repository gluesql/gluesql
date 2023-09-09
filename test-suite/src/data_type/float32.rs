use {
    crate::*,
    gluesql_core::prelude::{Payload, Value::*},
};

test_case!(float32, {
    let g = get_tester!();

    let test_cases = [
        (
            "CREATE TABLE line (x FLOAT32, y FLOAT32)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO line VALUES (0.3134, 0.156)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT x, y FROM line;"#,
            Ok(select!(
                x          |  y
                F32        |  F32;
                0.3134_f32    0.156_f32
            )),
        ),
        (
            r#"UPDATE line SET x=2.0, y=1.0 WHERE x=0.3134 AND y=0.156"#,
            Ok(Payload::Update(1)),
        ),
        (
            r#"SELECT x, y FROM line"#,
            Ok(select!(
                x       |   y
                F32     |   F32;
                2.0_f32     1.0_f32
            )),
        ),
        (
            r#"DELETE FROM line WHERE x=2.0 AND y=1.0"#,
            Ok(Payload::Delete(1)),
        ),
        (
            r#"SELECT CAST('-71.064544' AS FLOAT32) AS float32"#,
            Ok(select!(
                float32
                F32;
                -71.064544_f32

            )),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
