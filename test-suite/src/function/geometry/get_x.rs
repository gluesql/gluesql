use {
    crate::*,
    gluesql_core::{
        error::EvaluateError,
        prelude::{Payload, Value::*},
    },
};

test_case!(get_x, {
    let g = get_tester!();

    let test_cases = [
        (
            "CREATE TABLE PointGroup (point_field POINT)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO PointGroup VALUES (POINT(0.3134, 0.156))"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT GET_X(point_field) AS point_field FROM PointGroup;"#,
            Ok(select!(
                point_field
                F64;
                0.3134
            )),
        ),
        (
            r#"SELECT GET_X(CAST('POINT(0.1 -0.2)' AS POINT)) AS ptx"#,
            Ok(select!(
                ptx
                F64;
                0.1

            )),
        ),
        (
            r#"SELECT GET_X(POINT(0.1, -0.2)) AS ptx"#,
            Ok(select!(
                ptx
                F64;
                0.1
            )),
        ),
        (
            r#"SELECT GET_X('cheese') AS ptx"#,
            Err(EvaluateError::FunctionRequiresPointValue("GET_X".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
