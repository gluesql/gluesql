use {
    crate::*,
    gluesql_core::{executor::EvaluateError, executor::Payload, prelude::Value::*},
};

test_case!(get_x, async move {
    let test_cases = [
        (
            "CREATE TABLE SingleItem (id FLOAT DEFAULT GET_X(POINT(0.3134, 0.156)))",
            Ok(Payload::Create),
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
        (
            "CREATE TABLE POINT (point_field POINT)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO POINT VALUES (POINT(0.3134, 0.156))"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT GET_X(point_field) AS point_field FROM POINT;"#,
            Ok(select!(
                point_field
                F64;
                0.3134
            )),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
