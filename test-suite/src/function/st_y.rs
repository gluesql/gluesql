use gluesql_core::executor::EvaluateError;

use {
    crate::*,
    gluesql_core::{executor::Payload, prelude::Value::*},
};

test_case!(st_y, async move {
    let test_cases = [
        (
            r#"SELECT ST_Y(ST_GEOFROMTEXT('POINT(-71.064544 42.28787)')) AS ptx"#,
            Ok(select!(
                ptx
                F64;
                42.28787
            )),
        ),
        (
            r#"SELECT ST_Y('cheese') AS ptx"#,
            Err(EvaluateError::FunctionRequiresPointValue("ST_Y".to_owned()).into()),
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
            r#"SELECT ST_Y(point_field) AS point_field FROM POINT;"#,
            Ok(select!(
                point_field
                F64;
                0.156
            )),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
