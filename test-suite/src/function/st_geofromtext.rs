use {crate::*, gluesql_core::prelude::Value::Point};

test_case!(st_geofromtext, async move {
    let test_cases = [(
        r#"SELECT ST_GEOFROMTEXT('POINT(-71.064544 42.28787)') AS pt"#,
        Ok(select!(
            pt
            Point;
            gluesql_core::data::Point::new(-71.064544, 42.28787)

        )),
    )];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
