use {crate::*, gluesql_core::ast::DataType};

test_case!(type_match, async move {
    let g = get_tester!();

    g.run("CREATE TABLE TypeMatch (uuid_value UUID, float_value FLOAT, int_value INT, bool_value BOOLEAN)").await.unwrap();
    g.run("INSERT INTO TypeMatch values(GENERATE_UUID(), 1.0, 1, true)")
        .await
        .unwrap();
    g.type_match(
        "SELECT * FROM TypeMatch",
        &[
            DataType::Uuid,
            DataType::Float,
            DataType::Int,
            DataType::Boolean,
        ],
    )
    .await;
});
