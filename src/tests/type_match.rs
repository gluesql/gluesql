use crate::{ast::DataType, *};

test_case!(type_match, async move {
    run!("CREATE TABLE TypeMatch (uuid_value UUID, float_value FLOAT, int_value INT, bool_value BOOLEAN)");
    run!("INSERT INTO TypeMatch values(GENERATE_UUID(), 1.0, 1, true)");
    type_match!(
        &[
            DataType::Uuid,
            DataType::Float,
            DataType::Int,
            DataType::Boolean
        ],
        "SELECT * FROM TypeMatch"
    );
});
