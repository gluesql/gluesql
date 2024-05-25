use {
    crate::*,
    gluesql_core::{
        ast::DataType,
        prelude::Value::{self, *},
    },
};

test_case!(typed_array, {
    let g = get_tester!();

    g.run(
        "
CREATE TABLE ArrayType (
    id INTEGER,
    items INT[]
)",
    )
    .await;

    g.run(
        r#"
INSERT INTO ArrayType VALUES
    (1, '{1, 2, 3}'),
    (2, '{4, 5, 6, 7}'),
    (3, '{8, 9, 10}');
"#,
    )
    .await;

    let l = |s: &str| Value::parse_typed_array(&DataType::Int, None, s).unwrap();

    g.test(
        "SELECT id, items FROM ArrayType",
        Ok(select_with_null!(
            id     | items;
            I64(1)   l("{1,2,3}");
            I64(2)   l("{4,5,6,7}");
            I64(3)   l("{8,9,10}")
        )),
    )
    .await;

    g.test(
        "SELECT id, items[1] AS second FROM ArrayType",
        Ok(select_with_null!(
            id     | second;
            I64(1)   I64(2);
            I64(2)   I64(5);
            I64(3)   I64(9)
        )),
    )
    .await;

    g.named_test(
        "select index expr without alias",
        "SELECT id, items[1] FROM ArrayType",
        Ok(select_with_null!(
            id     | "items[1]";
            I64(1)   I64(2);
            I64(2)   I64(5);
            I64(3)   I64(9)
        )),
    )
    .await;
});
