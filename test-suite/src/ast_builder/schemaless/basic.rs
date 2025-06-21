use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload},
    gluesql_core::prelude::Value::{self, *},
    serde_json::json,
};

test_case!(basic, {
    let glue = get_glue!();

    let actual = table("Logs")
        .create_table()
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create schemaless table");

    let row1 = json!({ "id": 1, "value": 30 }).to_string();
    let row2 = json!({ "id": 2, "rate": 3.0, "list": [1, 2, 3] }).to_string();

    let actual = table("Logs")
        .insert()
        .values(vec![
            vec![text(&row1)],
            vec![text(&row2)],
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(2));
    assert_eq!(actual, expected, "insert schemaless data");

    let actual = table("Logs")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select_map![
        json!({ "id": 1, "value": 30 }),
        json!({ "id": 2, "rate": 3.0, "list": [1, 2, 3] })
    ]);
    assert_eq!(actual, expected, "select schemaless data");

    let actual = table("Logs")
        .delete()
        .filter("id = 1")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Delete(1));
    assert_eq!(actual, expected, "delete schemaless row");

    let actual = table("Logs")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select_map![
        json!({ "id": 2, "rate": 3.0, "list": [1, 2, 3] })
    ]);
    assert_eq!(actual, expected, "select after delete");

    let actual = table("Logs")
        .drop_table()
        .execute(glue)
        .await;
    let expected = Ok(Payload::DropTable(1));
    assert_eq!(actual, expected, "drop schemaless table");
});
