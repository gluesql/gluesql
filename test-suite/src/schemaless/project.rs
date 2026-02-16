use {
    crate::*,
    gluesql_core::{data::Value, executor::Payload, prelude::Value::I64},
    serde_json::json,
};

test_case!(project, {
    let g = get_tester!();

    g.run("CREATE TABLE A").await;
    g.run("CREATE TABLE B").await;

    g.run(r#"INSERT INTO A VALUES ('{"a_id":1,"a":"left"}');"#)
        .await;
    g.run(r#"INSERT INTO B VALUES ('{"b_id":10,"a_id":1,"b":"right"}');"#)
        .await;

    g.named_test(
        "schemaless wildcard projection on single table",
        "SELECT * FROM A",
        Ok(select_map![json!({"a_id": 1, "a": "left"})]),
    )
    .await;

    g.named_test(
        "schemaless qualified wildcard projection on single table",
        "SELECT A.* FROM A",
        Ok(select_map![json!({"a_id": 1, "a": "left"})]),
    )
    .await;

    g.named_test(
        "schemaless projection by explicit fields in join",
        "SELECT A.a_id AS a_id, B.b_id AS b_id FROM A JOIN B WHERE A.a_id = B.a_id",
        Ok(select!(
            a_id | b_id
            I64  | I64;
            1      10
        )),
    )
    .await;

    g.named_test(
        "schemaless qualified wildcard picks right side map in join",
        "SELECT B.* FROM A JOIN B WHERE A.a_id = B.a_id",
        Ok(select_map![json!({"b_id": 10, "a_id": 1, "b": "right"})]),
    )
    .await;

    g.named_test(
        "schemaless qualified wildcard keeps table side in join",
        "SELECT A.*, B.* FROM A JOIN B WHERE A.a_id = B.a_id",
        Ok(Payload::Select {
            labels: vec!["_doc".to_owned(), "_doc".to_owned()],
            rows: vec![vec![
                Value::parse_json_map(r#"{"a_id":1,"a":"left"}"#).unwrap(),
                Value::parse_json_map(r#"{"b_id":10,"a_id":1,"b":"right"}"#).unwrap(),
            ]],
        }),
    )
    .await;
});
