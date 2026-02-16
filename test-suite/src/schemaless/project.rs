use {
    crate::*,
    gluesql_core::{data::Value, error::PlanError, executor::Payload, prelude::Value::I64},
    serde_json::json,
};

test_case!(project, {
    let g = get_tester!();

    g.run("CREATE TABLE A").await;
    g.run("CREATE TABLE B").await;
    g.run("CREATE TABLE S (a_id INTEGER, tag TEXT);").await;

    g.run(r#"INSERT INTO A VALUES ('{"a_id":1,"a":"left"}');"#)
        .await;
    g.run(r#"INSERT INTO B VALUES ('{"b_id":10,"a_id":1,"b":"right"}');"#)
        .await;
    g.run("INSERT INTO S VALUES (1, 'schema');").await;

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
        "schemaless wildcard projection with root alias",
        "SELECT * FROM A AS P",
        Ok(select_map![json!({"a_id": 1, "a": "left"})]),
    )
    .await;

    g.named_test(
        "schemaless qualified wildcard projection with root alias",
        "SELECT P.* FROM A AS P",
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
        "schemaless qualified wildcard keeps tabular output in join",
        "SELECT B.* FROM A JOIN B WHERE A.a_id = B.a_id",
        Ok(Payload::Select {
            labels: vec!["_doc".to_owned()],
            rows: vec![vec![
                Value::parse_json_map(r#"{"b_id":10,"a_id":1,"b":"right"}"#).unwrap(),
            ]],
        }),
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

    g.named_test(
        "wildcard join with schemaless root and schemaful join is rejected",
        "SELECT * FROM A JOIN S WHERE A.a_id = S.a_id",
        Err(PlanError::SchemalessMixedJoinWildcardProjection.into()),
    )
    .await;

    g.named_test(
        "wildcard join with schemaful root and schemaless join is rejected",
        "SELECT * FROM S JOIN A WHERE S.a_id = A.a_id",
        Err(PlanError::SchemalessMixedJoinWildcardProjection.into()),
    )
    .await;

    {
        g.run("CREATE TABLE C (_doc INTEGER);").await;
        g.run("INSERT INTO C VALUES (7);").await;

        g.named_test(
            "schemaful _doc column stays tabular",
            "SELECT _doc FROM C",
            Ok(select!(
                _doc
                I64;
                7
            )),
        )
        .await;
    }
});
