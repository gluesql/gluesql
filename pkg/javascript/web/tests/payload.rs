#![cfg(target_arch = "wasm32")]

wasm_bindgen_test_configure!(run_in_browser);

use {
    gloo_utils::format::JsValueSerdeExt,
    gluesql_js::Glue,
    serde_json::{json, Value as Json},
    wasm_bindgen_futures::JsFuture,
    wasm_bindgen_test::*,
};

#[wasm_bindgen_test]
async fn queries() {
    let mut glue = Glue::new();

    let test_cases = [
        (
            "CREATE TABLE Foo (id INTEGER)",
            json!([{ "type": "CREATE TABLE" }]),
        ),
        (
            "INSERT INTO Foo VALUES (1), (2), (3)",
            json!([{
                "type": "INSERT",
                "affected": 3
            }]),
        ),
        (
            "SELECT * FROM Foo",
            json!([{
                "type": "SELECT",
                "rows": [
                    { "id": 1 },
                    { "id": 2 },
                    { "id": 3 }
                ]
            }]),
        ),
        (
            "UPDATE Foo SET id = id + 2 WHERE id = 3",
            json!([{
                "type": "UPDATE",
                "affected": 1
            }]),
        ),
        (
            "DELETE FROM Foo WHERE id < 5",
            json!([{
                "type": "DELETE",
                "affected": 2
            }]),
        ),
        (
            "SELECT * FROM Foo",
            json!([{
                "type": "SELECT",
                "rows": [{ "id": 5 }]
            }]),
        ),
        (
            "SHOW COLUMNS FROM Foo",
            json!([{
                "type": "SHOW COLUMNS",
                "columns": [{
                    "name": "id",
                    "type": "INT"
                }]
            }]),
        ),
        (
            "SHOW TABLES",
            json!([{
                "type": "SHOW TABLES",
                "tables": ["Foo"]
            }]),
        ),
        (
            "SHOW VERSION",
            json!([{
                "type": "SHOW VERSION",
                "version": env!("CARGO_PKG_VERSION"),
            }]),
        ),
        (
            "DROP TABLE IF EXISTS Foo",
            json!([{ "type": "DROP TABLE" }]),
        ),
    ];

    for (sql, expected) in test_cases {
        let actual: Json = JsFuture::from(glue.query(sql.to_owned()))
            .await
            .unwrap()
            .into_serde()
            .unwrap();

        assert_eq!(actual, expected, "{sql}");
    }
}
