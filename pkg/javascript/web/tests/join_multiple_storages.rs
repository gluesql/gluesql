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
async fn join_multiple_storages() {
    let mut glue = Glue::new();
    let loaded = glue.load_indexeddb();
    JsFuture::from(loaded).await.unwrap();

    let mut test = |sql: &'static str, expected| {
        let result = glue.query(sql.to_owned());

        async move {
            let actual: Json = JsFuture::from(result).await.unwrap().into_serde().unwrap();

            assert_eq!(actual, expected, "{sql}");
        }
    };

    test(
        "
        CREATE TABLE Mem (mid INTEGER) ENGINE = memory;
        CREATE TABLE Loc (lid INTEGER) ENGINE = localStorage;
        CREATE TABLE Ses (sid INTEGER) ENGINE = sessionStorage;
        CREATE TABLE Idb (iid INTEGER) ENGINE = indexedDB;
        ",
        json!([
              { "type": "CREATE TABLE" },
              { "type": "CREATE TABLE" },
              { "type": "CREATE TABLE" },
              { "type": "CREATE TABLE" }
        ]),
    )
    .await;

    test(
        "
        INSERT INTO Mem VALUES (10), (11);
        INSERT INTO Loc VALUES (20), (21);
        INSERT INTO Ses VALUES (30), (31);
        INSERT INTO Idb VALUES (40), (41);
        ",
        json!([
              { "type": "INSERT", "affected": 2 },
              { "type": "INSERT", "affected": 2 },
              { "type": "INSERT", "affected": 2 },
              { "type": "INSERT", "affected": 2 }
        ]),
    )
    .await;

    test(
        "
        SELECT mid, lid, sid, iid 
        FROM Mem
        JOIN Loc
        JOIN Ses
        JOIN Idb;
        ",
        json!([{
            "type": "SELECT",
            "rows": [
                { "mid": 10, "lid": 20, "sid": 30, "iid": 40 },
                { "mid": 10, "lid": 20, "sid": 30, "iid": 41 },
                { "mid": 10, "lid": 20, "sid": 31, "iid": 40 },
                { "mid": 10, "lid": 20, "sid": 31, "iid": 41 },
                { "mid": 10, "lid": 21, "sid": 30, "iid": 40 },
                { "mid": 10, "lid": 21, "sid": 30, "iid": 41 },
                { "mid": 10, "lid": 21, "sid": 31, "iid": 40 },
                { "mid": 10, "lid": 21, "sid": 31, "iid": 41 },
                { "mid": 11, "lid": 20, "sid": 30, "iid": 40 },
                { "mid": 11, "lid": 20, "sid": 30, "iid": 41 },
                { "mid": 11, "lid": 20, "sid": 31, "iid": 40 },
                { "mid": 11, "lid": 20, "sid": 31, "iid": 41 },
                { "mid": 11, "lid": 21, "sid": 30, "iid": 40 },
                { "mid": 11, "lid": 21, "sid": 30, "iid": 41 },
                { "mid": 11, "lid": 21, "sid": 31, "iid": 40 },
                { "mid": 11, "lid": 21, "sid": 31, "iid": 41 }
            ]
        }]),
    )
    .await;
}
