use {
    crate::*,
    gluesql_core::{
        error::ExecuteError,
        prelude::{Payload, Value::*},
    },
};

test_case!(showindexes, {
    let g = get_tester!();

    g.run(
        "
CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT
)",
    )
    .await;

    g.run(
        "
        INSERT INTO Test
            (id, num, name)
        VALUES
            (1, 2, 'Hello'),
            (1, 17, 'World'),
            (11, 7, 'Great'),
            (4, 7, 'Job');
    ",
    )
    .await;

    g.test("CREATE INDEX idx_id ON Test (id)", Ok(Payload::CreateIndex))
        .await;
    g.test(
        "CREATE INDEX idx_name ON Test (name)",
        Ok(Payload::CreateIndex),
    )
    .await;
    g.test(
        "CREATE INDEX idx_id2 ON Test (id + num)",
        Ok(Payload::CreateIndex),
    )
    .await;
    g.test(
        "show indexes from Test",
        Ok(select!(
            TABLE_NAME        | INDEX_NAME            | ORDER             | EXPRESSION            | UNIQUENESS;
            Str               | Str                   | Str               | Str                   | Bool;
            "Test".to_owned()   "idx_id".to_owned()     "BOTH".to_owned()   "id".to_owned()         false;
            "Test".to_owned()   "idx_name".to_owned()   "BOTH".to_owned()   "name".to_owned()       false;
            "Test".to_owned()   "idx_id2".to_owned()    "BOTH".to_owned()   "id + num".to_owned()   false
        ))
    ).await;
    g.test(
        "show indexes from NoTable",
        Err(ExecuteError::TableNotFound("NoTable".to_owned()).into()),
    )
    .await;
});
