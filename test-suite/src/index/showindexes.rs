use {
    crate::*,
    gluesql_core::{executor::ExecuteError, prelude::Payload, prelude::Value::*},
};

test_case!(showindexes, async move {
    run!(
        "
CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT
)"
    );

    run!(
        "
        INSERT INTO Test
            (id, num, name)
        VALUES
            (1, 2, 'Hello'),
            (1, 17, 'World'),
            (11, 7, 'Great'),
            (4, 7, 'Job');
    "
    );

    test!("CREATE INDEX idx_id ON Test (id)", Ok(Payload::CreateIndex));
    test!(
        "CREATE INDEX idx_name ON Test (name)",
        Ok(Payload::CreateIndex)
    );
    test!(
        "CREATE INDEX idx_id2 ON Test (id + num)",
        Ok(Payload::CreateIndex)
    );
    test!(
        "show indexes from Test",
        Ok(select!(
            TABLE_NAME        | INDEX_NAME            | ORDER             | EXPRESSION            | UNIQUENESS;
            Str               | Str                   | Str               | Str                   | Bool;
            "Test".to_owned()   "idx_id".to_owned()     "BOTH".to_owned()   "id".to_owned()         false;
            "Test".to_owned()   "idx_name".to_owned()   "BOTH".to_owned()   "name".to_owned()       false;
            "Test".to_owned()   "idx_id2".to_owned()    "BOTH".to_owned()   "id + num".to_owned()   false
        ))
    );
    test!(
        "show indexes from NoTable",
        Err(ExecuteError::TableNotFound("NoTable".to_owned()).into())
    );
});
