use crate::*;

test_case!(showindexes, async move {
    use gluesql_core::{data::SchemaIndexOrd, executor::ExecuteError, prelude::Payload};

    run!(
        r#"
CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT
)"#
    );

    run!(
        r#"
        INSERT INTO Test
            (id, num, name)
        VALUES
            (1, 2, "Hello"),
            (1, 17, "World"),
            (11, 7, "Great"),
            (4, 7, "Job");
    "#
    );

    test!(Ok(Payload::CreateIndex), "CREATE INDEX idx_id ON Test (id)");
    test!(
        Ok(Payload::CreateIndex),
        "CREATE INDEX idx_name ON Test (name)"
    );
    test!(
        Ok(Payload::CreateIndex),
        "CREATE INDEX idx_id2 ON Test (id + num)"
    );

    test!(
        Ok(Payload::ShowIndexes(vec![
            ("idx_id".to_string(), SchemaIndexOrd::Both),
            ("idx_name".to_string(), SchemaIndexOrd::Both),
            ("idx_id2".to_string(), SchemaIndexOrd::Both)
        ])),
        "show indexes from Test"
    );

    test!(
        Err(ExecuteError::TableNotFound("NoTable".to_string()).into()),
        "show indexes from NoTable"
    );
});
