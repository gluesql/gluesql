use {
    crate::*,
    gluesql_core::{executor::ExecuteError, prelude::Payload},
};

test_case!(showindexes, async move {
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

    test!("CREATE INDEX idx_id ON Test (id)", Ok(Payload::CreateIndex));
    test!(
        "CREATE INDEX idx_name ON Test (name)",
        Ok(Payload::CreateIndex)
    );
    test!(
        "CREATE INDEX idx_id2 ON Test (id + num)",
        Ok(Payload::CreateIndex)
    );

    // test!(
    //     "show indexes from Test",
    //     Ok(Payload::ShowIndexes(vec![
    //         SchemaIndex {
    //             name: "idx_id".to_owned(),
    //             order: SchemaIndexOrd::Both,
    //             expr: Expr::Identifier("id".to_owned()),
    //             created: Utc::now().naive_utc(),
    //         },
    //         SchemaIndex {
    //             name: "idx_name".to_owned(),
    //             order: SchemaIndexOrd::Both,
    //             expr: Expr::Identifier("name".to_owned()),
    //             created: Utc::now().naive_utc(),
    //         },
    //         SchemaIndex {
    //             name: "idx_id2".to_owned(),
    //             order: SchemaIndexOrd::Both,
    //             expr: Expr::BinaryOp {
    //                 left: Box::new(Expr::Identifier("id".to_owned())),
    //                 op: BinaryOperator::Plus,
    //                 right: Box::new(Expr::Identifier("num".to_owned()))
    //             },
    //             created: Utc::now().naive_utc(),
    //         }
    //     ]))
    // );

    test!(
        "show indexes from NoTable",
        Err(ExecuteError::TableNotFound("NoTable".to_owned()).into())
    );
});
