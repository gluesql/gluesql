use crate::*;

test_case!(showindexes, async move {
    use gluesql_core::{
       // ast::IndexOperator::*,
       // executor::AlterError,
        prelude::{Payload,},
        data::{SchemaIndex, SchemaIndexOrd,},
        ast::{Expr, BinaryOperator,},
       // store::IndexError,
       // translate::TranslateError,
    };

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

    test!(Ok(Payload::ShowIndexes(
        vec![
        SchemaIndex{name:"idx_id".to_string(), order:SchemaIndexOrd::Both, 
                 expr:Expr::Identifier("id".to_string())},
             SchemaIndex { name: "idx_name".to_string(), 
                 expr: Expr::Identifier("name".to_string()), order: SchemaIndexOrd::Both }, 
             SchemaIndex { name: "idx_id2".to_string(), order: SchemaIndexOrd::Both, 
                 expr: Expr::BinaryOp { left: Box::new(Expr::Identifier("id".to_string())), op: BinaryOperator::Plus, 
                                   right: Box::new(Expr::Identifier("num".to_string())) }},
             ])),
          "show indexes from Test");

});
