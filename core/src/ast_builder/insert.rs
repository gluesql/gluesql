// Insert {
        /// TABLE
//        table_name: ObjectName,
        /// COLUMNS
//       columnList: string, Vec<String>,
        /// A SQL query that specifies what to insert
//        source: Query,
//    },

use {
    super::ExprNode,
    crate::{
        ast::{Expr,},
        result::Result,
    },
};


#[derive(Clone)]
pub struct InsertNode {
    table_name: String,
    colums: Vec<String>,
    source: QueryNode,
}


// INSERT INTO Foo VALUES (1, "Hello"), (2, "World");
table("Foo")
    .insert()
    .values(vec![
        vec![1, "Hello"],
        vec![2, "World"],
    ])
    .build();

// INSERT INTO Foo (id, name) VALUES (1, "hello"), (2, "world");
table("Foo")
    .insert()
    .columns("id, name")
    .values(vec![
        vec![1, "Hello"],
        vec![2, "World"],
    ])
    .build();

// INSERT INTO Foo SELECT id, name FROM Bar LIMIT 10;
table("Foo")
    .insert()
    // .as_select("SELECT id, name FROM Bar LIMIT 10")
    .as_select(table("Bar").project("id, name").limit(10))
    .build();