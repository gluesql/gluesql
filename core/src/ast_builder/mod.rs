mod delete;
mod expr;
mod expr_list;
mod select;

pub use {
    delete::DeleteNode,
    expr::ExprNode,
    expr_list::ExprList,
    select::{
        GroupByNode, HavingNode, LimitNode, LimitOffsetNode, OffsetLimitNode, OffsetNode,
        SelectNode,
    },
};

pub struct Builder;

impl Builder {
    pub fn table(table_name: &str) -> TableNode {
        let table_name = table_name.to_owned();

        TableNode { table_name }
    }
}

pub struct TableNode {
    table_name: String,
}

impl TableNode {
    pub fn select(self) -> SelectNode {
        SelectNode::new(self.table_name)
    }

    pub fn delete(self) -> DeleteNode {
        DeleteNode::new(self.table_name)
    }
}

/*

let builder=  Builder:;new();

builder.selectxj

let glue = Glue::new();;


glue.select().where()...

glue.update().


glue.select().from("Foo")

glue.table("Foo").insert(..);
let query = glue
    .table("Foo")
    .select()
    .col("id")
    .col("name")
    .col(["col3", "col4"])
    .filter("id = 10")
    .filter(["id2", BinaryOp::Gt, 10])
    .filter([
       "id",
       BinaryOp::Eq,
       glue.table("Bar").select(["rate"]).limit(1)
    ])
    .offset(10)
    .limit(10);

let query = glue
    .table("Foo")
    .select()
    .join("Bar")
    .on("Bar.foo_id = Foo.id")
    .filter("name > 100");

/*
 * SELECT * FROM Foo
 * JOIN Bar ON Bar.foo_id = Foo.id
 * WHERE name > 100
 */

let query = builder.table("Foo").select().to_sql();

let insert = glue
    .table("Foo")
    .insert()
    .col("id")
    .col(["name", "wow"])
    .col(vec!["bar"])
    .values([1, "abc", true, 2])
    .values([2, "hello", false, 2])
    .values(vec![
        [Value::I64(3), "hello", false, 2],
        [5, "world", true, 3],
    ])
    .values(r#"(1, "hello", False, 3)"#);


query.execute().await // SELECT * FROM Foo;


let update = glue
    .table("Foo")
    .update()
    .set("id = 1")
    .set("col1", 10)
    .filter("id > 30");


select()
   update()
    delete('asf').where(
        */
