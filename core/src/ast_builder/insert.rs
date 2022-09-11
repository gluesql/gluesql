// Insert {
/// TABLE
//        table_name: ObjectName,
/// COLUMNS
//       columnList: string, Vec<String>,
/// A SQL query that specifies what to insert
//        source: Query,
//    },
pub use {
    super::{ColumnList, ExprList, QueryNode},
    crate::{
        ast::{Expr, Statement},
        result::Result,
    },
};

#[derive(Clone)]
pub struct InsertNode {
    table_name: String,
    columns: Option<ColumnList>,
}

impl InsertNode {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            columns: None,
        }
    }

    pub fn columns<T: Into<ColumnList>>(mut self, columns: T) -> Self {
        self.columns = Some(columns.into());
        self
    }

    pub fn values<T: Into<ExprList>>(mut self, values: Vec<T>) -> InsertSourceNode {
        let values: Vec<ExprList> = values.into_iter().map(Into::into).collect();

        InsertSourceNode {
            insert_node: self,
            source: QueryNode::Values(values),
        }
    }

    pub fn as_select<T: Into<QueryNode>>(mut self, query: T) -> InsertSourceNode {
        InsertSourceNode {
            insert_node: self,
            source: query.into(),
        }
    }
}

#[derive(Clone)]
pub struct InsertSourceNode {
    insert_node: InsertNode,
    source: QueryNode,
}

impl InsertSourceNode {
    pub fn build(self) -> Result<Statement> {
        panic!();
    }
}

/*
#[cfg(test)]
mod tests{
    use crate::ast_builder::{table, test};

    #[test]
    fn insert(){
        let actual = table("Foo")
            .insert()
            .values(vec![
                "'hello', 'world'",
                "NULL, NULL",
            ])
            .build();

        let actual = table("Foo")
                    .insert()
                    .values(vec![
                        vec![1, 5],
                        vec![2, 3],
                    ])
                    .build();
        let expected = r#"INSERT INTO Foo VALUES (1, 5), (2, 3)"#;
        test(actual, expected);


        let actual = table("Foo")
                    .insert()
                    .columns("id, name")
                    .values(vec![
                        vec![1, 5],
                        vec![2, 3],
                    ])
                    .build();
        let expected = r#"INSERT INTO Foo (id, name) VALUES (1, 5), (2, 3))"#;
        test(actual, expected);

        let actual = table!("Foo")
                    .insert()
                    .as_select(table("Bar").project("id, name").limit(10))
                    .build();
        let expected = r#"INSERT INTO Foo SELECT id, name FROM Bar LIMIT 10"#;
        test(actual, expected);
    }
}
*/
