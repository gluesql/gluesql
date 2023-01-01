pub use {
    super::{Build, ColumnList, ExprList, QueryNode, SelectNode},
    crate::{
        ast::{Expr, Statement},
        result::Result,
    },
};

#[derive(Clone, Debug)]
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

    pub fn values<'a, T: Into<ExprList<'a>>>(self, values: Vec<T>) -> InsertSourceNode<'a> {
        let values: Vec<ExprList> = values.into_iter().map(Into::into).collect();

        InsertSourceNode {
            insert_node: self,
            source: QueryNode::Values(values),
        }
    }

    pub fn as_select<'a, T: Into<QueryNode<'a>>>(self, query: T) -> InsertSourceNode<'a> {
        InsertSourceNode {
            insert_node: self,
            source: query.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct InsertSourceNode<'a> {
    insert_node: InsertNode,
    source: QueryNode<'a>,
}

impl<'a> Build for InsertSourceNode<'a> {
    fn build(self) -> Result<Statement> {
        let table_name = self.insert_node.table_name;
        let columns = self.insert_node.columns;
        let columns = columns.map_or_else(|| Ok(vec![]), |v| v.try_into())?;
        let source = self.source.try_into()?;

        Ok(Statement::Insert {
            table_name,
            columns,
            source,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{num, table, test, Build};

    #[test]
    fn insert() {
        let actual = table("Foo").insert().values(vec!["1, 5", "2, 3"]).build();
        let expected = r#"INSERT INTO Foo VALUES (1, 5), (2, 3)"#;
        test(actual, expected);

        let actual = table("Foo")
            .insert()
            .columns("id, name")
            .values(vec![vec![num(1), num(5)], vec![num(2), num(3)]])
            .build();
        let expected = r#"INSERT INTO Foo (id, name) VALUES (1, 5), (2, 3)"#;
        test(actual, expected);

        let actual = table("Foo")
            .insert()
            .columns(vec!["hi"])
            .values(vec![vec![num(7)]])
            .build();
        let expected = r#"INSERT INTO Foo (hi) VALUES (7)"#;
        test(actual, expected);

        let actual = table("Foo")
            .insert()
            .as_select(table("Bar").select().project("id, name").limit(10))
            .build();
        let expected = r#"INSERT INTO Foo SELECT id, name FROM Bar LIMIT 10"#;
        test(actual, expected);
    }
}
