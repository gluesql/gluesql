use {
    super::{AssignmentNode, Build, ExprNode},
    crate::{
        ast::{Assignment, Expr, Statement},
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub struct UpdateNode {
    table_name: String,
}

impl UpdateNode {
    pub fn new(table_name: String) -> Self {
        Self { table_name }
    }

    pub fn filter<'a, T: Into<ExprNode<'a>>>(self, expr: T) -> UpdateFilterNode<'a> {
        UpdateFilterNode::new(self.table_name, expr)
    }

    pub fn set<'a, T: Into<ExprNode<'a>>>(self, id: &str, value: T) -> UpdateSetNode<'a> {
        UpdateSetNode::new(self.table_name, None, id, value)
    }
}

#[derive(Clone, Debug)]
pub struct UpdateFilterNode<'a> {
    table_name: String,
    selection: ExprNode<'a>,
}

impl<'a> UpdateFilterNode<'a> {
    pub fn new<T: Into<ExprNode<'a>>>(table_name: String, expr: T) -> Self {
        Self {
            table_name,
            selection: expr.into(),
        }
    }

    pub fn filter<T: Into<ExprNode<'a>>>(mut self, expr: T) -> Self {
        self.selection = self.selection.and(expr.into());
        self
    }

    pub fn set<T: Into<ExprNode<'a>>>(self, id: &str, value: T) -> UpdateSetNode<'a> {
        UpdateSetNode::new(self.table_name, Some(self.selection), id, value)
    }
}

#[derive(Clone, Debug)]
pub struct UpdateSetNode<'a> {
    table_name: String,
    selection: Option<ExprNode<'a>>,
    assignments: Vec<AssignmentNode<'a>>,
}

impl<'a> UpdateSetNode<'a> {
    pub fn new<T: Into<ExprNode<'a>>>(
        table_name: String,
        selection: Option<ExprNode<'a>>,
        id: &str,
        value: T,
    ) -> Self {
        let assignments = vec![AssignmentNode::Expr(id.to_owned(), value.into())];

        Self {
            table_name,
            selection,
            assignments,
        }
    }

    pub fn set<T: Into<ExprNode<'a>>>(mut self, id: &str, value: T) -> Self {
        self.assignments
            .push(AssignmentNode::Expr(id.to_owned(), value.into()));
        self
    }
}

impl<'a> Build for UpdateSetNode<'a> {
    fn build(self) -> Result<Statement> {
        let table_name = self.table_name;
        let selection = self.selection.map(Expr::try_from).transpose()?;
        let assignments = self
            .assignments
            .into_iter()
            .map(Assignment::try_from)
            .collect::<Result<Vec<_>>>()?;
        Ok(Statement::Update {
            table_name,
            assignments,
            selection,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{Build, col, num, table, test, text};

    #[test]
    fn update() {
        let actual = table("Foo").update().set("id", "2").build();
        let expected = "UPDATE Foo SET id = 2";
        test(actual, expected);

        let actual = table("Foo")
            .update()
            .set("id", "2")
            .set("name", "Bar")
            .build();
        let expected = "UPDATE Foo SET id = 2, name=Bar";
        test(actual, expected);

        let actual = table("Foo")
            .update()
            .filter("Bar = 1")
            .set("id", "2")
            .set("name", "americano")
            .build();
        let expected = "UPDATE Foo SET id = 2, name = americano WHERE Bar = 1";
        test(actual, expected);

        let actual = table("Foo")
            .update()
            .filter(col("id").gt(num(1)))
            .filter("name = 'americano'")
            .set("name", text("espresso"))
            .build();
        let expected = "
            UPDATE Foo
            SET name = 'espresso'
            WHERE id > 1 AND name = 'americano'";
        test(actual, expected);

        let actual = table("Foo")
            .update()
            .filter("body_item = 1")
            .set("id", "2")
            .set(
                "head_item",
                "(SELECT id FROM head_item WHERE level = 3 LIMIT 1)",
            )
            .build();
        let expected = "UPDATE Foo SET id = 2, head_item = (SELECT id FROM head_item WHERE level = 3 LIMIT 1) WHERE body_item = 1";
        test(actual, expected);
    }
}
