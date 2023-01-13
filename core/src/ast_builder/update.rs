use {
    super::{AssignmentNode, Build, ExprNode},
    crate::{
        ast::{Assignment, Expr, Statement},
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub struct UpdateNode<'a> {
    table_name: String,
    assignments: Vec<AssignmentNode<'a>>,
    selection: Option<ExprNode<'a>>,
}

impl<'a> UpdateNode<'a> {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            assignments: Vec::new(),
            selection: None,
        }
    }

    pub fn filter<T: Into<ExprNode<'a>>>(mut self, expr: T) -> Self {
        self.selection = Some(expr.into());
        self
    }

    pub fn set<T: Into<ExprNode<'a>>>(mut self, id: &str, value: T) -> Self {
        self.assignments
            .push(AssignmentNode::Expr(id.to_owned(), value.into()));
        self
    }
}

impl<'a> Build for UpdateNode<'a> {
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
    use crate::ast_builder::{table, test, Build};

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
            .set("id", "2")
            .set("name", "americano")
            .filter("Bar = 1")
            .build();
        let expected = "UPDATE Foo SET id = 2, name = americano WHERE Bar = 1";
        test(actual, expected);

        let actual = table("Foo")
            .update()
            .set("id", "2")
            .set(
                "head_item",
                "(SELECT id FROM head_item WHERE level = 3 LIMIT 1)",
            )
            .filter("body_item = 1")
            .build();
        let expected = "UPDATE Foo SET id = 2, head_item = (SELECT id FROM head_item WHERE level = 3 LIMIT 1) WHERE body_item = 1";
        test(actual, expected);
    }
}
