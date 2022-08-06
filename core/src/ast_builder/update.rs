use crate::{
    ast::{Assignment, Expr, ObjectName, Statement},
    result::Result,
};

use super::AssignmentNode;
use super::ExprNode;

#[derive(Clone)]
pub struct UpdateNode {
    table_name: String,
    assignments: Vec<AssignmentNode>,
    selection: Option<ExprNode>,
}

impl UpdateNode {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            assignments: Vec::new(),
            selection: None,
        }
    }

    pub fn filter<T: Into<ExprNode>>(mut self, expr: T) -> Self {
        self.selection = Some(expr.into());
        self
    }
    pub fn set<T: Into<AssignmentNode>>(mut self, assignment: T) -> Self {
        self.assignments.push(assignment.into());
        self
    }
    pub fn build(self) -> Result<Statement> {
        let table_name = ObjectName(vec![self.table_name]);
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
    use crate::ast_builder::{table, test};

    #[test]
    fn update() {
        let actual = table("Foo").update().set("id=2").build();
        let expected = "UPDATE Foo SET id = 2";
        test(actual, expected);

        let actual = table("Foo")
            .update()
            .set("id = 2")
            .set("name = '😝'")
            .build();
        let expected = "UPDATE Foo SET id = 2, name='😝'";
        test(actual, expected);

        let actual = table("Foo")
            .update()
            .set("id = 2")
            .set("name = '😝'")
            .filter("Bar = 1")
            .build();
        let expected = "UPDATE Foo SET id = 2, name='😝' WHERE Bar = 1";
        test(actual, expected);

        let actual = table("Foo")
            .update()
            .set("id = 2")
            .set("head_item = (SELECT id FROM head_item WHERE level = 3 LIMIT 1)")
            .filter("body_item = 1")
            .build();
        let expected = "UPDATE Foo SET id = 2, head_item = (SELECT id FROM head_item WHERE level = 3 LIMIT 1) WHERE body_item = 1";
        test(actual, expected);
    }
}
