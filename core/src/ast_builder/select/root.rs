use {
    super::{build_stmt, NodeData, Prebuild},
    crate::{
        ast::{Expr, ObjectName, SelectItem, Statement, TableFactor, TableWithJoins},
        ast_builder::{
            ExprList, ExprNode, GroupByNode, LimitNode, OffsetNode, ProjectNode, SelectItemList,
        },
        result::Result,
    },
};

#[derive(Clone)]
pub struct SelectNode {
    table_name: String,
    filter_expr: Option<ExprNode>,
}

impl SelectNode {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            filter_expr: None,
        }
    }

    pub fn filter<T: Into<ExprNode>>(mut self, expr: T) -> Self {
        self.filter_expr = Some(expr.into());

        self
    }

    pub fn group_by<T: Into<ExprList>>(self, expr_list: T) -> GroupByNode {
        GroupByNode::new(self, expr_list)
    }

    pub fn offset<T: Into<ExprNode>>(self, expr: T) -> OffsetNode {
        OffsetNode::new(self, expr)
    }

    pub fn limit<T: Into<ExprNode>>(self, expr: T) -> LimitNode {
        LimitNode::new(self, expr)
    }

    pub fn project<T: Into<SelectItemList>>(self, select_items: T) -> ProjectNode {
        ProjectNode::new(self, select_items)
    }

    pub fn build(self) -> Result<Statement> {
        let select_data = self.prebuild()?;

        Ok(build_stmt(select_data))
    }
}

impl Prebuild for SelectNode {
    fn prebuild(self) -> Result<NodeData> {
        let relation = TableFactor::Table {
            name: ObjectName(vec![self.table_name]),
            alias: None,
            index: None,
        };

        let from = TableWithJoins {
            relation,
            joins: vec![],
        };

        let selection = self.filter_expr.map(Expr::try_from).transpose()?;

        Ok(NodeData {
            projection: vec![SelectItem::Wildcard],
            from,
            selection,
            group_by: vec![],
            having: None,
            offset: None,
            limit: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{BinaryOperator, Expr},
        ast_builder::{table, test},
    };

    #[test]
    fn select() {
        let actual = table("App").select().build();
        let expected = "SELECT * FROM App";
        test(actual, expected);

        let actual = table("Bar").select().filter("id IS NULL").build();
        let expected = "SELECT * FROM Bar WHERE id IS NULL";
        test(actual, expected);

        let actual = table("Foo")
            .select()
            .filter(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("col1".to_owned())),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Identifier("col2".to_owned())),
            })
            .build();
        let expected = "SELECT * FROM Foo WHERE col1 > col2";
        test(actual, expected);
    }
}
