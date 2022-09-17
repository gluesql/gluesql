use {
    super::{NodeData, Prebuild},
    crate::{
        ast::{Expr, ObjectName, SelectItem, Statement, TableFactor, TableWithJoins},
        ast_builder::{
            ExprList, ExprNode, GroupByNode, LimitNode, OffsetNode, OrderByExprList, OrderByNode,
            ProjectNode, SelectItemList,
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
        if let Some(exprs) = self.filter_expr {
            self.filter_expr = Some(exprs.and(expr));
        } else {
            self.filter_expr = Some(expr.into());
        }

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

    pub fn order_by<T: Into<OrderByExprList>>(self, order_by_exprs: T) -> OrderByNode {
        OrderByNode::new(self, order_by_exprs)
    }

    pub fn build(self) -> Result<Statement> {
        self.prebuild().map(NodeData::build_stmt)
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
            order_by: vec![],
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

        let actual = table("Bar")
            .select()
            .filter("id IS NULL")
            .filter("id > 10")
            .filter("id < 20")
            .build();
        let expected = "SELECT * FROM Bar WHERE id IS NULL AND id > 10 AND id < 20";
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
