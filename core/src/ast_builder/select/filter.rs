use {
    super::Prebuild,
    crate::{
        ast::Select,
        ast_builder::{
            ExprList, ExprNode, GroupByNode, HashJoinNode, JoinConstraintNode, JoinNode, LimitNode,
            OffsetNode, OrderByExprList, OrderByNode, ProjectNode, QueryNode, SelectItemList,
            SelectNode, TableFactorNode,
        },
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub enum PrevNode<'a> {
    Select(SelectNode<'a>),
    Join(Box<JoinNode<'a>>),
    JoinConstraint(Box<JoinConstraintNode<'a>>),
    HashJoin(Box<HashJoinNode<'a>>),
}

impl<'a> Prebuild<Select> for PrevNode<'a> {
    fn prebuild(self) -> Result<Select> {
        match self {
            Self::Select(node) => node.prebuild(),
            Self::Join(node) => node.prebuild(),
            Self::JoinConstraint(node) => node.prebuild(),
            Self::HashJoin(node) => node.prebuild(),
        }
    }
}

impl<'a> From<JoinNode<'a>> for PrevNode<'a> {
    fn from(node: JoinNode<'a>) -> Self {
        PrevNode::Join(Box::new(node))
    }
}

impl<'a> From<JoinConstraintNode<'a>> for PrevNode<'a> {
    fn from(node: JoinConstraintNode<'a>) -> Self {
        PrevNode::JoinConstraint(Box::new(node))
    }
}

impl<'a> From<HashJoinNode<'a>> for PrevNode<'a> {
    fn from(node: HashJoinNode<'a>) -> Self {
        PrevNode::HashJoin(Box::new(node))
    }
}

impl<'a> From<SelectNode<'a>> for PrevNode<'a> {
    fn from(node: SelectNode<'a>) -> Self {
        PrevNode::Select(node)
    }
}
#[derive(Clone, Debug)]
pub struct FilterNode<'a> {
    prev_node: PrevNode<'a>,
    filter_expr: ExprNode<'a>,
}

impl<'a> FilterNode<'a> {
    pub fn new<N: Into<PrevNode<'a>>, T: Into<ExprNode<'a>>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            filter_expr: expr.into(),
        }
    }

    pub fn filter<T: Into<ExprNode<'a>>>(mut self, expr: T) -> Self {
        let exprs = self.filter_expr;
        self.filter_expr = exprs.and(expr);
        self
    }

    pub fn offset<T: Into<ExprNode<'a>>>(self, expr: T) -> OffsetNode<'a> {
        OffsetNode::new(self, expr)
    }

    pub fn limit<T: Into<ExprNode<'a>>>(self, expr: T) -> LimitNode<'a> {
        LimitNode::new(self, expr)
    }

    pub fn project<T: Into<SelectItemList<'a>>>(self, select_items: T) -> ProjectNode<'a> {
        ProjectNode::new(self, select_items)
    }

    pub fn group_by<T: Into<ExprList<'a>>>(self, expr_list: T) -> GroupByNode<'a> {
        GroupByNode::new(self, expr_list)
    }

    pub fn order_by<T: Into<OrderByExprList<'a>>>(self, order_by_exprs: T) -> OrderByNode<'a> {
        OrderByNode::new(self, order_by_exprs)
    }

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode {
        QueryNode::FilterNode(self).alias_as(table_alias)
    }
}

impl<'a> Prebuild<Select> for FilterNode<'a> {
    fn prebuild(self) -> Result<Select> {
        let mut select: Select = self.prev_node.prebuild()?;
        select.selection = Some(self.filter_expr.try_into()?);

        Ok(select)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast::{
                BinaryOperator, Expr, Join, JoinConstraint, JoinExecutor, JoinOperator, Query,
                Select, SetExpr, Statement, TableFactor, TableWithJoins,
            },
            ast_builder::{col, expr, table, test, Build, SelectItemList},
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn filter() {
        // select node -> filter node -> build
        let actual = table("Bar").select().filter("id IS NULL").build();
        let expected = "SELECT * FROM Bar WHERE id IS NULL";
        test(actual, expected);

        // select node -> filter node -> build
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

        // filter node -> filter node -> build
        let actual = table("Bar")
            .select()
            .filter("id IS NULL")
            .filter("id > 10")
            .filter("id < 20")
            .build();
        let expected = "SELECT * FROM Bar WHERE id IS NULL AND id > 10 AND id < 20";
        test(actual, expected);

        // join node -> filter node -> build
        let actual = table("Foo")
            .select()
            .join("Bar")
            .filter("id IS NULL")
            .build();
        let expected = "SELECT * FROM Foo JOIN Bar WHERE id IS NULL";
        test(actual, expected);

        // join node -> filter node -> build
        let actual = table("Foo")
            .select()
            .join_as("Bar", "b")
            .filter("id IS NULL")
            .build();
        let expected = "SELECT * FROM Foo JOIN Bar AS b WHERE id IS NULL";
        test(actual, expected);

        // join node -> filter node -> build
        let actual = table("Foo")
            .select()
            .left_join("Bar")
            .filter("id IS NULL")
            .build();
        let expected = "SELECT * FROM Foo LEFT JOIN Bar WHERE id IS NULL";
        test(actual, expected);

        // join node -> filter node -> build
        let actual = table("Foo")
            .select()
            .left_join_as("Bar", "b")
            .filter("id IS NULL")
            .build();
        let expected = "SELECT * FROM Foo LEFT JOIN Bar AS b WHERE id IS NULL";
        test(actual, expected);

        // join constraint node -> filter node -> build
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .filter("id IS NULL")
            .build();
        let expected = "SELECT * FROM Foo JOIN Bar ON Foo.id = Bar.id WHERE id IS NULL";
        test(actual, expected);

        // hash join node -> filter node -> build
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .filter("PlayerItem.amount > 10")
            .build();
        let expected = {
            let join = Join {
                relation: TableFactor::Table {
                    name: "PlayerItem".to_owned(),
                    alias: None,
                    index: None,
                },
                join_operator: JoinOperator::Inner(JoinConstraint::None),
                join_executor: JoinExecutor::Hash {
                    key_expr: col("PlayerItem.user_id").try_into().unwrap(),
                    value_expr: col("Player.id").try_into().unwrap(),
                    where_clause: None,
                },
            };
            let select = Select {
                projection: SelectItemList::from("*").try_into().unwrap(),
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: "Player".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: vec![join],
                },
                selection: Some(expr("PlayerItem.amount > 10").try_into().unwrap()),
                group_by: Vec::new(),
                having: None,
            };

            Ok(Statement::Query(Query {
                body: SetExpr::Select(Box::new(select)),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            }))
        };
        assert_eq!(actual, expected);

        // select node -> filter node -> derived subquery
        let actual = table("Bar")
            .select()
            .filter("id IS NULL")
            .alias_as("Sub")
            .select()
            .build();
        let expected = "SELECT * FROM (SELECT * FROM Bar WHERE id IS NULL) Sub";
        test(actual, expected);
    }
}
