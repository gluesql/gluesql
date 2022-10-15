use {
    super::{NodeData, Prebuild},
    crate::{
        ast_builder::{
            ExprNode, FilterNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode,
            JoinNode, LimitNode, OffsetNode, OrderByExprList, ProjectNode, SelectItemList,
            SelectNode,
        },
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
    Having(HavingNode),
    GroupBy(GroupByNode),
    Filter(FilterNode),
    JoinNode(JoinNode),
    JoinConstraint(JoinConstraintNode),
    HashJoin(HashJoinNode),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Select(node) => node.prebuild(),
            Self::Having(node) => node.prebuild(),
            Self::GroupBy(node) => node.prebuild(),
            Self::Filter(node) => node.prebuild(),
            Self::JoinNode(node) => node.prebuild(),
            Self::JoinConstraint(node) => node.prebuild(),
            Self::HashJoin(node) => node.prebuild(),
        }
    }
}

impl From<SelectNode> for PrevNode {
    fn from(node: SelectNode) -> Self {
        PrevNode::Select(node)
    }
}

impl From<HavingNode> for PrevNode {
    fn from(node: HavingNode) -> Self {
        PrevNode::Having(node)
    }
}

impl From<GroupByNode> for PrevNode {
    fn from(node: GroupByNode) -> Self {
        PrevNode::GroupBy(node)
    }
}

impl From<FilterNode> for PrevNode {
    fn from(node: FilterNode) -> Self {
        PrevNode::Filter(node)
    }
}

impl From<JoinNode> for PrevNode {
    fn from(node: JoinNode) -> Self {
        PrevNode::JoinNode(node)
    }
}

impl From<JoinConstraintNode> for PrevNode {
    fn from(node: JoinConstraintNode) -> Self {
        PrevNode::JoinConstraint(node)
    }
}

impl From<HashJoinNode> for PrevNode {
    fn from(node: HashJoinNode) -> Self {
        PrevNode::HashJoin(node)
    }
}

#[derive(Clone)]
pub struct OrderByNode {
    prev_node: PrevNode,
    expr_list: OrderByExprList,
}

impl OrderByNode {
    pub fn new<N: Into<PrevNode>, T: Into<OrderByExprList>>(prev_node: N, expr_list: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr_list: expr_list.into(),
        }
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
}

impl Prebuild for OrderByNode {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.order_by = self.expr_list.try_into()?;

        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{
            Join, JoinConstraint, JoinExecutor, JoinOperator, Query, Select, SetExpr, Statement,
            TableFactor, TableWithJoins,
        },
        ast_builder::{col, table, test, Build, ExprNode, OrderByExprList, SelectItemList},
    };

    #[test]
    fn order_by() {
        // select node -> order by node(exprs vec) -> build
        let actual = table("Foo").select().order_by(vec!["name desc"]).build();
        let expected = "
            SELECT * FROM Foo
            ORDER BY name DESC
        ";
        test(actual, expected);

        // select node -> order by node(exprs string) -> build
        let actual = table("Bar")
            .select()
            .order_by("name asc, id desc, country")
            .offset(10)
            .build();
        let expected = "
                SELECT * FROM Bar 
                ORDER BY name asc, id desc, country 
                OFFSET 10
            ";
        test(actual, expected);

        // group by node -> order by node -> build
        let actual = table("Bar")
            .select()
            .group_by("name")
            .order_by(vec!["id desc"])
            .build();
        let expected = "
                SELECT * FROM Bar 
                GROUP BY name 
                ORDER BY id desc
            ";
        test(actual, expected);

        // having node -> order by node -> build
        let actual = table("Foo")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .order_by(ExprNode::Identifier("name".to_owned()))
            .limit(3)
            .offset(2)
            .build();
        let expected = "
            SELECT * FROM Foo
            GROUP BY city
            HAVING COUNT(name) < 100
            ORDER BY name
            LIMIT 3
            OFFSET 2
        ";
        test(actual, expected);

        // filter node -> order by node -> build
        let actaul = table("Foo")
            .select()
            .filter("id > 10")
            .filter("id < 20")
            .order_by("id asc")
            .build();
        let expected = "
            SELECT * FROM Foo
            WHERE id > 10 AND id < 20
            ORDER BY id ASC";
        test(actaul, expected);

        // join node -> order by node -> build
        let actual = table("Foo")
            .select()
            .join("Bar")
            .order_by("Foo.id desc")
            .build();
        let expected = "
            SELECT * FROM Foo
            JOIN Bar
            ORDER BY Foo.id desc
        ";
        test(actual, expected);

        // join constraint node -> order by node -> build
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .order_by("Foo.id desc")
            .build();
        let expected = "
            SELECT * FROM Foo
            JOIN Bar ON Foo.id = Bar.id
            ORDER BY Foo.id desc
        ";
        test(actual, expected);

        // hash join node -> order by node -> build
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .order_by("Player.score DESC")
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
                selection: None,
                group_by: Vec::new(),
                having: None,
            };

            Ok(Statement::Query(Query {
                body: SetExpr::Select(Box::new(select)),
                order_by: OrderByExprList::from("Player.score DESC")
                    .try_into()
                    .unwrap(),
                limit: None,
                offset: None,
            }))
        };
        assert_eq!(actual, expected);
    }
}
