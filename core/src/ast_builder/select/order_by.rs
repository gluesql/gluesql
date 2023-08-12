use {
    super::{Prebuild, ValuesNode},
    crate::{
        ast::Query,
        ast_builder::{
            ExprNode, FilterNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode,
            JoinNode, LimitNode, OffsetNode, OrderByExprList, ProjectNode, QueryNode, SelectNode,
            TableFactorNode,
        },
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub enum PrevNode<'a> {
    Select(SelectNode<'a>),
    Having(HavingNode<'a>),
    GroupBy(GroupByNode<'a>),
    Filter(FilterNode<'a>),
    JoinNode(JoinNode<'a>),
    JoinConstraint(JoinConstraintNode<'a>),
    HashJoin(Box<HashJoinNode<'a>>),
    ProjectNode(Box<ProjectNode<'a>>),
    Values(ValuesNode<'a>),
}

impl<'a> Prebuild<Query> for PrevNode<'a> {
    fn prebuild(self) -> Result<Query> {
        match self {
            Self::Select(node) => node.prebuild(),
            Self::Having(node) => node.prebuild(),
            Self::GroupBy(node) => node.prebuild(),
            Self::Filter(node) => node.prebuild(),
            Self::JoinNode(node) => node.prebuild(),
            Self::JoinConstraint(node) => node.prebuild(),
            Self::HashJoin(node) => node.prebuild(),
            Self::ProjectNode(node) => node.prebuild(),
            Self::Values(node) => node.prebuild(),
        }
    }
}

impl<'a> From<SelectNode<'a>> for PrevNode<'a> {
    fn from(node: SelectNode<'a>) -> Self {
        PrevNode::Select(node)
    }
}

impl<'a> From<HavingNode<'a>> for PrevNode<'a> {
    fn from(node: HavingNode<'a>) -> Self {
        PrevNode::Having(node)
    }
}

impl<'a> From<GroupByNode<'a>> for PrevNode<'a> {
    fn from(node: GroupByNode<'a>) -> Self {
        PrevNode::GroupBy(node)
    }
}

impl<'a> From<FilterNode<'a>> for PrevNode<'a> {
    fn from(node: FilterNode<'a>) -> Self {
        PrevNode::Filter(node)
    }
}

impl<'a> From<JoinNode<'a>> for PrevNode<'a> {
    fn from(node: JoinNode<'a>) -> Self {
        PrevNode::JoinNode(node)
    }
}

impl<'a> From<JoinConstraintNode<'a>> for PrevNode<'a> {
    fn from(node: JoinConstraintNode<'a>) -> Self {
        PrevNode::JoinConstraint(node)
    }
}

impl<'a> From<HashJoinNode<'a>> for PrevNode<'a> {
    fn from(node: HashJoinNode<'a>) -> Self {
        PrevNode::HashJoin(Box::new(node))
    }
}

impl<'a> From<ProjectNode<'a>> for PrevNode<'a> {
    fn from(node: ProjectNode<'a>) -> Self {
        PrevNode::ProjectNode(Box::new(node))
    }
}

impl<'a> From<ValuesNode<'a>> for PrevNode<'a> {
    fn from(node: ValuesNode<'a>) -> Self {
        PrevNode::Values(node)
    }
}

#[derive(Clone, Debug)]
pub struct OrderByNode<'a> {
    prev_node: PrevNode<'a>,
    expr_list: OrderByExprList<'a>,
}

impl<'a> OrderByNode<'a> {
    pub fn new<N: Into<PrevNode<'a>>, T: Into<OrderByExprList<'a>>>(
        prev_node: N,
        expr_list: T,
    ) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr_list: expr_list.into(),
        }
    }

    pub fn offset<T: Into<ExprNode<'a>>>(self, expr: T) -> OffsetNode<'a> {
        OffsetNode::new(self, expr)
    }

    pub fn limit<T: Into<ExprNode<'a>>>(self, expr: T) -> LimitNode<'a> {
        LimitNode::new(self, expr)
    }

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode {
        QueryNode::OrderByNode(self).alias_as(table_alias)
    }
}

impl<'a> Prebuild<Query> for OrderByNode<'a> {
    fn prebuild(self) -> Result<Query> {
        let mut node_data = self.prev_node.prebuild()?;
        node_data.order_by = self.expr_list.try_into()?;

        Ok(node_data)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast::{
                Join, JoinConstraint, JoinExecutor, JoinOperator, Query, Select, SetExpr,
                Statement, TableFactor, TableWithJoins,
            },
            ast_builder::{col, table, test, Build, ExprNode, OrderByExprList, SelectItemList},
        },
        pretty_assertions::assert_eq,
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
            .order_by(ExprNode::Identifier("name".into()))
            .offset(2)
            .limit(3)
            .build();
        let expected = "
            SELECT * FROM Foo
            GROUP BY city
            HAVING COUNT(name) < 100
            ORDER BY name
            OFFSET 2
            LIMIT 3
        ";
        test(actual, expected);

        // filter node -> order by node -> build
        let actual = table("Foo")
            .select()
            .filter("id > 10")
            .filter("id < 20")
            .order_by("id asc")
            .build();
        let expected = "
            SELECT * FROM Foo
            WHERE id > 10 AND id < 20
            ORDER BY id ASC";
        test(actual, expected);

        // project node -> order by node -> build
        let actual = table("Foo")
            .select()
            .project("id")
            .order_by("id asc")
            .build();
        let expected = "SELECT id FROM Foo ORDER BY id asc";
        test(actual, expected);

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

        // select -> order by node -> derived subquery
        let actual = table("Foo")
            .select()
            .order_by(vec!["name desc"])
            .alias_as("Sub")
            .select()
            .build();
        let expected = "
            SELECT * FROM (
                SELECT * FROM Foo
                ORDER BY name DESC
            ) Sub
        ";
        test(actual, expected);
    }
}
