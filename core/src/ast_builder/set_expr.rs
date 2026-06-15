use {
    super::{
        ExprNode, FilterNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode, JoinNode,
        LimitNode, OffsetNode, OrderByExprList, OrderByNode, ProjectNode, SelectNode,
        TableFactorNode,
        select::{BuildQuery, BuildQueryPlan, ValuesNode},
    },
    crate::{
        ast::{Query, SetExpr},
        plan::{QueryPlan, SetExprPlan},
        result::Result,
    },
};

/// A builder node for a SQL set expression (`SELECT`, `VALUES`, or `UNION`).
///
/// Use `.limit()`, `.offset()`, or `.order_by()` to apply query-level clauses
/// to the combined result.
#[derive(Clone, Debug)]
pub enum SetExprNode<'a> {
    Select(SelectNode<'a>),
    Values(ValuesNode<'a>),
    Join(Box<JoinNode<'a>>),
    JoinConstraint(Box<JoinConstraintNode<'a>>),
    HashJoin(Box<HashJoinNode<'a>>),
    GroupBy(GroupByNode<'a>),
    Having(HavingNode<'a>),
    Filter(FilterNode<'a>),
    Project(Box<ProjectNode<'a>>),
    Union {
        left: Box<SetExprNode<'a>>,
        right: Box<SetExprNode<'a>>,
        all: bool,
    },
}

impl<'a> SetExprNode<'a> {
    pub fn order_by<T: Into<OrderByExprList<'a>>>(self, expr_list: T) -> OrderByNode<'a> {
        OrderByNode::new(self, expr_list)
    }

    pub fn limit<T: Into<ExprNode<'a>>>(self, expr: T) -> LimitNode<'a> {
        LimitNode::new(self, expr)
    }

    pub fn offset<T: Into<ExprNode<'a>>>(self, expr: T) -> OffsetNode<'a> {
        OffsetNode::new(self, expr)
    }

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode<'a> {
        super::QueryNode::SetExpr(self).alias_as(table_alias)
    }
}

impl BuildQuery for SetExprNode<'_> {
    fn build_query(self) -> Result<Query> {
        let body = prebuild_set_expr(self)?;
        Ok(Query {
            body,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        })
    }
}

impl BuildQueryPlan for SetExprNode<'_> {
    fn build_query_plan(self) -> Result<QueryPlan> {
        let body = build_set_expr_plan(self)?;
        Ok(QueryPlan {
            body,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        })
    }
}

pub(super) fn build_set_expr_plan(node: SetExprNode<'_>) -> Result<SetExprPlan> {
    match node {
        SetExprNode::Select(n) => {
            let q = n.build_query_plan()?;
            Ok(q.body)
        }
        SetExprNode::Values(n) => {
            let q = n.build_query_plan()?;
            Ok(q.body)
        }
        SetExprNode::Join(n) => {
            let q = n.build_query_plan()?;
            Ok(q.body)
        }
        SetExprNode::JoinConstraint(n) => {
            let q = n.build_query_plan()?;
            Ok(q.body)
        }
        SetExprNode::HashJoin(n) => {
            let q = n.build_query_plan()?;
            Ok(q.body)
        }
        SetExprNode::GroupBy(n) => {
            let q = n.build_query_plan()?;
            Ok(q.body)
        }
        SetExprNode::Having(n) => {
            let q = n.build_query_plan()?;
            Ok(q.body)
        }
        SetExprNode::Filter(n) => {
            let q = n.build_query_plan()?;
            Ok(q.body)
        }
        SetExprNode::Project(n) => {
            let q = n.build_query_plan()?;
            Ok(q.body)
        }
        SetExprNode::Union { left, right, all } => {
            let left = build_set_expr_plan(*left)?;
            let right = build_set_expr_plan(*right)?;
            Ok(SetExprPlan::Union {
                left: Box::new(left),
                right: Box::new(right),
                all,
            })
        }
    }
}

/// Build a `SetExpr` from a `SetExprNode`.
///
/// This is `pub(super)` so that `limit::PrevNode`, `offset::PrevNode`, and
/// `order_by::PrevNode` can call it when constructing the base `Query` they
/// then decorate.
pub(super) fn prebuild_set_expr(node: SetExprNode<'_>) -> Result<SetExpr> {
    match node {
        SetExprNode::Select(n) => {
            let q = n.build_query()?;
            Ok(q.body)
        }
        SetExprNode::Values(n) => {
            let q = n.build_query()?;
            Ok(q.body)
        }
        SetExprNode::Join(n) => {
            let q = n.build_query()?;
            Ok(q.body)
        }
        SetExprNode::JoinConstraint(n) => {
            let q = n.build_query()?;
            Ok(q.body)
        }
        SetExprNode::HashJoin(n) => {
            let q = n.build_query()?;
            Ok(q.body)
        }
        SetExprNode::GroupBy(n) => {
            let q = n.build_query()?;
            Ok(q.body)
        }
        SetExprNode::Having(n) => {
            let q = n.build_query()?;
            Ok(q.body)
        }
        SetExprNode::Filter(n) => {
            let q = n.build_query()?;
            Ok(q.body)
        }
        SetExprNode::Project(n) => {
            let q = n.build_query()?;
            Ok(q.body)
        }
        SetExprNode::Union { left, right, all } => {
            let left = prebuild_set_expr(*left)?;
            let right = prebuild_set_expr(*right)?;
            Ok(SetExpr::Union {
                left: Box::new(left),
                right: Box::new(right),
                all,
            })
        }
    }
}

// ── From impls ──────────────────────────────────────────────────────────────

impl<'a> From<SelectNode<'a>> for SetExprNode<'a> {
    fn from(node: SelectNode<'a>) -> Self {
        SetExprNode::Select(node)
    }
}

impl<'a> From<ValuesNode<'a>> for SetExprNode<'a> {
    fn from(node: ValuesNode<'a>) -> Self {
        SetExprNode::Values(node)
    }
}

impl<'a> From<JoinNode<'a>> for SetExprNode<'a> {
    fn from(node: JoinNode<'a>) -> Self {
        SetExprNode::Join(Box::new(node))
    }
}

impl<'a> From<JoinConstraintNode<'a>> for SetExprNode<'a> {
    fn from(node: JoinConstraintNode<'a>) -> Self {
        SetExprNode::JoinConstraint(Box::new(node))
    }
}

impl<'a> From<HashJoinNode<'a>> for SetExprNode<'a> {
    fn from(node: HashJoinNode<'a>) -> Self {
        SetExprNode::HashJoin(Box::new(node))
    }
}

impl<'a> From<GroupByNode<'a>> for SetExprNode<'a> {
    fn from(node: GroupByNode<'a>) -> Self {
        SetExprNode::GroupBy(node)
    }
}

impl<'a> From<HavingNode<'a>> for SetExprNode<'a> {
    fn from(node: HavingNode<'a>) -> Self {
        SetExprNode::Having(node)
    }
}

impl<'a> From<FilterNode<'a>> for SetExprNode<'a> {
    fn from(node: FilterNode<'a>) -> Self {
        SetExprNode::Filter(node)
    }
}

impl<'a> From<ProjectNode<'a>> for SetExprNode<'a> {
    fn from(node: ProjectNode<'a>) -> Self {
        SetExprNode::Project(Box::new(node))
    }
}

// ── SetExprBuild trait ───────────────────────────────────────────────────────

/// Extension trait that adds `union` and `union_all` to any node that can
/// produce a set expression.
pub trait SetExprBuild<'a>: Into<SetExprNode<'a>> + Sized {
    fn union(self, right: impl Into<SetExprNode<'a>>) -> SetExprNode<'a> {
        SetExprNode::Union {
            left: Box::new(self.into()),
            right: Box::new(right.into()),
            all: false,
        }
    }

    fn union_all(self, right: impl Into<SetExprNode<'a>>) -> SetExprNode<'a> {
        SetExprNode::Union {
            left: Box::new(self.into()),
            right: Box::new(right.into()),
            all: true,
        }
    }
}

impl<'a, T: Into<SetExprNode<'a>>> SetExprBuild<'a> for T {}

#[cfg(test)]
mod tests {
    use {
        super::SetExprBuild,
        crate::ast_builder::{QueryNode, table, test_query, values},
    };

    #[test]
    fn chained_union() {
        // a.union(b) returns SetExprNode; calling .union(c) on that verifies
        // the blanket SetExprBuild impl works for SetExprNode itself.
        let actual: QueryNode = table("A")
            .select()
            .project("id")
            .union(table("B").select().project("id"))
            .union(table("C").select().project("id"))
            .into();
        let expected = "SELECT id FROM A UNION SELECT id FROM B UNION SELECT id FROM C";
        test_query(actual, expected);
    }

    #[test]
    fn chained_union_all() {
        let actual: QueryNode = table("A")
            .select()
            .project("id")
            .union_all(table("B").select().project("id"))
            .union_all(table("C").select().project("id"))
            .into();
        let expected = "SELECT id FROM A UNION ALL SELECT id FROM B UNION ALL SELECT id FROM C";
        test_query(actual, expected);
    }

    // ── SetExprNode variant coverage ─────────────────────────────────────────

    #[test]
    fn values_as_union_branch() {
        // SetExprNode::Values via prebuild_set_expr
        let actual: QueryNode = table("A")
            .select()
            .project("id")
            .union(values(vec!["1"]))
            .into();
        let expected = "SELECT id FROM A UNION VALUES(1)";
        test_query(actual, expected);
    }

    #[test]
    fn filter_as_union_branch() {
        // SetExprNode::Filter via prebuild_set_expr
        let actual: QueryNode = table("A")
            .select()
            .filter("id > 1")
            .union(table("B").select().filter("id > 2"))
            .into();
        let expected = "SELECT * FROM A WHERE id > 1 UNION SELECT * FROM B WHERE id > 2";
        test_query(actual, expected);
    }

    #[test]
    fn group_by_as_union_branch() {
        // SetExprNode::GroupBy via prebuild_set_expr
        let actual: QueryNode = table("A")
            .select()
            .group_by("city")
            .union(table("B").select().group_by("city"))
            .into();
        let expected = "SELECT * FROM A GROUP BY city UNION SELECT * FROM B GROUP BY city";
        test_query(actual, expected);
    }

    #[test]
    fn having_as_union_branch() {
        // SetExprNode::Having via prebuild_set_expr
        let actual: QueryNode = table("A")
            .select()
            .group_by("city")
            .having("COUNT(*) > 1")
            .union(table("B").select().group_by("city").having("COUNT(*) > 2"))
            .into();
        let expected = "SELECT * FROM A GROUP BY city HAVING COUNT(*) > 1 UNION SELECT * FROM B GROUP BY city HAVING COUNT(*) > 2";
        test_query(actual, expected);
    }

    // ── SetExprNode query-level decorators ───────────────────────────────────

    #[test]
    fn union_with_limit() {
        // SetExprNode::limit() → LimitNode with PrevNode::SetExpr
        let actual: QueryNode = table("A")
            .select()
            .project("id")
            .union(table("B").select().project("id"))
            .limit(5)
            .into();
        let expected = "SELECT id FROM A UNION SELECT id FROM B LIMIT 5";
        test_query(actual, expected);
    }

    #[test]
    fn union_with_offset() {
        // SetExprNode::offset() → OffsetNode with PrevNode::SetExpr
        let actual: QueryNode = table("A")
            .select()
            .project("id")
            .union(table("B").select().project("id"))
            .offset(3)
            .into();
        let expected = "SELECT id FROM A UNION SELECT id FROM B OFFSET 3";
        test_query(actual, expected);
    }

    #[test]
    fn union_with_order_by() {
        // SetExprNode::order_by() → OrderByNode with PrevNode::SetExpr
        let actual: QueryNode = table("A")
            .select()
            .project("id")
            .union(table("B").select().project("id"))
            .order_by("id DESC")
            .into();
        let expected = "SELECT id FROM A UNION SELECT id FROM B ORDER BY id DESC";
        test_query(actual, expected);
    }

    #[test]
    fn union_alias_as() {
        // SetExprNode::alias_as() → derived subquery used as a FROM source
        let actual: QueryNode = table("A")
            .select()
            .project("id")
            .union(table("B").select().project("id"))
            .alias_as("Sub")
            .select()
            .into();
        let expected = "SELECT * FROM (SELECT id FROM A UNION SELECT id FROM B) Sub";
        test_query(actual, expected);
    }
}
