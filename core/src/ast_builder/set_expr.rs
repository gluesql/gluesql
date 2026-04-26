use {
    super::{
        ExprNode, FilterNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode, JoinNode,
        LimitNode, OffsetNode, OrderByExprList, OrderByNode, ProjectNode, SelectNode,
        TableFactorNode,
        select::{Prebuild, ValuesNode},
    },
    crate::{
        ast::{Query, SetExpr},
        result::Result,
    },
};

/// A node that represents a SQL set expression: `SELECT`, `VALUES`, or `UNION`.
///
/// This type is the correct operand for `UNION` / `UNION ALL`.  Query-level
/// clauses (`ORDER BY`, `LIMIT`, `OFFSET`) are intentionally absent — they
/// belong to the outer [`Query`], not to a branch of a `UNION`.  Trying to
/// pass a [`LimitNode`] or [`OrderByNode`] as a `UNION` operand is therefore
/// a **compile-time** error.
///
/// Use `.limit()` / `.offset()` / `.order_by()` *on* a `SetExprNode` to apply
/// those clauses to the entire set expression (including the result of a
/// `UNION`).
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

impl Prebuild<Query> for SetExprNode<'_> {
    fn prebuild(self) -> Result<Query> {
        let body = prebuild_set_expr(self)?;
        Ok(Query {
            body,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        })
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
            let q: Query = n.prebuild()?;
            Ok(q.body)
        }
        SetExprNode::Values(n) => {
            let q: Query = n.prebuild()?;
            Ok(q.body)
        }
        SetExprNode::Join(n) => {
            let q: Query = n.prebuild()?;
            Ok(q.body)
        }
        SetExprNode::JoinConstraint(n) => {
            let q: Query = n.prebuild()?;
            Ok(q.body)
        }
        SetExprNode::HashJoin(n) => {
            let q: Query = n.prebuild()?;
            Ok(q.body)
        }
        SetExprNode::GroupBy(n) => {
            let q: Query = n.prebuild()?;
            Ok(q.body)
        }
        SetExprNode::Having(n) => {
            let q: Query = n.prebuild()?;
            Ok(q.body)
        }
        SetExprNode::Filter(n) => {
            let q: Query = n.prebuild()?;
            Ok(q.body)
        }
        SetExprNode::Project(n) => {
            let q: Query = n.prebuild()?;
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
///
/// Implemented automatically for every `T: Into<SetExprNode<'a>>`, which
/// includes `SelectNode`, `ProjectNode`, `FilterNode`, `ValuesNode`, and
/// `SetExprNode` itself.  It is intentionally **not** implemented for
/// `LimitNode`, `OffsetNode`, or `OrderByNode`, making it a compile-time
/// error to use those nodes as `UNION` operands.
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
