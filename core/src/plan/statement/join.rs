use {
    super::{ExprPlan, TableFactorPlan},
    crate::ast,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JoinPlan {
    pub relation: TableFactorPlan,
    pub join_operator: JoinOperatorPlan,
    pub join_executor: JoinExecutorPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinExecutorPlan {
    NestedLoop,
    Hash {
        key_expr: ExprPlan,
        value_expr: ExprPlan,
        where_clause: Option<ExprPlan>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinOperatorPlan {
    Inner(JoinConstraintPlan),
    LeftOuter(JoinConstraintPlan),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinConstraintPlan {
    On(ExprPlan),
    None,
}

impl From<ast::Join> for JoinPlan {
    fn from(join: ast::Join) -> Self {
        let ast::Join {
            relation,
            join_operator,
        } = join;

        Self {
            relation: relation.into(),
            join_operator: join_operator.into(),
            join_executor: JoinExecutorPlan::NestedLoop,
        }
    }
}

impl From<ast::JoinOperator> for JoinOperatorPlan {
    fn from(join_operator: ast::JoinOperator) -> Self {
        match join_operator {
            ast::JoinOperator::Inner(constraint) => Self::Inner(constraint.into()),
            ast::JoinOperator::LeftOuter(constraint) => Self::LeftOuter(constraint.into()),
        }
    }
}

impl From<ast::JoinConstraint> for JoinConstraintPlan {
    fn from(join_constraint: ast::JoinConstraint) -> Self {
        match join_constraint {
            ast::JoinConstraint::On(expr) => Self::On(expr.into()),
            ast::JoinConstraint::None => Self::None,
        }
    }
}
