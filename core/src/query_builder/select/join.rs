mod hash_join;
mod join_constraint;
mod root;

pub use {hash_join::HashJoinNode, join_constraint::JoinConstraintNode, root::JoinNode};

use crate::{
    ast::{Expr, JoinConstraint, JoinOperator},
    plan::{JoinConstraintPlan, JoinOperatorPlan},
};

#[derive(Clone, Copy, Debug)]
pub(in crate::query_builder::select) enum JoinOperatorType {
    Inner,
    Left,
}

pub(super) fn join_operator_with_constraint(
    operator_type: JoinOperatorType,
    constraint: Option<Expr>,
) -> JoinOperator {
    match (operator_type, constraint) {
        (JoinOperatorType::Inner, Some(expr)) => JoinOperator::Inner(JoinConstraint::On(expr)),
        (JoinOperatorType::Inner, None) => JoinOperator::Inner(JoinConstraint::None),
        (JoinOperatorType::Left, Some(expr)) => JoinOperator::LeftOuter(JoinConstraint::On(expr)),
        (JoinOperatorType::Left, None) => JoinOperator::LeftOuter(JoinConstraint::None),
    }
}

pub(super) fn join_operator_plan_with_constraint(
    operator_type: JoinOperatorType,
    constraint: JoinConstraintPlan,
) -> JoinOperatorPlan {
    match operator_type {
        JoinOperatorType::Inner => JoinOperatorPlan::Inner(constraint),
        JoinOperatorType::Left => JoinOperatorPlan::LeftOuter(constraint),
    }
}
