mod hash_join;
mod join_constraint;
mod root;

pub use {hash_join::HashJoinNode, join_constraint::JoinConstraintNode, root::JoinNode};

use crate::ast::{JoinConstraint, JoinExecutor, JoinOperator, Select, TableFactor};

#[derive(Clone, Copy, Debug)]
pub enum JoinOperatorType {
    Inner,
    Left,
}

impl From<JoinOperatorType> for JoinOperator {
    fn from(join_operator_type: JoinOperatorType) -> Self {
        match join_operator_type {
            JoinOperatorType::Inner => JoinOperator::Inner(JoinConstraint::None),
            JoinOperatorType::Left => JoinOperator::LeftOuter(JoinConstraint::None),
        }
    }
}

pub struct JoinConstraintData {
    select: Select,
    relation: TableFactor,
    operator_type: JoinOperatorType,
    executor: JoinExecutor,
}
