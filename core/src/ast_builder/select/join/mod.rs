mod hash_join;
mod join_constraint;
mod root;

pub use {hash_join::HashJoinNode, join_constraint::JoinConstraintNode, root::JoinNode};

#[derive(Clone, Copy)]
pub enum JoinOperatorType {
    Inner,
    Left,
}

use crate::{
    ast::{JoinExecutor, TableFactor},
    ast_builder::select::NodeData,
};

pub struct JoinConstraintData {
    node_data: NodeData,
    relation: TableFactor,
    operator_type: JoinOperatorType,
    executor: JoinExecutor,
}
