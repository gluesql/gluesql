use crate::{
    ast::{TableAlias, TableFactor},
    plan::{SourcePlan, TableAccessPlan, TableAliasPlan, TableSourcePlan},
};

mod inner_hash_join;
mod inner_join_condition;
mod inner_nested_loop_join;
mod left_outer_hash_join;
mod left_outer_join_condition;
mod left_outer_nested_loop_join;

pub use {
    inner_hash_join::InnerHashJoinNode, inner_join_condition::InnerJoinConditionNode,
    inner_nested_loop_join::InnerNestedLoopJoinNode, left_outer_hash_join::LeftOuterHashJoinNode,
    left_outer_join_condition::LeftOuterJoinConditionNode,
    left_outer_nested_loop_join::LeftOuterNestedLoopJoinNode,
};

fn table_source_plan(name: String, alias: Option<String>) -> SourcePlan {
    SourcePlan::Table(TableSourcePlan {
        name,
        alias: alias.map(|name| TableAliasPlan {
            name,
            columns: Vec::new(),
        }),
        access: TableAccessPlan::FullScan,
    })
}

fn table_factor(name: String, alias: Option<String>) -> TableFactor {
    TableFactor::Table {
        name,
        alias: alias.map(|name| TableAlias {
            name,
            columns: Vec::new(),
        }),
    }
}
