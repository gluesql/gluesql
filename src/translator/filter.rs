use crate::translator::Row;
use nom_sql::{ConditionBase, ConditionExpression, ConditionTree, Operator};
use std::convert::From;
use std::fmt::Debug;

pub struct Filter {
    where_clause: Option<ConditionExpression>,
}

impl Filter {
    pub fn check<'a, T: Debug>(&'a self, row: &'a Row<T>) -> bool {
        let parse_base = |base: &'a ConditionBase| match base {
            ConditionBase::Field(v) => row.get_literal(&v.name),
            ConditionBase::Literal(literal) => Some(literal),
            _ => None,
        };

        let parse_expr = |expr: &'a ConditionExpression| match expr {
            ConditionExpression::Base(base) => parse_base(&base),
            _ => None,
        };

        let check_tree = |tree: &'a ConditionTree| {
            let left = parse_expr(&tree.left);
            let right = parse_expr(&tree.right);

            match tree.operator {
                Operator::Equal => left.is_some() && right.is_some() && left == right,
                _ => true,
            }
        };

        let check_expr = |expr: &'a ConditionExpression| match expr {
            ConditionExpression::ComparisonOp(tree) => check_tree(&tree),
            _ => false,
        };

        self.where_clause.as_ref().map_or(false, check_expr)
    }
}

impl From<Option<ConditionExpression>> for Filter {
    fn from(where_clause: Option<ConditionExpression>) -> Self {
        Filter { where_clause }
    }
}
