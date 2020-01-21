use crate::translator::Row;
use nom_sql::{ConditionBase, ConditionExpression, ConditionTree, Literal, Operator};
use std::convert::From;
use std::fmt::Debug;

fn parse_expr<'a, T: Debug>(row: &'a Row<T>, expr: &'a ConditionExpression) -> Option<&'a Literal> {
    let parse_base = |base: &'a ConditionBase| match base {
        ConditionBase::Field(v) => row.get_literal(&v.name),
        ConditionBase::Literal(literal) => Some(literal),
        _ => None,
    };

    match expr {
        ConditionExpression::Base(base) => parse_base(&base),
        _ => None,
    }
}

fn check_expr<'a, T: Debug>(row: &'a Row<T>, expr: &'a ConditionExpression) -> bool {
    let check_tree = |tree: &'a ConditionTree| match tree.operator {
        Operator::Equal => {
            let left = parse_expr(row, &tree.left);
            let right = parse_expr(row, &tree.right);

            left.is_some() && right.is_some() && left == right
        }
        _ => true,
    };

    match expr {
        ConditionExpression::ComparisonOp(tree) => check_tree(&tree),
        ConditionExpression::LogicalOp(tree) => check_tree(&tree),
        _ => false,
    }
}

pub struct Filter {
    where_clause: Option<ConditionExpression>,
}

impl Filter {
    pub fn check<'a, T: Debug>(&'a self, row: &'a Row<T>) -> bool {
        self.where_clause
            .as_ref()
            .map_or(true, |expr| check_expr(row, expr))
    }
}

impl From<Option<ConditionExpression>> for Filter {
    fn from(where_clause: Option<ConditionExpression>) -> Self {
        Filter { where_clause }
    }
}
