use crate::row::Row;
use nom_sql::{ConditionBase, ConditionExpression, ConditionTree, Literal, Operator};
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
    let check_tree = |tree: &'a ConditionTree| {
        let check = || Ok((check_expr(row, &tree.left), check_expr(row, &tree.right)));
        let parse = || Ok((parse_expr(row, &tree.left), parse_expr(row, &tree.right)));

        let result: Result<bool, ()> = match tree.operator {
            Operator::Equal => parse().map(|(l, r)| l.is_some() && r.is_some() && l == r),
            Operator::NotEqual => parse().map(|(l, r)| l.is_some() && r.is_some() && l != r),
            Operator::And => check().map(|(l, r)| l && r),
            Operator::Or => check().map(|(l, r)| l || r),
            _ => Ok(false),
        };

        result.unwrap()
    };

    match expr {
        ConditionExpression::ComparisonOp(tree) => check_tree(&tree),
        ConditionExpression::LogicalOp(tree) => check_tree(&tree),
        ConditionExpression::NegationOp(expr) => !check_expr(row, expr),
        ConditionExpression::Bracketed(expr) => check_expr(row, expr),
        _ => false,
    }
}

pub struct Filter<'a> {
    pub where_clause: &'a Option<ConditionExpression>,
}

impl Filter<'_> {
    pub fn check<'a, T: Debug>(&'a self, row: &'a Row<T>) -> bool {
        self.where_clause
            .as_ref()
            .map_or(true, |expr| check_expr(row, expr))
    }
}
