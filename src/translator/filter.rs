use crate::translator::Row;
use nom_sql::{ConditionBase, ConditionExpression, ConditionTree, Literal, Operator};
use std::convert::From;
use std::fmt::Debug;

pub struct Filter {
    where_clause: Option<ConditionExpression>,
}

impl Filter {
    fn parse_base<'a, T: Debug>(
        &self,
        row: &'a Row<T>,
        base: &'a ConditionBase,
    ) -> Option<&'a Literal> {
        match base {
            ConditionBase::Field(v) => row.get_literal(&v.name),
            ConditionBase::Literal(literal) => Some(literal),
            _ => None,
        }
    }

    fn parse_expr<'a, T: Debug>(
        &self,
        row: &'a Row<T>,
        expr: &'a ConditionExpression,
    ) -> Option<&'a Literal> {
        match expr {
            ConditionExpression::Base(base) => self.parse_base(row, &base),
            _ => None,
        }
    }

    fn check_tree<T: Debug>(&self, row: &Row<T>, tree: &ConditionTree) -> bool {
        let left = self.parse_expr(row, &tree.left);
        let right = self.parse_expr(row, &tree.right);

        match tree.operator {
            Operator::Equal => left.is_some() && right.is_some() && left == right,
            _ => true,
        }
    }

    fn check_expr<T: Debug>(&self, row: &Row<T>, expr: &ConditionExpression) -> bool {
        match expr {
            ConditionExpression::ComparisonOp(tree) => self.check_tree(row, &tree),
            _ => false,
        }
    }

    pub fn check<T: Debug>(&self, row: &Row<T>) -> bool {
        self.where_clause
            .as_ref()
            .map_or(false, |expr| self.check_expr(row, &expr))
    }
}

impl From<Option<ConditionExpression>> for Filter {
    fn from(where_clause: Option<ConditionExpression>) -> Self {
        Filter { where_clause }
    }
}
