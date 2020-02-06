use crate::executor::select;
use crate::row::Row;
use crate::storage::Store;
use nom_sql::{ConditionBase, ConditionExpression, ConditionTree, Literal, Operator};
use std::fmt::Debug;

pub struct Filter<'a, T: 'static + Debug> {
    pub where_clause: &'a Option<ConditionExpression>,
    pub storage: &'a dyn Store<T>,
}

impl<T: 'static + Debug> Filter<'_, T> {
    pub fn check<'a>(&'a self, row: &'a Row<T>) -> bool {
        let context = Context {
            row,
            storage: self.storage,
        };

        self.where_clause
            .as_ref()
            .map_or(true, |expr| check_expr(&context, expr))
    }
}

struct Context<'a, T: Debug> {
    storage: &'a dyn Store<T>,
    row: &'a Row<T>,
}

enum Parsed<'a> {
    Ref(&'a Literal),
    Val(Literal),
}

impl<'a> PartialEq for Parsed<'a> {
    fn eq(&self, other: &Parsed<'a>) -> bool {
        fn get<'a>(p: &'a Parsed<'a>) -> &'a Literal {
            match p {
                Parsed::Ref(p) => p,
                Parsed::Val(p) => &p,
            }
        }

        get(self) == get(other)
    }
}

fn parse_expr<'a, T: 'static + Debug>(
    context: &'a Context<'a, T>,
    expr: &'a ConditionExpression,
) -> Option<Parsed<'a>> {
    let parse_base = |base: &'a ConditionBase| match base {
        ConditionBase::Field(v) => context
            .row
            .get_literal(&v.name)
            .map(|literal| Parsed::Ref(literal)),
        ConditionBase::Literal(literal) => Some(Parsed::Ref(literal)),
        ConditionBase::NestedSelect(statement) => {
            let (_, literal) = select(context.storage, statement)
                .into_iter()
                .nth(0)
                .unwrap()
                .items
                .into_iter()
                .nth(0)
                .unwrap();

            Some(Parsed::Val(literal))
        }
        _ => None,
    };

    match expr {
        ConditionExpression::Base(base) => parse_base(&base),
        _ => None,
    }
}

fn check_expr<'a, T: 'static + Debug>(
    context: &'a Context<'a, T>,
    expr: &'a ConditionExpression,
) -> bool {
    let check_tree = |tree: &'a ConditionTree| {
        let check = || {
            Ok((
                check_expr(context, &tree.left),
                check_expr(context, &tree.right),
            ))
        };
        let parse = || {
            Ok((
                parse_expr(context, &tree.left),
                parse_expr(context, &tree.right),
            ))
        };

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
        ConditionExpression::NegationOp(expr) => !check_expr(context, expr),
        ConditionExpression::Bracketed(expr) => check_expr(context, expr),
        _ => false,
    }
}
