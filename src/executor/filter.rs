use crate::data::{Row, Value};
use crate::executor::{select, Context};
use crate::storage::Store;
use nom_sql::{
    Column, ConditionBase, ConditionExpression, ConditionTree, Literal, Operator, Table,
};
use std::fmt::Debug;

pub struct Filter<'a, T: 'static + Debug> {
    pub where_clause: &'a Option<ConditionExpression>,
    pub storage: &'a dyn Store<T>,
    pub context: Option<&'a Context<'a, T>>,
}

impl<T: 'static + Debug> Filter<'_, T> {
    pub fn check<'a>(
        &'a self,
        table: &'a Table,
        columns: &'a Vec<Column>,
        row: &'a Row<T>,
    ) -> bool {
        let context = Context {
            table,
            row,
            columns,
            next: self.context,
        };

        self.where_clause
            .as_ref()
            .map_or(true, |expr| check_expr(self.storage, &context, expr))
    }
}

enum Parsed<'a> {
    LiteralRef(&'a Literal),
    ValueRef(&'a Value),
    Value(Value),
}

impl<'a> PartialEq for Parsed<'a> {
    fn eq(&self, other: &Parsed<'a>) -> bool {
        use Parsed::*;

        match (self, other) {
            (LiteralRef(lr), LiteralRef(lr2)) => lr == lr2,
            (LiteralRef(lr), ValueRef(vr)) => vr == lr,
            (LiteralRef(lr), Value(v)) => &v == lr,
            (Value(v), LiteralRef(lr)) => &v == lr,
            (Value(v), ValueRef(vr)) => &v == vr,
            (Value(v), Value(v2)) => v == v2,
            (ValueRef(vr), LiteralRef(lr)) => vr == lr,
            (ValueRef(vr), ValueRef(vr2)) => vr == vr2,
            (ValueRef(vr), Value(v)) => &v == vr,
        }
    }
}

impl Parsed<'_> {
    fn exists_in(&self, list: ParsedList<'_>) -> bool {
        match list {
            ParsedList::LiteralRef(literals) => literals
                .iter()
                .any(|literal| &Parsed::LiteralRef(&literal) == self),
            ParsedList::Value(values) => values
                .into_iter()
                .any(|value| &Parsed::Value(value) == self),
        }
    }
}

enum ParsedList<'a> {
    LiteralRef(&'a Vec<Literal>),
    Value(Box<dyn Iterator<Item = Value> + 'a>),
}

fn parse_expr<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    context: &'a Context<'a, T>,
    expr: &'a ConditionExpression,
) -> Option<Parsed<'a>> {
    let parse_base = |base: &'a ConditionBase| match base {
        ConditionBase::Field(column) => context
            .get_value(&column)
            .map(|value| Parsed::ValueRef(value)),
        ConditionBase::Literal(literal) => Some(Parsed::LiteralRef(literal)),
        ConditionBase::NestedSelect(statement) => {
            let first_row = select(storage, statement, Some(context)).nth(0).unwrap();
            let value = Row::take_first_value(first_row).unwrap();

            Some(Parsed::Value(value))
        }
        _ => None,
    };

    match expr {
        ConditionExpression::Base(base) => parse_base(&base),
        _ => None,
    }
}

fn parse_in_expr<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    context: &'a Context<'a, T>,
    expr: &'a ConditionExpression,
) -> Option<ParsedList<'a>> {
    let parse_base = |base: &'a ConditionBase| match base {
        ConditionBase::LiteralList(literals) => Some(ParsedList::LiteralRef(literals)),
        ConditionBase::NestedSelect(statement) => {
            let values = select(storage, statement, Some(context))
                .map(Row::take_first_value)
                .map(|value| value.unwrap());
            let values = Box::new(values);

            Some(ParsedList::Value(values))
        }
        _ => None,
    };

    match expr {
        ConditionExpression::Base(base) => parse_base(&base),
        _ => None,
    }
}

fn check_expr<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    context: &'a Context<'a, T>,
    expr: &'a ConditionExpression,
) -> bool {
    let check = |expr| check_expr(storage, context, expr);
    let parse = |expr| parse_expr(storage, context, expr);
    let parse_in = |expr| parse_in_expr(storage, context, expr);

    let check_tree = |tree: &'a ConditionTree| {
        let zip_check = || Ok((check(&tree.left), check(&tree.right)));
        let zip_parse = || match (parse(&tree.left), parse(&tree.right)) {
            (Some(l), Some(r)) => Ok((l, r)),
            _ => Err(()),
        };
        let zip_in = || match (parse(&tree.left), parse_in(&tree.right)) {
            (Some(l), Some(r)) => Ok((l, r)),
            _ => Err(()),
        };

        let result: Result<bool, ()> = match tree.operator {
            Operator::Equal => zip_parse().map(|(l, r)| l == r),
            Operator::NotEqual => zip_parse().map(|(l, r)| l != r),
            Operator::And => zip_check().map(|(l, r)| l && r),
            Operator::Or => zip_check().map(|(l, r)| l || r),
            Operator::In => zip_in().map(|(l, r)| l.exists_in(r)),
            _ => Ok(false),
        };

        result.unwrap()
    };

    match expr {
        ConditionExpression::ComparisonOp(tree) => check_tree(&tree),
        ConditionExpression::LogicalOp(tree) => check_tree(&tree),
        ConditionExpression::NegationOp(expr) => !check(expr),
        ConditionExpression::Bracketed(expr) => check(expr),
        _ => false,
    }
}
