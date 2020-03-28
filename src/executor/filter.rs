use nom_sql::{
    Column, ConditionBase, ConditionExpression, ConditionTree, Literal, Operator, SelectStatement,
    Table,
};
use std::fmt::Debug;

use crate::data::{Row, Value};
use crate::executor::{fetch_select_params, select, BlendContext, FilterContext};
use crate::storage::Store;
use crate::Result;

pub struct Filter<'a, T: 'static + Debug> {
    storage: &'a dyn Store<T>,
    where_clause: Option<&'a ConditionExpression>,
    context: Option<&'a FilterContext<'a>>,
}

impl<'a, T: 'static + Debug> Filter<'a, T> {
    pub fn new(
        storage: &'a dyn Store<T>,
        where_clause: Option<&'a ConditionExpression>,
        context: Option<&'a FilterContext<'a>>,
    ) -> Self {
        Self {
            storage,
            where_clause,
            context,
        }
    }

    pub fn check(&self, table: &Table, columns: &Vec<Column>, row: &Row) -> bool {
        let context = FilterContext::new(table, columns, row, self.context);

        self.where_clause
            .map_or(true, |expr| check_expr(self.storage, &context, expr))
    }
}

pub struct BlendedFilter<'a, T: 'static + Debug> {
    filter: &'a Filter<'a, T>,
    context: &'a BlendContext<'a, T>,
}

impl<'a, T: 'static + Debug> BlendedFilter<'a, T> {
    pub fn new(filter: &'a Filter<'a, T>, context: &'a BlendContext<'a, T>) -> Self {
        Self { filter, context }
    }

    fn check_expr(
        storage: &dyn Store<T>,
        filter_context: Option<&FilterContext<'_>>,
        blend_context: &BlendContext<'_, T>,
        expr: &ConditionExpression,
    ) -> bool {
        let BlendContext {
            table,
            columns,
            row,
            next,
            ..
        } = blend_context;

        let filter_context = FilterContext::new(table, &columns, &row, filter_context);

        match next {
            Some(blend_context) => {
                Self::check_expr(storage, Some(&filter_context), blend_context, expr)
            }
            None => check_expr(storage, &filter_context, expr),
        }
    }

    pub fn check(&self, item: Option<(&Table, &Vec<Column>, &Row)>) -> Result<bool> {
        let BlendedFilter {
            filter:
                Filter {
                    storage,
                    where_clause,
                    context: next,
                },
            context: blend_context,
        } = self;

        let c;
        let filter_context = match item {
            Some((table, columns, row)) => {
                c = FilterContext::new(table, columns, row, *next);

                Some(&c)
            }
            None => *next,
        };

        Ok(where_clause.map_or(true, |expr| {
            Self::check_expr(*storage, filter_context, blend_context, expr)
        }))
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
    fn exists_in<T: 'static + Debug>(&self, list: ParsedList<'_, T>) -> bool {
        match list {
            ParsedList::LiteralRef(literals) => literals
                .iter()
                .any(|literal| &Parsed::LiteralRef(&literal) == self),
            ParsedList::Value(storage, statement, filter_context) => {
                let params = fetch_select_params(storage, statement).unwrap();
                let v = select(storage, statement, &params, Some(filter_context))
                    .unwrap()
                    .map(|c| c.unwrap())
                    .map(Row::take_first_value)
                    .map(|value| value.unwrap())
                    .any(|value| &Parsed::Value(value) == self);

                v
            }
        }
    }
}

enum ParsedList<'a, T: 'static + Debug> {
    LiteralRef(&'a Vec<Literal>),
    Value(&'a dyn Store<T>, &'a SelectStatement, &'a FilterContext<'a>),
}

fn parse_expr<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    filter_context: &'a FilterContext<'a>,
    expr: &'a ConditionExpression,
) -> Option<Parsed<'a>> {
    let parse_base = |base: &'a ConditionBase| match base {
        ConditionBase::Field(column) => filter_context
            .get_value(&column)
            .map(|value| Parsed::ValueRef(value)),
        ConditionBase::Literal(literal) => Some(Parsed::LiteralRef(literal)),
        ConditionBase::NestedSelect(statement) => {
            let params = fetch_select_params(storage, statement).unwrap();
            let value = select(storage, statement, &params, Some(filter_context))
                .unwrap()
                .map(|c| c.unwrap())
                .map(Row::take_first_value)
                .next()
                .expect("Row does not exist")
                .expect("Failed to take first value");

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
    filter_context: &'a FilterContext<'a>,
    expr: &'a ConditionExpression,
) -> Option<ParsedList<'a, T>> {
    let parse_base = |base: &'a ConditionBase| match base {
        ConditionBase::LiteralList(literals) => Some(ParsedList::LiteralRef(literals)),
        ConditionBase::NestedSelect(statement) => {
            Some(ParsedList::Value(storage, statement, filter_context))
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
    filter_context: &'a FilterContext<'a>,
    expr: &'a ConditionExpression,
) -> bool {
    let check = |expr| check_expr(storage, filter_context, expr);
    let parse = |expr| parse_expr(storage, filter_context, expr);
    let parse_in = |expr| parse_in_expr(storage, filter_context, expr);

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

        let result: std::result::Result<bool, ()> = match tree.operator {
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
