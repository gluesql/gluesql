use crate::executor::{select, Context};
use crate::row::Row;
use crate::storage::Store;
use nom_sql::{ConditionBase, ConditionExpression, ConditionTree, Literal, Operator, Table};
use std::fmt::Debug;

pub struct Filter<'a, T: 'static + Debug> {
    pub where_clause: &'a Option<ConditionExpression>,
    pub storage: &'a dyn Store<T>,
    pub context: Option<&'a Context<'a, T>>,
}

impl<T: 'static + Debug> Filter<'_, T> {
    pub fn check<'a>(&'a self, table: &'a Table, row: &'a Row<T>) -> bool {
        let context = Context {
            table,
            row,
            next: self.context,
        };

        self.where_clause
            .as_ref()
            .map_or(true, |expr| check_expr(self.storage, &context, expr))
    }
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

enum ParsedList<'a> {
    Ref(&'a Vec<Literal>),
    Val(Vec<Literal>),
}

impl<'a> ParsedList<'a> {
    fn contains(&self, target: &Parsed<'a>) -> bool {
        let target: &Literal = match target {
            Parsed::Ref(literal) => literal,
            Parsed::Val(literal) => &literal,
        };

        let literals: &Vec<Literal> = match &self {
            ParsedList::Ref(literals) => literals,
            ParsedList::Val(literals) => &literals,
        };

        literals.iter().any(|literal| literal == target)
    }
}

fn parse_expr<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    context: &'a Context<'a, T>,
    expr: &'a ConditionExpression,
) -> Option<Parsed<'a>> {
    let parse_base = |base: &'a ConditionBase| match base {
        ConditionBase::Field(v) => context.get_literal(&v).map(|literal| Parsed::Ref(literal)),
        ConditionBase::Literal(literal) => Some(Parsed::Ref(literal)),
        ConditionBase::NestedSelect(statement) => {
            let (_, literal) = select(storage, statement, Some(context))
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

fn parse_in_expr<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    context: &'a Context<'a, T>,
    expr: &'a ConditionExpression,
) -> Option<ParsedList<'a>> {
    let parse_base = |base: &'a ConditionBase| match base {
        ConditionBase::LiteralList(literals) => Some(ParsedList::Ref(literals)),
        ConditionBase::NestedSelect(statement) => {
            let literals = select(storage, statement, Some(context))
                .into_iter()
                .map(|row| row.items.into_iter().nth(0).unwrap().1)
                .collect();

            Some(ParsedList::Val(literals))
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
        let zip_parse = || Ok((parse(&tree.left), parse(&tree.right)));
        let zip_in = || Ok((parse(&tree.left), parse_in(&tree.right)));

        let result: Result<bool, ()> = match tree.operator {
            Operator::Equal => zip_parse().map(|(l, r)| l.is_some() && r.is_some() && l == r),
            Operator::NotEqual => zip_parse().map(|(l, r)| l.is_some() && r.is_some() && l != r),
            Operator::And => zip_check().map(|(l, r)| l && r),
            Operator::Or => zip_check().map(|(l, r)| l || r),
            Operator::In => zip_in()
                .map(|(l, r)| l.is_some() && r.is_some() && r.unwrap().contains(&l.unwrap())),
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
