use nom_sql::{
    ArithmeticBase, ArithmeticExpression, ArithmeticOperator, ConditionBase, ConditionExpression,
    ConditionTree, Operator,
};
use std::fmt::Debug;

use crate::executor::{fetch_select_params, select, BlendContext, FilterContext};
use crate::result::Result;
use crate::storage::Store;

use super::error::FilterError;
use super::parsed::{Parsed, ParsedList};

fn parse_arithmetic<'a>(
    filter_context: &'a FilterContext<'a>,
    expr: &'a ArithmeticExpression,
) -> Result<Parsed<'a>> {
    let parse_base = |base: &'a ArithmeticBase| -> Result<Parsed<'a>> {
        match base {
            ArithmeticBase::Column(column) => {
                let value = filter_context.get_value(&column)?;

                Ok(Parsed::ValueRef(value))
            }
            ArithmeticBase::Scalar(literal) => Ok(Parsed::LiteralRef(&literal)),
        }
    };

    let l = parse_base(&expr.left)?;
    let r = parse_base(&expr.right)?;

    match expr.op {
        ArithmeticOperator::Add => Ok(l.add(&r)?),
        ArithmeticOperator::Subtract => Ok(l.subtract(&r)?),
        ArithmeticOperator::Multiply => Ok(l.multiply(&r)?),
        ArithmeticOperator::Divide => Ok(l.divide(&r)?),
    }
}

fn parse_expr<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    filter_context: &'a FilterContext<'a>,
    expr: &'a ConditionExpression,
) -> Result<Parsed<'a>> {
    let parse_base = |base: &'a ConditionBase| match base {
        ConditionBase::Field(column) => filter_context
            .get_value(&column)
            .map(|value| Parsed::ValueRef(value)),
        ConditionBase::Literal(literal) => Ok(Parsed::LiteralRef(literal)),
        ConditionBase::NestedSelect(statement) => {
            let params = fetch_select_params(storage, statement)?;
            let value = select(storage, statement, &params, Some(filter_context))?
                .map(|row| row?.take_first_value())
                .next()
                .ok_or(FilterError::NestedSelectRowNotFound)??;

            Ok(Parsed::Value(value))
        }
        ConditionBase::LiteralList(_) => Err(FilterError::UnreachableConditionBase.into()),
    };

    let parse_arithmetic = |expr| parse_arithmetic(filter_context, expr);

    match expr {
        ConditionExpression::Base(base) => parse_base(&base),
        ConditionExpression::Arithmetic(expr) => parse_arithmetic(&expr),
        _ => Err(FilterError::Unimplemented.into()),
    }
}

fn parse_in_expr<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    filter_context: &'a FilterContext<'a>,
    expr: &'a ConditionExpression,
) -> Result<ParsedList<'a, T>> {
    let parse_base = |base: &'a ConditionBase| match base {
        ConditionBase::Field(column) => filter_context
            .get_value(&column)
            .map(|value| ParsedList::Parsed(Parsed::ValueRef(value))),
        ConditionBase::Literal(literal) => Ok(ParsedList::Parsed(Parsed::LiteralRef(literal))),
        ConditionBase::LiteralList(literals) => Ok(ParsedList::LiteralRef(literals)),
        ConditionBase::NestedSelect(statement) => Ok(ParsedList::Value {
            storage,
            statement,
            filter_context,
        }),
    };

    match expr {
        ConditionExpression::Base(base) => parse_base(&base),
        _ => Err(FilterError::Unimplemented.into()),
    }
}

pub fn check_expr<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    filter_context: &'a FilterContext<'a>,
    expr: &'a ConditionExpression,
) -> Result<bool> {
    let check = |expr| check_expr(storage, filter_context, expr);
    let parse = |expr| parse_expr(storage, filter_context, expr);
    let parse_in = |expr| parse_in_expr(storage, filter_context, expr);

    let check_tree = |tree: &'a ConditionTree| {
        let zip_check = || Ok((check(&tree.left)?, check(&tree.right)?));
        let zip_parse = || Ok((parse(&tree.left)?, parse(&tree.right)?));
        let zip_in = || Ok((parse(&tree.left)?, parse_in(&tree.right)?));

        match tree.operator {
            Operator::Equal => zip_parse().map(|(l, r)| l == r),
            Operator::NotEqual => zip_parse().map(|(l, r)| l != r),
            Operator::And => zip_check().map(|(l, r)| l && r),
            Operator::Or => zip_check().map(|(l, r)| l || r),
            Operator::In => zip_in().and_then(|(l, r)| l.exists_in(r)),
            Operator::Less => zip_parse().map(|(l, r)| l < r),
            Operator::LessOrEqual => zip_parse().map(|(l, r)| l <= r),
            Operator::Greater => zip_parse().map(|(l, r)| l > r),
            Operator::GreaterOrEqual => zip_parse().map(|(l, r)| l >= r),
            Operator::Not | Operator::Like | Operator::NotLike | Operator::Is => {
                Err(FilterError::Unimplemented.into())
            }
        }
    };

    match expr {
        ConditionExpression::ComparisonOp(tree) => check_tree(&tree),
        ConditionExpression::LogicalOp(tree) => check_tree(&tree),
        ConditionExpression::NegationOp(expr) => check(expr).map(|b| !b),
        ConditionExpression::Bracketed(expr) => check(expr),
        ConditionExpression::Arithmetic(_) | ConditionExpression::Base(_) => {
            Err(FilterError::Unimplemented.into())
        }
    }
}

pub fn check_blended_expr<T: 'static + Debug>(
    storage: &dyn Store<T>,
    filter_context: Option<&FilterContext<'_>>,
    blend_context: &BlendContext<'_, T>,
    expr: &ConditionExpression,
) -> Result<bool> {
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
            check_blended_expr(storage, Some(&filter_context), blend_context, expr)
        }
        None => check_expr(storage, &filter_context, expr),
    }
}
