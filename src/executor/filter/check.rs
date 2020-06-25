use boolinator::Boolinator;
use std::fmt::Debug;

use sqlparser::ast::{BinaryOperator, Expr, Value as AstValue};

use crate::executor::{select, BlendContext, FilterContext};
use crate::result::Result;
use crate::storage::Store;
// use crate::data::Value;

use super::error::FilterError;
// use super::parsed::{Parsed, ParsedList};
use super::parsed::Parsed;

/*
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
*/

fn parse_expr<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    filter_context: &'a FilterContext<'a>,
    expr: &'a Expr,
) -> Result<Parsed<'a>> {
    let parse_value = |value: &'a AstValue| match value {
        v @ AstValue::Number(_) => Ok(Parsed::LiteralRef(v)),
        _ => Err(FilterError::Unimplemented.into()),
    };

    match expr {
        Expr::Value(value) => parse_value(&value),
        Expr::Identifier(ident) => match ident.quote_style {
            Some(_) => Ok(Parsed::StringRef(&ident.value)),
            None => filter_context
                .get_value(&ident.value)
                .map(|value| Parsed::ValueRef(value)),
        },
        Expr::CompoundIdentifier(idents) => {
            if idents.len() != 2 {
                return Err(FilterError::UnsupportedCompoundIdentifier(expr.to_string()).into());
            }

            let table_alias = &idents[0].value;
            let column = &idents[1].value;

            filter_context
                .get_alias_value(table_alias, column)
                .map(|value| Parsed::ValueRef(value))
        }
        Expr::Subquery(query) => select(storage, &query, Some(filter_context))?
            .map(|row| row?.take_first_value())
            .map(|value| value.map(Parsed::Value))
            .next()
            .ok_or(FilterError::NestedSelectRowNotFound)?,
        _ => Err(FilterError::Unimplemented.into()),
    }

    /*
    let parse_base = |base: &'a ConditionBase| match base {
        ConditionBase::Field(column) => filter_context
            .get_value(&column)
            .map(|value| Parsed::ValueRef(value)),
        ConditionBase::Literal(literal) => Ok(Parsed::LiteralRef(literal)),
        ConditionBase::NestedSelect(statement) => {
            let value = select(storage, statement, Some(filter_context))?
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
    */
}

/*
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
*/

pub fn check_expr<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    filter_context: &'a FilterContext<'a>,
    expr: &'a Expr,
) -> Result<bool> {
    let parse = |expr| parse_expr(storage, filter_context, expr);

    match expr {
        Expr::BinaryOp { op, left, right } => {
            let zip_parse = || Ok((parse(left)?, parse(right)?));

            match op {
                BinaryOperator::Eq => zip_parse().map(|(l, r)| l == r),
                _ => Err(FilterError::Unimplemented.into()),
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let negated = *negated;
            let target = parse(expr)?;

            list.iter()
                .filter_map(|expr| {
                    parse(expr).map_or_else(
                        |error| Some(Err(error)),
                        |parsed| (target == parsed).as_some(Ok(!negated)),
                    )
                })
                .next()
                .unwrap_or(Ok(negated))
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let negated = *negated;
            let target = parse(expr)?;

            select(storage, &subquery, Some(filter_context))?
                .map(|row| row?.take_first_value())
                .filter_map(|value| {
                    value.map_or_else(
                        |error| Some(Err(error)),
                        |value| (target == Parsed::ValueRef(&value)).as_some(Ok(!negated)),
                    )
                })
                .next()
                .unwrap_or(Ok(negated))
        }
        _ => Err(FilterError::Unimplemented.into()),
    }

    /*
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
            _ => Err(FilterError::Unimplemented.into()),
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
        _ => Err(FilterError::Unimplemented.into()),
    }
    */
}

pub fn check_blended_expr<T: 'static + Debug>(
    storage: &dyn Store<T>,
    filter_context: Option<&FilterContext<'_>>,
    blend_context: &BlendContext<'_, T>,
    expr: &Expr,
) -> Result<bool> {
    let BlendContext {
        table_alias,
        columns,
        row,
        next,
        ..
    } = blend_context;

    let filter_context = FilterContext::new(table_alias, &columns, &row, filter_context);

    match next {
        Some(blend_context) => {
            check_blended_expr(storage, Some(&filter_context), blend_context, expr)
        }
        None => check_expr(storage, &filter_context, expr),
    }
}
