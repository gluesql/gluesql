use {
    super::{
        ast_literal::translate_trim_where_field, expr::translate_expr, translate_object_name,
        TranslateError,
    },
    crate::{
        ast::{Aggregate, Expr, Function, ObjectName, TrimWhereField},
        result::Result,
    },
    sqlparser::ast::{
        Expr as SqlExpr, Function as SqlFunction, FunctionArg as SqlFunctionArg,
        TrimWhereField as SqlTrimWhereField,
    },
};

pub fn translate_trim(
    expr: &SqlExpr,
    trim_where: &Option<(SqlTrimWhereField, Box<SqlExpr>)>,
) -> Result<Expr> {
    let expr = translate_expr(expr)?;
    let trim_where = trim_where
        .as_ref()
        .map(
            |(trim_where_field, expr)| -> Result<(TrimWhereField, Expr)> {
                Ok((
                    translate_trim_where_field(trim_where_field),
                    translate_expr(expr)?,
                ))
            },
        )
        .transpose()?;
    let (filter_chars, trim_where_field) = match trim_where {
        Some((trim_where_field, filter_chars)) => (Some(filter_chars), Some(trim_where_field)),
        None => (None, None),
    };

    Ok(Expr::Function(Box::new(Function::Trim {
        expr,
        filter_chars,
        trim_where_field,
    })))
}

fn check_len(name: String, found: usize, expected: usize) -> Result<()> {
    if found == expected {
        Ok(())
    }
    else {
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name,
            found,
            expected,
        }
        .into())
    }
}

fn check_len_range(name: String, found:usize, expected_minimum: usize, expected_maximum:usize) -> Result<()> {
    if found >= expected_minimum && found <= expected_maximum {
        Ok(())
    }
    else {
        Err(TranslateError::FunctionArgsLengthNotWithinRange {
            name,
            expected_minimum,
            expected_maximum,
            found,
        }
        .into())
    }
}

fn translate_function_zero_arg(func: Function, args: Vec<&SqlExpr>, name: String) -> Result<Expr> {
    check_len(name, args.len(), 0)?;

    Ok(Expr::Function(Box::new(func)))
}

fn translate_function_one_arg<T: FnOnce(Expr) -> Function>(func: T, args: Vec<&SqlExpr>, name: String) -> Result<Expr> {
    check_len(name, args.len(), 1)?;

    translate_expr(args[0])
        .map(func)
        .map(Box::new)
        .map(Expr::Function)
}

fn translate_aggrecate_one_arg<T: FnOnce(Expr) -> Aggregate>(func: T, args: Vec<&SqlExpr>, name: String) -> Result<Expr> {
    check_len(name, args.len(), 1)?;

    translate_expr(args[0])
        .map(func)
        .map(Box::new)
        .map(Expr::Aggregate)
}

fn translate_function_range<T: FnOnce(Expr, Option<Expr>) -> Function>(func: T, args: Vec<&SqlExpr>, name: String) -> Result<Expr> {
    check_len_range(name, args.len(), 1, 2)?;

    let expr = translate_expr(args[0])?;
    let chars = if args.len() == 1 {
        None
    } else {
        Some(translate_expr(args[1])?)
    };

    let result = func(expr, chars);

    Ok(Expr::Function(Box::new(result)))
}

pub fn translate_function(sql_function: &SqlFunction) -> Result<Expr> {
    let SqlFunction { name, args, .. } = sql_function;
    let name = {
        let ObjectName(names) = translate_object_name(name);

        names[0].to_uppercase()
    };
    let args = args
        .iter()
        .map(|arg| match arg {
            SqlFunctionArg::Named { .. } => {
                Err(TranslateError::NamedFunctionArgNotSupported.into())
            }
            SqlFunctionArg::Unnamed(expr) => Ok(expr),
        })
        .collect::<Result<Vec<_>>>()?;

    match name.as_str() {
        "LOWER" => translate_function_one_arg(Function::Lower, args, name),
        "UPPER" => translate_function_one_arg(Function::Upper, args, name),
        "LEFT" => {
            check_len(name, args.len(), 2)?;

            let expr = translate_expr(args[0])?;
            let size = translate_expr(args[1])?;

            Ok(Expr::Function(Box::new(Function::Left { expr, size })))
        }
        "RIGHT" => {
            check_len(name, args.len(), 2)?;

            let expr = translate_expr(args[0])?;
            let size = translate_expr(args[1])?;

            Ok(Expr::Function(Box::new(Function::Right { expr, size })))
        }
        "SQRT" => {
            check_len(name, args.len(), 1)?;

            translate_expr(args[0])
                .map(Function::Sqrt)
                .map(Box::new)
                .map(Expr::Function)
        }
        "POWER" => {
            check_len(name, args.len(), 2)?;

            let expr = translate_expr(args[0])?;
            let power = translate_expr(args[1])?;

            Ok(Expr::Function(Box::new(Function::Power { expr, power })))
        }
        "LPAD" => {
            check_len_range(name, args.len(), 2, 3)?;

            let expr = translate_expr(args[0])?;
            let size = translate_expr(args[1])?;
            let fill = if args.len() == 2 {
                None
            } else {
                Some(translate_expr(args[2])?)
            };

            Ok(Expr::Function(Box::new(Function::Lpad {
                expr,
                size,
                fill,
            })))
        }
        "RPAD" => {
            check_len_range(name, args.len(), 2, 3)?;

            let expr = translate_expr(args[0])?;
            let size = translate_expr(args[1])?;
            let fill = if args.len() == 2 {
                None
            } else {
                Some(translate_expr(args[2])?)
            };

            Ok(Expr::Function(Box::new(Function::Rpad {
                expr,
                size,
                fill,
            })))
        }
        "CEIL" => translate_function_one_arg(Function::Ceil, args, name),
        "ROUND" => translate_function_one_arg(Function::Round, args, name),
        "FLOOR" => translate_function_one_arg(Function::Floor, args, name),
        "EXP" => translate_function_one_arg(Function::Exp, args, name),
        "LN" => translate_function_one_arg(Function::Ln, args, name),
        "LOG2" => translate_function_one_arg(Function::Log2, args, name),
        "LOG10" => translate_function_one_arg(Function::Log10, args, name),
        "SIN" => translate_function_one_arg(Function::Sin, args, name),
        "COS" => translate_function_one_arg(Function::Cos, args, name),
        "TAN" => translate_function_one_arg(Function::Tan, args, name),
        "ASIN" => translate_function_one_arg(Function::ASin, args, name),
        "ACOS" => translate_function_one_arg(Function::ACos, args, name),
        "ATAN" => translate_function_one_arg(Function::ATan, args, name),
        "RADIANS" => translate_function_one_arg(Function::Radians, args, name),
        "DEGREES" => translate_function_one_arg(Function::Degrees, args, name),
        "PI" => translate_function_zero_arg(Function::Pi(), args, name),
        "GCD" => {
            check_len(name, args.len(), 2)?;

            let left = translate_expr(args[0])?;
            let right = translate_expr(args[1])?;

            Ok(Expr::Function(Box::new(Function::Gcd { left, right })))
        }
        "LCM" => {
            check_len(name, args.len(), 2)?;

            let left = translate_expr(args[0])?;
            let right = translate_expr(args[1])?;

            Ok(Expr::Function(Box::new(Function::Lcm { left, right })))
        }
        "LTRIM" => translate_function_range(|expr, chars| Function::Ltrim{expr, chars}, args, name),
        "RTRIM" => translate_function_range(|expr, chars| Function::Rtrim{expr, chars}, args, name),
        "COUNT" => translate_aggrecate_one_arg(Aggregate::Count, args, name),
        "SUM" => translate_aggrecate_one_arg(Aggregate::Sum, args, name),
        "MIN" => translate_aggrecate_one_arg(Aggregate::Min, args, name),
        "MAX" => translate_aggrecate_one_arg(Aggregate::Max, args, name),
        "DIV" => {
            check_len(name, args.len(), 2)?;

            let dividend = translate_expr(args[0])?;
            let divisor = translate_expr(args[1])?;

            Ok(Expr::Function(Box::new(Function::Div {
                dividend,
                divisor,
            })))
        }
        "MOD" => {
            check_len(name, args.len(), 2)?;

            let dividend = translate_expr(args[0])?;
            let divisor = translate_expr(args[1])?;

            Ok(Expr::Function(Box::new(Function::Mod {
                dividend,
                divisor,
            })))
        }
        "REVERSE" => translate_function_one_arg(Function::Reverse, args, name),
        "SUBSTR" => {
            check_len_range(name, args.len(), 2, 3)?;

            let expr = translate_expr(args[0])?;
            let start = translate_expr(args[1])?;
            let count = (args.len() > 2)
                .then(|| translate_expr(args[2]))
                .transpose()?;

            Ok(Expr::Function(Box::new(Function::Substr {
                expr,
                start,
                count,
            })))
        }
        _ => Err(TranslateError::UnsupportedFunction(name).into()),
    }
}
