use {
    super::{
        ast_literal::translate_trim_where_field, expr::translate_expr, translate_object_name,
        TranslateError,
    },
    crate::{
        ast::{Aggregate, CountArgExpr, Expr, Function},
        result::Result,
    },
    sqlparser::ast::{
        Expr as SqlExpr, Function as SqlFunction, FunctionArg as SqlFunctionArg,
        FunctionArgExpr as SqlFunctionArgExpr, TrimWhereField as SqlTrimWhereField,
    },
};

pub fn translate_trim(
    expr: &SqlExpr,
    trim_where: &Option<SqlTrimWhereField>,
    trim_what: &Option<Box<SqlExpr>>,
) -> Result<Expr> {
    let expr = translate_expr(expr)?;
    let trim_where_field = trim_where.as_ref().map(translate_trim_where_field);
    let filter_chars = trim_what
        .as_ref()
        .map(|expr| translate_expr(expr.as_ref()))
        .transpose()?;

    Ok(Expr::Function(Box::new(Function::Trim {
        expr,
        filter_chars,
        trim_where_field,
    })))
}

pub fn translate_positon(sub_expr: &SqlExpr, from_expr: &SqlExpr) -> Result<Expr> {
    let sub_expr = translate_expr(sub_expr)?;
    let from_expr = translate_expr(from_expr)?;
    Ok(Expr::Function(Box::new(Function::Position {
        sub_expr,
        from_expr,
    })))
}

fn check_len(name: String, found: usize, expected: usize) -> Result<()> {
    if found == expected {
        Ok(())
    } else {
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name,
            found,
            expected,
        }
        .into())
    }
}

fn check_len_range(
    name: String,
    found: usize,
    expected_minimum: usize,
    expected_maximum: usize,
) -> Result<()> {
    if found >= expected_minimum && found <= expected_maximum {
        Ok(())
    } else {
        Err(TranslateError::FunctionArgsLengthNotWithinRange {
            name,
            expected_minimum,
            expected_maximum,
            found,
        }
        .into())
    }
}

fn check_len_min(name: String, found: usize, expected_minimum: usize) -> Result<()> {
    if found >= expected_minimum {
        Ok(())
    } else {
        Err(TranslateError::FunctionArgsLengthNotMatchingMin {
            name,
            expected_minimum,
            found,
        }
        .into())
    }
}

fn translate_function_zero_arg(func: Function, args: Vec<&SqlExpr>, name: String) -> Result<Expr> {
    check_len(name, args.len(), 0)?;

    Ok(Expr::Function(Box::new(func)))
}

fn translate_function_one_arg<T: FnOnce(Expr) -> Function>(
    func: T,
    args: Vec<&SqlExpr>,
    name: String,
) -> Result<Expr> {
    check_len(name, args.len(), 1)?;

    translate_expr(args[0])
        .map(func)
        .map(Box::new)
        .map(Expr::Function)
}

fn translate_aggregate_one_arg<T: FnOnce(Expr) -> Aggregate>(
    func: T,
    args: Vec<&SqlExpr>,
    name: String,
) -> Result<Expr> {
    check_len(name, args.len(), 1)?;

    translate_expr(args[0])
        .map(func)
        .map(Box::new)
        .map(Expr::Aggregate)
}

fn translate_function_trim<T: FnOnce(Expr, Option<Expr>) -> Function>(
    func: T,
    args: Vec<&SqlExpr>,
    name: String,
) -> Result<Expr> {
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

pub fn translate_function_arg_exprs(
    function_arg_exprs: Vec<&SqlFunctionArgExpr>,
) -> Result<Vec<&SqlExpr>> {
    function_arg_exprs
        .into_iter()
        .map(|function_arg| match function_arg {
            SqlFunctionArgExpr::Expr(expr) => Ok(expr),
            SqlFunctionArgExpr::Wildcard | SqlFunctionArgExpr::QualifiedWildcard(_) => {
                Err(TranslateError::WildcardFunctionArgNotAccepted.into())
            }
        })
        .collect::<Result<Vec<_>>>()
}

pub fn translate_function(sql_function: &SqlFunction) -> Result<Expr> {
    let SqlFunction { name, args, .. } = sql_function;
    let name = translate_object_name(name)?.to_uppercase();

    let function_arg_exprs = args
        .iter()
        .map(|arg| match arg {
            SqlFunctionArg::Named { .. } => {
                Err(TranslateError::NamedFunctionArgNotSupported.into())
            }
            SqlFunctionArg::Unnamed(arg_expr) => Ok(arg_expr),
        })
        .collect::<Result<Vec<_>>>()?;

    if name.as_str() == "COUNT" {
        check_len(name, args.len(), 1)?;

        let count_arg = match function_arg_exprs[0] {
            SqlFunctionArgExpr::Expr(expr) => CountArgExpr::Expr(translate_expr(expr)?),
            SqlFunctionArgExpr::QualifiedWildcard(idents) => {
                let table_name = translate_object_name(idents)?;
                let idents = format!("{}.*", table_name);

                return Err(TranslateError::QualifiedWildcardInCountNotSupported(idents).into());
            }
            SqlFunctionArgExpr::Wildcard => CountArgExpr::Wildcard,
        };

        return Ok(Expr::Aggregate(Box::new(Aggregate::Count(count_arg))));
    }

    let args = translate_function_arg_exprs(function_arg_exprs)?;

    match name.as_str() {
        "SUM" => translate_aggregate_one_arg(Aggregate::Sum, args, name),
        "MIN" => translate_aggregate_one_arg(Aggregate::Min, args, name),
        "MAX" => translate_aggregate_one_arg(Aggregate::Max, args, name),
        "AVG" => translate_aggregate_one_arg(Aggregate::Avg, args, name),
        "VARIANCE" => translate_aggregate_one_arg(Aggregate::Variance, args, name),
        "STDEV" => translate_aggregate_one_arg(Aggregate::Stdev, args, name),
        "CONCAT" => {
            check_len_min(name, args.len(), 1)?;
            let exprs = args
                .into_iter()
                .map(translate_expr)
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::Function(Box::new(Function::Concat(exprs))))
        }
        "LOWER" => translate_function_one_arg(Function::Lower, args, name),
        "UPPER" => translate_function_one_arg(Function::Upper, args, name),
        "LEFT" => {
            check_len(name, args.len(), 2)?;

            let expr = translate_expr(args[0])?;
            let size = translate_expr(args[1])?;

            Ok(Expr::Function(Box::new(Function::Left { expr, size })))
        }
        "IFNULL" => {
            check_len(name, args.len(), 2)?;
            let expr = translate_expr(args[0])?;
            let then = translate_expr(args[1])?;
            Ok(Expr::Function(Box::new(Function::IfNull { expr, then })))
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
        "LOG" => {
            check_len(name, args.len(), 2)?;

            let antilog = translate_expr(args[0])?;
            let base = translate_expr(args[1])?;

            Ok(Expr::Function(Box::new(Function::Log { antilog, base })))
        }
        "LOG2" => translate_function_one_arg(Function::Log2, args, name),
        "LOG10" => translate_function_one_arg(Function::Log10, args, name),
        "SIN" => translate_function_one_arg(Function::Sin, args, name),
        "COS" => translate_function_one_arg(Function::Cos, args, name),
        "TAN" => translate_function_one_arg(Function::Tan, args, name),
        "ASIN" => translate_function_one_arg(Function::Asin, args, name),
        "ACOS" => translate_function_one_arg(Function::Acos, args, name),
        "ATAN" => translate_function_one_arg(Function::Atan, args, name),
        "RADIANS" => translate_function_one_arg(Function::Radians, args, name),
        "DEGREES" => translate_function_one_arg(Function::Degrees, args, name),
        "PI" => translate_function_zero_arg(Function::Pi(), args, name),
        "NOW" => translate_function_zero_arg(Function::Now(), args, name),
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
        "LTRIM" => {
            translate_function_trim(|expr, chars| Function::Ltrim { expr, chars }, args, name)
        }
        "RTRIM" => {
            translate_function_trim(|expr, chars| Function::Rtrim { expr, chars }, args, name)
        }
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
        "REPEAT" => {
            check_len(name, args.len(), 2)?;

            let expr = translate_expr(args[0])?;
            let num = translate_expr(args[1])?;

            Ok(Expr::Function(Box::new(Function::Repeat { expr, num })))
        }
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
        "UNWRAP" => {
            check_len(name, args.len(), 2)?;

            let expr = translate_expr(args[0])?;
            let selector = translate_expr(args[1])?;

            Ok(Expr::Function(Box::new(Function::Unwrap {
                expr,
                selector,
            })))
        }
        "ABS" => translate_function_one_arg(Function::Abs, args, name),
        "SIGN" => translate_function_one_arg(Function::Sign, args, name),
        "GENERATE_UUID" => translate_function_zero_arg(Function::GenerateUuid(), args, name),
        "FORMAT" => {
            check_len(name, args.len(), 2)?;

            let expr = translate_expr(args[0])?;
            let format = translate_expr(args[1])?;

            Ok(Expr::Function(Box::new(Function::Format { expr, format })))
        }
        "TO_DATE" => {
            check_len(name, args.len(), 2)?;

            let expr = translate_expr(args[0])?;
            let format = translate_expr(args[1])?;

            Ok(Expr::Function(Box::new(Function::ToDate { expr, format })))
        }

        "TO_TIMESTAMP" => {
            check_len(name, args.len(), 2)?;

            let expr = translate_expr(args[0])?;
            let format = translate_expr(args[1])?;

            Ok(Expr::Function(Box::new(Function::ToTimestamp {
                expr,
                format,
            })))
        }
        "TO_TIME" => {
            check_len(name, args.len(), 2)?;

            let expr = translate_expr(args[0])?;
            let format = translate_expr(args[1])?;

            Ok(Expr::Function(Box::new(Function::ToTime { expr, format })))
        }

        _ => Err(TranslateError::UnsupportedFunction(name).into()),
    }
}
