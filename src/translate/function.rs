use {
    super::{expr::translate_expr, translate_object_name, TranslateError},
    crate::{
        ast::{Aggregate, Expr, Function, ObjectName},
        result::Result,
    },
    sqlparser::ast::{Function as SqlFunction, FunctionArg as SqlFunctionArg},
};

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

    let check_len = |name, found, expected| -> Result<_> {
        if found == expected {
            Ok(())
        } else {
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name,
                expected,
                found,
            }
            .into())
        }
    };

    let check_len_range = |name, found, expected_minimum, expected_maximum| -> Result<_> {
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
    };

    macro_rules! aggr {
        ($aggregate: expr) => {{
            check_len(name, args.len(), 1)?;

            translate_expr(args[0])
                .map($aggregate)
                .map(Box::new)
                .map(Expr::Aggregate)
        }};
    }

    macro_rules! func_with_one_arg {
        ($func: expr) => {{
            check_len(name, args.len(), 1)?;

            translate_expr(args[0])
                .map($func)
                .map(Box::new)
                .map(Expr::Function)
        }};
    }

    match name.as_str() {
        "LOWER" => func_with_one_arg!(Function::Lower),
        "UPPER" => func_with_one_arg!(Function::Upper),
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
        "LPAD" => {
            check_len_range(name, args.len(), 2, 3)?;

            let expr = translate_expr(args[0])?;
            let size = translate_expr(args[1])?;
            let fill = if args.len() == 2 {
                None
            } else {
                Some(translate_expr(args[2])?)
            };

            Ok(Expr::Function(Box::new(Function::Lpad { expr, size, fill })))
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

            Ok(Expr::Function(Box::new(Function::Rpad { expr, size, fill })))
        }
        "CEIL" => func_with_one_arg!(Function::Ceil),
        "ROUND" => func_with_one_arg!(Function::Round),
        "FLOOR" => func_with_one_arg!(Function::Floor),
        "SIN" => func_with_one_arg!(Function::Sin),
        "COS" => func_with_one_arg!(Function::Cos),
        "TAN" => func_with_one_arg!(Function::Tan),
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
        "COUNT" => aggr!(Aggregate::Count),
        "SUM" => aggr!(Aggregate::Sum),
        "MIN" => aggr!(Aggregate::Min),
        "MAX" => aggr!(Aggregate::Max),
        "TRIM" => func_with_one_arg!(Function::Trim),
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
        _ => Err(TranslateError::UnsupportedFunction(name).into()),
    }
}
