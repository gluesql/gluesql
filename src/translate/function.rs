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

    macro_rules! func_with_two_arg {
        ($func: ident) => {{
            check_len_range(stringify!($func).to_owned(), args.len(), 1, 2)?;
            let expr = translate_expr(args[0])?;
            let chars = if args.len() == 1 {
                None
            } else {
                Some(translate_expr(args[1])?)
            };
            Ok(Expr::Function(Box::new(Function::$func { expr, chars })))
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
        "CEIL" => func_with_one_arg!(Function::Ceil),
        "ROUND" => func_with_one_arg!(Function::Round),
        "FLOOR" => func_with_one_arg!(Function::Floor),
        "EXP" => func_with_one_arg!(Function::Exp),
        "LN" => func_with_one_arg!(Function::Ln),
        "LOG2" => func_with_one_arg!(Function::Log2),
        "LOG10" => func_with_one_arg!(Function::Log10),
        "SIN" => func_with_one_arg!(Function::Sin),
        "COS" => func_with_one_arg!(Function::Cos),
        "TAN" => func_with_one_arg!(Function::Tan),
        "ASIN" => func_with_one_arg!(Function::ASin),
        "ACOS" => func_with_one_arg!(Function::ACos),
        "ATAN" => func_with_one_arg!(Function::ATan),
        "RADIANS" => func_with_one_arg!(Function::Radians),
        "DEGREES" => func_with_one_arg!(Function::Degrees),
        "PI" => {
            check_len(name, args.len(), 0)?;

            Ok(Expr::Function(Box::new(Function::Pi())))
        }
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
        "LTRIM" => func_with_two_arg!(Ltrim),
        "RTRIM" => func_with_two_arg!(Rtrim),
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
        "REVERSE" => func_with_one_arg!(Function::Reverse),
        _ => Err(TranslateError::UnsupportedFunction(name).into()),
    }
}
