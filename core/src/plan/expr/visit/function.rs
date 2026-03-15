use crate::{
    ast::{Expr, Function},
    plan::PlanError,
};

use super::{try_visit_expr, visit_mut_expr};

macro_rules! apply_mut {
    ($visit:expr) => {
        $visit
    };
}

macro_rules! apply_try {
    ($visit:expr) => {
        $visit?
    };
}

macro_rules! visit_function_children {
    ($func:expr, $visit_expr:ident, $f:expr, $apply:ident) => {
        match $func {
            Function::Abs(expr)
            | Function::Lower(expr)
            | Function::Initcap(expr)
            | Function::Upper(expr)
            | Function::Asin(expr)
            | Function::Acos(expr)
            | Function::Atan(expr)
            | Function::Ceil(expr)
            | Function::Round(expr)
            | Function::Trunc(expr)
            | Function::Floor(expr)
            | Function::Exp(expr)
            | Function::Ln(expr)
            | Function::Log2(expr)
            | Function::Log10(expr)
            | Function::Sin(expr)
            | Function::Cos(expr)
            | Function::Tan(expr)
            | Function::Sqrt(expr)
            | Function::Radians(expr)
            | Function::Degrees(expr)
            | Function::LastDay(expr)
            | Function::Reverse(expr)
            | Function::Sign(expr)
            | Function::Ascii(expr)
            | Function::Chr(expr)
            | Function::Md5(expr)
            | Function::Hex(expr)
            | Function::IsEmpty(expr)
            | Function::Length(expr)
            | Function::Entries(expr)
            | Function::Keys(expr)
            | Function::Values(expr)
            | Function::Dedup(expr)
            | Function::GetX(expr)
            | Function::GetY(expr) => $apply!($visit_expr(expr, $f)),
            Function::AddMonth { expr, size }
            | Function::Left { expr, size }
            | Function::Right { expr, size }
            | Function::Repeat { expr, num: size }
            | Function::Skip { expr, size }
            | Function::Take { expr, size } => {
                $apply!($visit_expr(expr, $f));
                $apply!($visit_expr(size, $f));
            }
            Function::Lpad { expr, size, fill } | Function::Rpad { expr, size, fill } => {
                $apply!($visit_expr(expr, $f));
                $apply!($visit_expr(size, $f));
                if let Some(e) = fill {
                    $apply!($visit_expr(e, $f));
                }
            }
            Function::Replace { expr, old, new } => {
                $apply!($visit_expr(expr, $f));
                $apply!($visit_expr(old, $f));
                $apply!($visit_expr(new, $f));
            }
            Function::Cast { expr, .. } | Function::Extract { expr, .. } => {
                $apply!($visit_expr(expr, $f));
            }
            Function::Coalesce(exprs)
            | Function::Concat(exprs)
            | Function::Greatest(exprs)
            | Function::Custom { exprs, .. } => {
                for e in exprs {
                    $apply!($visit_expr(e, $f));
                }
            }
            Function::ConcatWs { separator, exprs } => {
                $apply!($visit_expr(separator, $f));
                for e in exprs {
                    $apply!($visit_expr(e, $f));
                }
            }
            Function::IfNull { expr, then } => {
                $apply!($visit_expr(expr, $f));
                $apply!($visit_expr(then, $f));
            }
            Function::NullIf { expr1, expr2 } => {
                $apply!($visit_expr(expr1, $f));
                $apply!($visit_expr(expr2, $f));
            }
            Function::Rand(expr) => {
                if let Some(e) = expr {
                    $apply!($visit_expr(e, $f));
                }
            }
            Function::Trim {
                expr, filter_chars, ..
            } => {
                $apply!($visit_expr(expr, $f));
                if let Some(e) = filter_chars {
                    $apply!($visit_expr(e, $f));
                }
            }
            Function::Log { antilog, base } => {
                $apply!($visit_expr(antilog, $f));
                $apply!($visit_expr(base, $f));
            }
            Function::Div { dividend, divisor } | Function::Mod { dividend, divisor } => {
                $apply!($visit_expr(dividend, $f));
                $apply!($visit_expr(divisor, $f));
            }
            Function::Gcd { left, right } | Function::Lcm { left, right } => {
                $apply!($visit_expr(left, $f));
                $apply!($visit_expr(right, $f));
            }
            Function::Power { expr, power } => {
                $apply!($visit_expr(expr, $f));
                $apply!($visit_expr(power, $f));
            }
            Function::Ltrim { expr, chars } | Function::Rtrim { expr, chars } => {
                $apply!($visit_expr(expr, $f));
                if let Some(e) = chars {
                    $apply!($visit_expr(e, $f));
                }
            }
            Function::Substr { expr, start, count } => {
                $apply!($visit_expr(expr, $f));
                $apply!($visit_expr(start, $f));
                if let Some(e) = count {
                    $apply!($visit_expr(e, $f));
                }
            }
            Function::Unwrap { expr, selector } => {
                $apply!($visit_expr(expr, $f));
                $apply!($visit_expr(selector, $f));
            }
            Function::Format { expr, format }
            | Function::ToDate { expr, format }
            | Function::ToTimestamp { expr, format }
            | Function::ToTime { expr, format } => {
                $apply!($visit_expr(expr, $f));
                $apply!($visit_expr(format, $f));
            }
            Function::Position {
                from_expr,
                sub_expr,
            } => {
                $apply!($visit_expr(from_expr, $f));
                $apply!($visit_expr(sub_expr, $f));
            }
            Function::FindIdx {
                from_expr,
                sub_expr,
                start,
            } => {
                $apply!($visit_expr(from_expr, $f));
                $apply!($visit_expr(sub_expr, $f));
                if let Some(e) = start {
                    $apply!($visit_expr(e, $f));
                }
            }
            Function::Append { expr, value } | Function::Prepend { expr, value } => {
                $apply!($visit_expr(expr, $f));
                $apply!($visit_expr(value, $f));
            }
            Function::Sort { expr, order } => {
                $apply!($visit_expr(expr, $f));
                if let Some(e) = order {
                    $apply!($visit_expr(e, $f));
                }
            }
            Function::Slice {
                expr,
                start,
                length,
            } => {
                $apply!($visit_expr(expr, $f));
                $apply!($visit_expr(start, $f));
                $apply!($visit_expr(length, $f));
            }
            Function::Point { x, y } => {
                $apply!($visit_expr(x, $f));
                $apply!($visit_expr(y, $f));
            }
            Function::CalcDistance {
                geometry1,
                geometry2,
            } => {
                $apply!($visit_expr(geometry1, $f));
                $apply!($visit_expr(geometry2, $f));
            }
            Function::Splice {
                list_data,
                begin_index,
                end_index,
                values,
            } => {
                $apply!($visit_expr(list_data, $f));
                $apply!($visit_expr(begin_index, $f));
                $apply!($visit_expr(end_index, $f));
                if let Some(e) = values {
                    $apply!($visit_expr(e, $f));
                }
            }
            Function::Now()
            | Function::CurrentDate()
            | Function::CurrentTime()
            | Function::CurrentTimestamp()
            | Function::Pi()
            | Function::GenerateUuid() => {}
        }
    };
}

pub fn visit_mut_function<F>(func: &mut Function, f: &mut F)
where
    F: FnMut(&mut Expr),
{
    visit_function_children!(func, visit_mut_expr, f, apply_mut);
}

pub fn try_visit_function<F>(func: &Function, f: &mut F) -> Result<(), PlanError>
where
    F: FnMut(&Expr) -> Result<(), PlanError>,
{
    visit_function_children!(func, try_visit_expr, f, apply_try);
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::Expr,
        parse_sql::parse_expr,
        plan::{
            PlanError,
            expr::{try_visit_expr, visit_mut_expr},
        },
        translate::{NO_PARAMS, translate_expr},
    };

    fn test(input: &str, expected: &str) {
        let parsed = parse_expr(input).expect(input);
        let mut expr = translate_expr(&parsed, NO_PARAMS).expect(input);

        visit_mut_expr(&mut expr, &mut |e| {
            if let Expr::Identifier(ident) = e {
                *e = Expr::Identifier(format!("_{ident}"));
            }
        });

        let expected_parsed = parse_expr(expected).expect(expected);
        let expected = translate_expr(&expected_parsed, NO_PARAMS).expect(expected);

        assert_eq!(expr, expected, "\ninput: {input}\nexpected: {expected:?}");
    }

    #[test]
    fn visit_mut_function_variants() {
        test("ABS(x)", "ABS(_x)");
        test("ADD_MONTH(d, n)", "ADD_MONTH(_d, _n)");
        test("LOWER(s)", "LOWER(_s)");
        test("INITCAP(s)", "INITCAP(_s)");
        test("UPPER(s)", "UPPER(_s)");
        test("LEFT(s, n)", "LEFT(_s, _n)");
        test("RIGHT(s, n)", "RIGHT(_s, _n)");
        test("ASIN(x)", "ASIN(_x)");
        test("ACOS(x)", "ACOS(_x)");
        test("ATAN(x)", "ATAN(_x)");
        test("LPAD(s, n)", "LPAD(_s, _n)");
        test("LPAD(s, n, f)", "LPAD(_s, _n, _f)");
        test("RPAD(s, n)", "RPAD(_s, _n)");
        test("RPAD(s, n, f)", "RPAD(_s, _n, _f)");
        test("REPLACE(s, o, n)", "REPLACE(_s, _o, _n)");
        test("CAST(x AS INTEGER)", "CAST(_x AS INTEGER)");
        test("CEIL(x)", "CEIL(_x)");
        test("COALESCE(a, b, c)", "COALESCE(_a, _b, _c)");
        test("CONCAT(a, b)", "CONCAT(_a, _b)");
        test("CONCAT_WS(sep, a, b)", "CONCAT_WS(_sep, _a, _b)");
        test("IFNULL(a, b)", "IFNULL(_a, _b)");
        test("NULLIF(a, b)", "NULLIF(_a, _b)");
        test("RAND()", "RAND()");
        test("RAND(x)", "RAND(_x)");
        test("ROUND(x)", "ROUND(_x)");
        test("TRUNC(x)", "TRUNC(_x)");
        test("FLOOR(x)", "FLOOR(_x)");
        test("TRIM(s)", "TRIM(_s)");
        test("TRIM(LEADING c FROM s)", "TRIM(LEADING _c FROM _s)");
        test("EXP(x)", "EXP(_x)");
        test("EXTRACT(YEAR FROM d)", "EXTRACT(YEAR FROM _d)");
        test("LN(x)", "LN(_x)");
        test("LOG(a, b)", "LOG(_a, _b)");
        test("LOG2(x)", "LOG2(_x)");
        test("LOG10(x)", "LOG10(_x)");
        test("DIV(a, b)", "DIV(_a, _b)");
        test("MOD(a, b)", "MOD(_a, _b)");
        test("GCD(a, b)", "GCD(_a, _b)");
        test("LCM(a, b)", "LCM(_a, _b)");
        test("SIN(x)", "SIN(_x)");
        test("COS(x)", "COS(_x)");
        test("TAN(x)", "TAN(_x)");
        test("SQRT(x)", "SQRT(_x)");
        test("POWER(a, b)", "POWER(_a, _b)");
        test("RADIANS(x)", "RADIANS(_x)");
        test("DEGREES(x)", "DEGREES(_x)");
        test("NOW()", "NOW()");
        test("CURRENT_DATE()", "CURRENT_DATE()");
        test("CURRENT_TIME()", "CURRENT_TIME()");
        test("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP()");
        test("PI()", "PI()");
        test("LAST_DAY(d)", "LAST_DAY(_d)");
        test("LTRIM(s)", "LTRIM(_s)");
        test("LTRIM(s, c)", "LTRIM(_s, _c)");
        test("RTRIM(s)", "RTRIM(_s)");
        test("RTRIM(s, c)", "RTRIM(_s, _c)");
        test("REVERSE(s)", "REVERSE(_s)");
        test("REPEAT(s, n)", "REPEAT(_s, _n)");
        test("SIGN(x)", "SIGN(_x)");
        test("SUBSTR(s, i)", "SUBSTR(_s, _i)");
        test("SUBSTR(s, i, n)", "SUBSTR(_s, _i, _n)");
        test("UNWRAP(m, k)", "UNWRAP(_m, _k)");
        test("GENERATE_UUID()", "GENERATE_UUID()");
        test("GREATEST(a, b, c)", "GREATEST(_a, _b, _c)");
        test("FORMAT(d, f)", "FORMAT(_d, _f)");
        test("TO_DATE(s, f)", "TO_DATE(_s, _f)");
        test("TO_TIMESTAMP(s, f)", "TO_TIMESTAMP(_s, _f)");
        test("TO_TIME(s, f)", "TO_TIME(_s, _f)");
        test("POSITION(a IN b)", "POSITION(_a IN _b)");
        test("FIND_IDX(a, b)", "FIND_IDX(_a, _b)");
        test("FIND_IDX(a, b, n)", "FIND_IDX(_a, _b, _n)");
        test("ASCII(s)", "ASCII(_s)");
        test("CHR(n)", "CHR(_n)");
        test("MD5(s)", "MD5(_s)");
        test("HEX(x)", "HEX(_x)");
        test("APPEND(l, v)", "APPEND(_l, _v)");
        test("SORT(l)", "SORT(_l)");
        test("SORT(l, o)", "SORT(_l, _o)");
        test("SLICE(l, s, n)", "SLICE(_l, _s, _n)");
        test("PREPEND(l, v)", "PREPEND(_l, _v)");
        test("SKIP(l, n)", "SKIP(_l, _n)");
        test("TAKE(l, n)", "TAKE(_l, _n)");
        test("GET_X(p)", "GET_X(_p)");
        test("GET_Y(p)", "GET_Y(_p)");
        test("POINT(x, y)", "POINT(_x, _y)");
        test(
            "CALC_DISTANCE(POINT(a, b), POINT(c, d))",
            "CALC_DISTANCE(POINT(_a, _b), POINT(_c, _d))",
        );
        test("IS_EMPTY(l)", "IS_EMPTY(_l)");
        test("LENGTH(s)", "LENGTH(_s)");
        test("ENTRIES(m)", "ENTRIES(_m)");
        test("KEYS(m)", "KEYS(_m)");
        test("VALUES(m)", "VALUES(_m)");
        test("SPLICE(l, a, b)", "SPLICE(_l, _a, _b)");
        test("SPLICE(l, a, b, v)", "SPLICE(_l, _a, _b, _v)");
        test("DEDUP(l)", "DEDUP(_l)");
    }

    #[test]
    fn try_visit_function_propagates_error() {
        let parsed = parse_expr("CONCAT(a, b)").expect("CONCAT(a, b)");
        let expr = translate_expr(&parsed, NO_PARAMS).expect("CONCAT(a, b)");

        let result = try_visit_expr(&expr, &mut |expr| match expr {
            Expr::Identifier(ident) if ident == "b" => Err(PlanError::Unreachable),
            _ => Ok(()),
        });

        assert_eq!(result, Err(PlanError::Unreachable));
    }
}
