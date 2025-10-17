use crate::ast::{Expr, Function};

pub fn is_deterministic(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) | Expr::TypedString { .. } => true,
        Expr::Identifier(_) | Expr::CompoundIdentifier { .. } => false,
        Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::UnaryOp { expr: inner, .. }
        | Expr::Nested(inner)
        | Expr::Interval { expr: inner, .. } => is_deterministic(inner),
        Expr::BinaryOp { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        }
        | Expr::ILike {
            expr: left,
            pattern: right,
            ..
        } => is_deterministic(left) && is_deterministic(right),
        Expr::Between {
            expr, low, high, ..
        } => is_deterministic(expr) && is_deterministic(low) && is_deterministic(high),
        Expr::InList { expr, list, .. } => {
            is_deterministic(expr) && list.iter().all(is_deterministic)
        }
        Expr::Function(function) => is_function_deterministic(function),
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            operand.as_deref().map(is_deterministic).unwrap_or(true)
                && when_then
                    .iter()
                    .all(|(when, then)| is_deterministic(when) && is_deterministic(then))
                && else_result.as_deref().map(is_deterministic).unwrap_or(true)
        }
        Expr::Array { elem } => elem.iter().all(is_deterministic),
        Expr::ArrayIndex { obj, indexes } => {
            is_deterministic(obj) && indexes.iter().all(is_deterministic)
        }
        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } | Expr::Aggregate(_) => {
            false
        }
    }
}

fn is_function_deterministic(function: &Function) -> bool {
    use Function::*;

    match function {
        Now() | CurrentDate() | CurrentTime() | CurrentTimestamp() | GenerateUuid() | Rand(_) => {
            false
        }
        Custom { .. } => false,
        Cast { expr, .. }
        | Abs(expr)
        | Initcap(expr)
        | Lower(expr)
        | Upper(expr)
        | Asin(expr)
        | Acos(expr)
        | Atan(expr)
        | Ceil(expr)
        | Round(expr)
        | Trunc(expr)
        | Floor(expr)
        | Exp(expr)
        | Ln(expr)
        | Log2(expr)
        | Log10(expr)
        | Sin(expr)
        | Cos(expr)
        | Tan(expr)
        | Sqrt(expr)
        | Radians(expr)
        | Degrees(expr)
        | LastDay(expr)
        | Reverse(expr)
        | Sign(expr)
        | IsEmpty(expr)
        | Length(expr)
        | Entries(expr)
        | Keys(expr)
        | Values(expr)
        | Ascii(expr)
        | Chr(expr)
        | Md5(expr)
        | Hex(expr)
        | GetX(expr)
        | GetY(expr)
        | Dedup(expr)
        | Extract { expr, .. }
        | Sort { expr, order: None } => is_deterministic(expr),
        Coalesce(exprs) | Concat(exprs) | Greatest(exprs) => exprs.iter().all(is_deterministic),
        ConcatWs { separator, exprs } => {
            is_deterministic(separator) && exprs.iter().all(is_deterministic)
        }
        IfNull { expr, then }
        | NullIf {
            expr1: expr,
            expr2: then,
        } => is_deterministic(expr) && is_deterministic(then),
        AddMonth { expr, size }
        | Left { expr, size }
        | Right { expr, size }
        | Repeat { expr, num: size }
        | Skip { expr, size }
        | Take { expr, size }
        | Append { expr, value: size }
        | Prepend { expr, value: size }
        | Div {
            dividend: expr,
            divisor: size,
        }
        | Mod {
            dividend: expr,
            divisor: size,
        }
        | Gcd {
            left: expr,
            right: size,
        }
        | Lcm {
            left: expr,
            right: size,
        }
        | Power { expr, power: size }
        | Format { expr, format: size }
        | ToDate { expr, format: size }
        | ToTimestamp { expr, format: size }
        | ToTime { expr, format: size }
        | Position {
            from_expr: size,
            sub_expr: expr,
        }
        | Point { x: expr, y: size }
        | CalcDistance {
            geometry1: expr,
            geometry2: size,
        }
        | Sort {
            expr,
            order: Some(size),
        } => is_deterministic(expr) && is_deterministic(size),
        Log { antilog, base } => is_deterministic(antilog) && is_deterministic(base),
        Replace { expr, old, new } => {
            is_deterministic(expr) && is_deterministic(old) && is_deterministic(new)
        }
        Lpad { expr, size, fill } | Rpad { expr, size, fill } => {
            is_deterministic(expr)
                && is_deterministic(size)
                && fill.as_ref().is_none_or(is_deterministic)
        }
        Trim {
            expr, filter_chars, ..
        } => is_deterministic(expr) && filter_chars.as_ref().is_none_or(is_deterministic),
        Ltrim { expr, chars } | Rtrim { expr, chars } => {
            is_deterministic(expr) && chars.as_ref().is_none_or(is_deterministic)
        }
        Slice {
            expr,
            start,
            length,
        } => is_deterministic(expr) && is_deterministic(start) && is_deterministic(length),
        Substr { expr, start, count } => {
            is_deterministic(expr)
                && is_deterministic(start)
                && count.as_ref().is_none_or(is_deterministic)
        }
        Unwrap { expr, selector } => is_deterministic(expr) && is_deterministic(selector),
        FindIdx {
            from_expr,
            sub_expr,
            start,
        } => {
            is_deterministic(from_expr)
                && is_deterministic(sub_expr)
                && start.as_ref().is_none_or(is_deterministic)
        }
        Splice {
            list_data,
            begin_index,
            end_index,
            values,
        } => {
            is_deterministic(list_data)
                && is_deterministic(begin_index)
                && is_deterministic(end_index)
                && values.as_ref().is_none_or(is_deterministic)
        }
        Pi() => true,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::is_deterministic,
        crate::{parse_sql::parse_expr, translate::translate_expr},
    };

    fn test(sql: &str, expected: bool) {
        let expr = parse_expr(sql).and_then(|parsed| translate_expr(&parsed));
        let actual = expr.map(|expr| is_deterministic(&expr));

        assert_eq!(actual, Ok(expected), "{sql} deterministic mismatch");
    }

    #[test]
    fn expression_cases() {
        test("1", true);
        test("INTERVAL 1 DAY", true);
        test("-(1 + 2)", true);
        test("('A' IS NULL)", true);
        test("CAST(1 AS INT)", true);
        test("(1 BETWEEN 0 AND 2)", true);
        test("1 + (2 * 3)", true);
        test("('A' LIKE 'A%')", true);
        test("('A' ILIKE 'A%')", true);
        test("ARRAY[1, 2, 3]", true);
        test("ARRAY['A', 'B'][1]", true);
        test("CASE 1 WHEN 1 THEN 2 ELSE 3 END", true);
        test("('A' IN ('A', 'B'))", true);

        test("id", false);
        test("Foo.id", false);
        test("NOW()", false);
        test("RAND()", false);
        test("(SELECT 1)", false);
        test("EXISTS (SELECT 1)", false);
        test("1 IN (SELECT 1)", false);
        test("SUM(id)", false);
    }

    #[test]
    fn function_branch_cases() {
        test("ABS(1)", true);
        test("ADD_MONTH(DATE '2020-01-01', 1)", true);
        test("LOWER('ABC')", true);
        test("INITCAP('abc')", true);
        test("UPPER('abc')", true);
        test("LEFT('abc', 1)", true);
        test("RIGHT('abc', 1)", true);
        test("ASIN(0)", true);
        test("ACOS(0)", true);
        test("ATAN(0)", true);
        test("LPAD('abc', 5, 'x')", true);
        test("RPAD('abc', 5, 'x')", true);
        test("REPLACE('abc', 'b', 'c')", true);
        test("CAST(1 AS INT)", true);
        test("CEIL(1.1)", true);
        test("COALESCE(1, 2)", true);
        test("CONCAT('a', 'b')", true);
        test("CONCAT_WS('-', 'a', 'b')", true);
        test("CUSTOM_FUNC()", false);
        test("IFNULL(1, 2)", true);
        test("NULLIF(1, 2)", true);
        test("RAND()", false);
        test("ROUND(1.2)", true);
        test("TRUNC(1.2)", true);
        test("FLOOR(1.2)", true);
        test("TRIM('  value  ')", true);
        test("EXP(1)", true);
        test("EXTRACT(YEAR FROM DATE '2020-01-01')", true);
        test("LN(1)", true);
        test("LOG(2, 10)", true);
        test("LOG2(2)", true);
        test("LOG10(10)", true);
        test("DIV(4, 2)", true);
        test("MOD(4, 2)", true);
        test("GCD(4, 2)", true);
        test("LCM(4, 2)", true);
        test("SIN(1)", true);
        test("COS(1)", true);
        test("TAN(1)", true);
        test("SQRT(4)", true);
        test("POWER(2, 3)", true);
        test("RADIANS(180)", true);
        test("DEGREES(3.14)", true);
        test("NOW()", false);
        test("CURRENT_DATE()", false);
        test("CURRENT_TIME()", false);
        test("CURRENT_TIMESTAMP()", false);
        test("PI()", true);
        test("LAST_DAY(DATE '2020-01-01')", true);
        test("LTRIM('  abc')", true);
        test("RTRIM('abc  ')", true);
        test("REVERSE('abc')", true);
        test("REPEAT('a', 2)", true);
        test("SIGN(1)", true);
        test("SUBSTR('abc', 1, 1)", true);
        test(r#"UNWRAP('{"a":1}', 'a')"#, true);
        test("GENERATE_UUID()", false);
        test("GREATEST(1, 2)", true);
        test("FORMAT('value', '%s')", true);
        test("TO_DATE('2020-01-01', '%Y-%m-%d')", true);
        test(
            "TO_TIMESTAMP('2020-01-01T00:00:00', '%Y-%m-%dT%H:%M:%S')",
            true,
        );
        test("TO_TIME('12:34:56', '%H:%M:%S')", true);
        test("POSITION('a' IN 'abc')", true);
        test("FIND_IDX('abc', 'a', 1)", true);
        test("ASCII('A')", true);
        test("CHR(65)", true);
        test("MD5('abc')", true);
        test("HEX('abc')", true);
        test("APPEND('[1,2]', '3')", true);
        test("SORT('[1,2]')", true);
        test("SORT('[1,2]', 'ASC')", true);
        test("SLICE('[1,2,3]', 1, 1)", true);
        test("PREPEND('[2,3]', '1')", true);
        test("SKIP('[1,2,3]', 1)", true);
        test("TAKE('[1,2,3]', 2)", true);
        test("GET_X('POINT(1 2)')", true);
        test("GET_Y('POINT(1 2)')", true);
        test("POINT(1, 2)", true);
        test("CALC_DISTANCE(POINT(0, 0), POINT(1, 1))", true);
        test("IS_EMPTY('[]')", true);
        test("LENGTH('[1,2]')", true);
        test(r#"ENTRIES('{"a":1}')"#, true);
        test(r#"KEYS('{"a":1}')"#, true);
        test(r#"VALUES('{"a":1}')"#, true);
        test("RAND(1)", false);
        test("SPLICE('[1,2,3]', 1, 2, '[9]')", true);
        test("DEDUP('[1,1,2]')", true);
    }

    #[test]
    #[should_panic(expected = "deterministic mismatch")]
    fn invalid_expression_panics() {
        test("(+", false);
    }
}
