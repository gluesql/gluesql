use crate::ast::{AstLiteral, Expr, Function};

pub fn may_return_null(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(AstLiteral::Null)
        | Expr::Identifier(_)
        | Expr::CompoundIdentifier { .. }
        | Expr::ArrayIndex { .. }
        | Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::Aggregate(_) => true,
        Expr::Value(value) => value.is_null(),
        Expr::Literal(_) | Expr::TypedString { .. } | Expr::IsNull(_) | Expr::IsNotNull(_) => false,
        Expr::UnaryOp { expr: inner, .. }
        | Expr::Nested(inner)
        | Expr::Interval { expr: inner, .. } => may_return_null(inner),
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
        } => may_return_null(left) || may_return_null(right),
        Expr::Between {
            expr, low, high, ..
        } => may_return_null(expr) || may_return_null(low) || may_return_null(high),
        Expr::InList { expr, list, .. } => {
            may_return_null(expr) || list.iter().any(may_return_null)
        }
        Expr::Function(function) => function_may_return_null(function),
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            operand.as_deref().is_some_and(may_return_null)
                || when_then
                    .iter()
                    .any(|(when, then)| may_return_null(when) || may_return_null(then))
                || else_result.as_deref().is_none_or(may_return_null)
        }
        Expr::Array { elem } => elem.iter().any(may_return_null),
    }
}

fn function_may_return_null(function: &Function) -> bool {
    use Function::*;

    match function {
        Coalesce(exprs) => exprs.iter().all(may_return_null),
        IfNull { expr, then } => may_return_null(expr) && may_return_null(then),
        NullIf { .. } | Custom { .. } => true,
        Now() | CurrentDate() | CurrentTime() | CurrentTimestamp() | Pi() | GenerateUuid()
        | Rand(_) => false,
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
        | Sort { expr, order: None } => may_return_null(expr),
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
        } => may_return_null(expr) || may_return_null(size),
        Log { antilog, base } => may_return_null(antilog) || may_return_null(base),
        Concat(exprs) | Greatest(exprs) => exprs.iter().any(may_return_null),
        ConcatWs { separator, exprs } => {
            may_return_null(separator) || exprs.iter().any(may_return_null)
        }
        Replace { expr, old, new } => {
            may_return_null(expr) || may_return_null(old) || may_return_null(new)
        }
        Lpad { expr, size, fill } | Rpad { expr, size, fill } => {
            may_return_null(expr)
                || may_return_null(size)
                || fill.as_ref().is_some_and(may_return_null)
        }
        Trim {
            expr, filter_chars, ..
        } => may_return_null(expr) || filter_chars.as_ref().is_some_and(may_return_null),
        Ltrim { expr, chars } | Rtrim { expr, chars } => {
            may_return_null(expr) || chars.as_ref().is_some_and(may_return_null)
        }
        Slice {
            expr,
            start,
            length,
        } => may_return_null(expr) || may_return_null(start) || may_return_null(length),
        Substr { expr, start, count } => {
            may_return_null(expr)
                || may_return_null(start)
                || count.as_ref().is_some_and(may_return_null)
        }
        Unwrap { expr, selector } => may_return_null(expr) || may_return_null(selector),
        FindIdx {
            from_expr,
            sub_expr,
            start,
        } => {
            may_return_null(from_expr)
                || may_return_null(sub_expr)
                || start.as_ref().is_some_and(may_return_null)
        }
        Splice {
            list_data,
            begin_index,
            end_index,
            values,
        } => {
            may_return_null(list_data)
                || may_return_null(begin_index)
                || may_return_null(end_index)
                || values.as_ref().is_some_and(may_return_null)
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::may_return_null,
        crate::{
            parse_sql::parse_expr,
            translate::{NO_PARAMS, translate_expr},
        },
    };

    fn test(sql: &str, expected: bool) {
        let expr = parse_expr(sql).and_then(|parsed| translate_expr(&parsed, NO_PARAMS));
        let actual = expr.map(|expr| may_return_null(&expr));

        assert_eq!(actual, Ok(expected), "{sql} nullability mismatch");
    }

    #[test]
    fn expression_cases() {
        test("NULL", true);
        test("id", true);
        test("Foo.id", true);
        test("COALESCE(NULL, id)", true);
        test("IFNULL(id, NULL)", true);
        test("CASE 1 WHEN 1 THEN id ELSE 0 END", true);
        test("1 IN (SELECT 1)", true);
        test("id IN (1, 2, 3)", true);
        test("ARRAY[1, id]", true);
        test("POSITION('a' IN id)", true);

        test("1", false);
        test("'A'", false);
        test("INTERVAL 1 DAY", false);
        test("NOT TRUE", false);
        test("1 BETWEEN 0 AND 2", false);
        test("'A' LIKE 'A%'", false);
        test("'A' ILIKE 'A%'", false);
        test("1 IN (1, 2, 3)", false);
        test("('A' IS NULL)", false);
        test("CASE 1 WHEN 1 THEN 2 ELSE 3 END", false);
    }

    #[test]
    fn function_branch_cases() {
        test("ABS(1)", false);
        test("ADD_MONTH(DATE '2020-01-01', 1)", false);
        test("LOWER('ABC')", false);
        test("INITCAP('abc')", false);
        test("UPPER('abc')", false);
        test("LEFT('abc', 1)", false);
        test("RIGHT('abc', 1)", false);
        test("ASIN(0)", false);
        test("ACOS(0)", false);
        test("ATAN(0)", false);
        test("LPAD('abc', 5, 'x')", false);
        test("RPAD('abc', 5, 'x')", false);
        test("REPLACE('abc', 'b', 'c')", false);
        test("CAST(1 AS INT)", false);
        test("CEIL(1.1)", false);
        test("COALESCE(1, 2)", false);
        test("CONCAT('a', 'b')", false);
        test("CONCAT_WS('-', 'a', 'b')", false);
        test("CUSTOM_FUNC()", true);
        test("IFNULL(1, 2)", false);
        test("NULLIF(1, 2)", true);
        test("RAND()", false);
        test("ROUND(1.2)", false);
        test("TRUNC(1.2)", false);
        test("FLOOR(1.2)", false);
        test("TRIM('  value  ')", false);
        test("EXP(1)", false);
        test("EXTRACT(YEAR FROM DATE '2020-01-01')", false);
        test("LN(1)", false);
        test("LOG(2, 10)", false);
        test("LOG2(2)", false);
        test("LOG10(10)", false);
        test("DIV(4, 2)", false);
        test("MOD(4, 2)", false);
        test("GCD(4, 2)", false);
        test("LCM(4, 2)", false);
        test("SIN(1)", false);
        test("COS(1)", false);
        test("TAN(1)", false);
        test("SQRT(4)", false);
        test("POWER(2, 3)", false);
        test("RADIANS(180)", false);
        test("DEGREES(3.14)", false);
        test("NOW()", false);
        test("CURRENT_DATE()", false);
        test("CURRENT_TIME()", false);
        test("CURRENT_TIMESTAMP()", false);
        test("PI()", false);
        test("LAST_DAY(DATE '2020-01-01')", false);
        test("LTRIM('  abc')", false);
        test("RTRIM('abc  ')", false);
        test("REVERSE('abc')", false);
        test("REPEAT('a', 2)", false);
        test("SIGN(1)", false);
        test("SUBSTR('abc', 1, 1)", false);
        test(r#"UNWRAP('{"a":1}', 'a')"#, false);
        test("GENERATE_UUID()", false);
        test("GREATEST(1, 2)", false);
        test("FORMAT('value', '%s')", false);
        test("TO_DATE('2020-01-01', '%Y-%m-%d')", false);
        test(
            "TO_TIMESTAMP('2020-01-01T00:00:00', '%Y-%m-%dT%H:%M:%S')",
            false,
        );
        test("TO_TIME('12:34:56', '%H:%M:%S')", false);
        test("POSITION('a' IN 'abc')", false);
        test("FIND_IDX('abc', 'a', 1)", false);
        test("ASCII('A')", false);
        test("CHR(65)", false);
        test("MD5('abc')", false);
        test("HEX('abc')", false);
        test("APPEND('[1,2]', '3')", false);
        test("SORT('[1,2]')", false);
        test("SORT('[1,2]', 'ASC')", false);
        test("SLICE('[1,2,3]', 1, 1)", false);
        test("PREPEND('[2,3]', '1')", false);
        test("SKIP('[1,2,3]', 1)", false);
        test("TAKE('[1,2,3]', 2)", false);
        test("GET_X('POINT(1 2)')", false);
        test("GET_Y('POINT(1 2)')", false);
        test("POINT(1, 2)", false);
        test("CALC_DISTANCE(POINT(0, 0), POINT(1, 1))", false);
        test("IS_EMPTY('[]')", false);
        test("LENGTH('[1,2]')", false);
        test(r#"ENTRIES('{"a":1}')"#, false);
        test(r#"KEYS('{"a":1}')"#, false);
        test(r#"VALUES('{"a":1}')"#, false);
        test("COALESCE(NULL)", true);
        test("IFNULL(NULL, NULL)", true);
        test("NULLIF(1, 1)", true);
        test("RAND(1)", false);
        test("COALESCE(NULL, NULL)", true);
        test("COALESCE()", true);
        test("SPLICE('[1,2,3]', 1, 2, '[9]')", false);
        test("DEDUP('[1,1,2]')", false);
    }

    #[test]
    #[should_panic(expected = "nullability mismatch")]
    fn invalid_expression_panics() {
        test("INVALID SQL", false);
    }
}
