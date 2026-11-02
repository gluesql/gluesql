use {super::super::Visitor, crate::plan::FunctionExprPlan};

pub fn walk_function<V: Visitor + ?Sized>(
    visitor: &mut V,
    function: &FunctionExprPlan,
) -> Result<(), V::Error> {
    match function {
        FunctionExprPlan::Abs(expr)
        | FunctionExprPlan::Lower(expr)
        | FunctionExprPlan::Initcap(expr)
        | FunctionExprPlan::Upper(expr)
        | FunctionExprPlan::Asin(expr)
        | FunctionExprPlan::Acos(expr)
        | FunctionExprPlan::Atan(expr)
        | FunctionExprPlan::Ceil(expr)
        | FunctionExprPlan::Round(expr)
        | FunctionExprPlan::Trunc(expr)
        | FunctionExprPlan::Floor(expr)
        | FunctionExprPlan::Exp(expr)
        | FunctionExprPlan::Ln(expr)
        | FunctionExprPlan::Log2(expr)
        | FunctionExprPlan::Log10(expr)
        | FunctionExprPlan::Sin(expr)
        | FunctionExprPlan::Cos(expr)
        | FunctionExprPlan::Tan(expr)
        | FunctionExprPlan::Sqrt(expr)
        | FunctionExprPlan::Radians(expr)
        | FunctionExprPlan::Degrees(expr)
        | FunctionExprPlan::LastDay(expr)
        | FunctionExprPlan::Reverse(expr)
        | FunctionExprPlan::Sign(expr)
        | FunctionExprPlan::Ascii(expr)
        | FunctionExprPlan::Chr(expr)
        | FunctionExprPlan::Md5(expr)
        | FunctionExprPlan::Hex(expr)
        | FunctionExprPlan::IsEmpty(expr)
        | FunctionExprPlan::Length(expr)
        | FunctionExprPlan::Entries(expr)
        | FunctionExprPlan::Keys(expr)
        | FunctionExprPlan::Values(expr)
        | FunctionExprPlan::Dedup(expr)
        | FunctionExprPlan::GetX(expr)
        | FunctionExprPlan::GetY(expr) => visitor.visit_expr(expr)?,
        FunctionExprPlan::AddMonth { expr, size }
        | FunctionExprPlan::Left { expr, size }
        | FunctionExprPlan::Right { expr, size }
        | FunctionExprPlan::Repeat { expr, num: size }
        | FunctionExprPlan::Skip { expr, size }
        | FunctionExprPlan::Take { expr, size } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(size)?;
        }
        FunctionExprPlan::Lpad { expr, size, fill }
        | FunctionExprPlan::Rpad { expr, size, fill } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(size)?;
            if let Some(e) = fill {
                visitor.visit_expr(e)?;
            }
        }
        FunctionExprPlan::Replace { expr, old, new } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(old)?;
            visitor.visit_expr(new)?;
        }
        FunctionExprPlan::Cast { expr, .. } | FunctionExprPlan::Extract { expr, .. } => {
            visitor.visit_expr(expr)?;
        }
        FunctionExprPlan::Coalesce(exprs)
        | FunctionExprPlan::Concat(exprs)
        | FunctionExprPlan::Greatest(exprs)
        | FunctionExprPlan::UnplannedCustom { exprs, .. }
        | FunctionExprPlan::Custom { exprs, .. } => {
            for e in exprs {
                visitor.visit_expr(e)?;
            }
        }
        FunctionExprPlan::ConcatWs { separator, exprs } => {
            visitor.visit_expr(separator)?;
            for e in exprs {
                visitor.visit_expr(e)?;
            }
        }
        FunctionExprPlan::IfNull { expr, then } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(then)?;
        }
        FunctionExprPlan::NullIf { expr1, expr2 } => {
            visitor.visit_expr(expr1)?;
            visitor.visit_expr(expr2)?;
        }
        FunctionExprPlan::Rand(expr) => {
            if let Some(e) = expr {
                visitor.visit_expr(e)?;
            }
        }
        FunctionExprPlan::Trim {
            expr, filter_chars, ..
        } => {
            visitor.visit_expr(expr)?;
            if let Some(e) = filter_chars {
                visitor.visit_expr(e)?;
            }
        }
        FunctionExprPlan::Log { antilog, base } => {
            visitor.visit_expr(antilog)?;
            visitor.visit_expr(base)?;
        }
        FunctionExprPlan::Div { dividend, divisor }
        | FunctionExprPlan::Mod { dividend, divisor } => {
            visitor.visit_expr(dividend)?;
            visitor.visit_expr(divisor)?;
        }
        FunctionExprPlan::Gcd { left, right } | FunctionExprPlan::Lcm { left, right } => {
            visitor.visit_expr(left)?;
            visitor.visit_expr(right)?;
        }
        FunctionExprPlan::Power { expr, power } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(power)?;
        }
        FunctionExprPlan::Ltrim { expr, chars } | FunctionExprPlan::Rtrim { expr, chars } => {
            visitor.visit_expr(expr)?;
            if let Some(e) = chars {
                visitor.visit_expr(e)?;
            }
        }
        FunctionExprPlan::Substr { expr, start, count } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(start)?;
            if let Some(e) = count {
                visitor.visit_expr(e)?;
            }
        }
        FunctionExprPlan::Unwrap { expr, selector } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(selector)?;
        }
        FunctionExprPlan::Format { expr, format }
        | FunctionExprPlan::ToDate { expr, format }
        | FunctionExprPlan::ToTimestamp { expr, format }
        | FunctionExprPlan::ToTime { expr, format } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(format)?;
        }
        FunctionExprPlan::Position {
            from_expr,
            sub_expr,
        } => {
            visitor.visit_expr(from_expr)?;
            visitor.visit_expr(sub_expr)?;
        }
        FunctionExprPlan::FindIdx {
            from_expr,
            sub_expr,
            start,
        } => {
            visitor.visit_expr(from_expr)?;
            visitor.visit_expr(sub_expr)?;
            if let Some(e) = start {
                visitor.visit_expr(e)?;
            }
        }
        FunctionExprPlan::Append { expr, value } | FunctionExprPlan::Prepend { expr, value } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(value)?;
        }
        FunctionExprPlan::Sort { expr, order } => {
            visitor.visit_expr(expr)?;
            if let Some(e) = order {
                visitor.visit_expr(e)?;
            }
        }
        FunctionExprPlan::Slice {
            expr,
            start,
            length,
        } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(start)?;
            visitor.visit_expr(length)?;
        }
        FunctionExprPlan::Point { x, y } => {
            visitor.visit_expr(x)?;
            visitor.visit_expr(y)?;
        }
        FunctionExprPlan::CalcDistance {
            geometry1,
            geometry2,
        } => {
            visitor.visit_expr(geometry1)?;
            visitor.visit_expr(geometry2)?;
        }
        FunctionExprPlan::Splice {
            list_data,
            begin_index,
            end_index,
            values,
        } => {
            visitor.visit_expr(list_data)?;
            visitor.visit_expr(begin_index)?;
            visitor.visit_expr(end_index)?;
            if let Some(e) = values {
                visitor.visit_expr(e)?;
            }
        }
        FunctionExprPlan::Now()
        | FunctionExprPlan::CurrentDate()
        | FunctionExprPlan::CurrentTime()
        | FunctionExprPlan::CurrentTimestamp()
        | FunctionExprPlan::Pi()
        | FunctionExprPlan::GenerateUuid() => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::super::try_visit_expr,
        crate::{
            parse_sql::parse_expr,
            plan::{ExprPlan, FunctionExprPlan},
            translate::{NO_PARAMS, translate_expr},
        },
    };

    fn test(input: &str, expected: &[&str]) {
        let parsed = parse_expr(input).expect(input);
        let expr = ExprPlan::from(translate_expr(&parsed, NO_PARAMS).expect(input));
        let mut visited = Vec::new();
        let result: Result<(), &str> = try_visit_expr(&expr, &mut |expr| {
            if let ExprPlan::Identifier(name) = expr {
                visited.push(name.clone());
            }
            Ok(())
        });
        assert_eq!(result, Ok(()), "{input}");
        assert_eq!(visited, expected, "{input}");
    }

    #[test]
    fn visit_function_variants() {
        test("ABS(x)", &["x"]);
        test("ADD_MONTH(d, n)", &["d", "n"]);
        test("LOWER(s)", &["s"]);
        test("INITCAP(s)", &["s"]);
        test("UPPER(s)", &["s"]);
        test("LEFT(s, n)", &["s", "n"]);
        test("RIGHT(s, n)", &["s", "n"]);
        test("ASIN(x)", &["x"]);
        test("ACOS(x)", &["x"]);
        test("ATAN(x)", &["x"]);
        test("LPAD(s, n)", &["s", "n"]);
        test("LPAD(s, n, f)", &["s", "n", "f"]);
        test("RPAD(s, n)", &["s", "n"]);
        test("RPAD(s, n, f)", &["s", "n", "f"]);
        test("REPLACE(s, o, n)", &["s", "o", "n"]);
        test("CAST(x AS INTEGER)", &["x"]);
        test("CEIL(x)", &["x"]);
        test("COALESCE(a, b, c)", &["a", "b", "c"]);
        test("CONCAT(a, b)", &["a", "b"]);
        test("CONCAT_WS(sep, a, b)", &["sep", "a", "b"]);
        test("IFNULL(a, b)", &["a", "b"]);
        test("NULLIF(a, b)", &["a", "b"]);
        test("RAND()", &[]);
        test("RAND(x)", &["x"]);
        test("ROUND(x)", &["x"]);
        test("TRUNC(x)", &["x"]);
        test("FLOOR(x)", &["x"]);
        test("TRIM(s)", &["s"]);
        test("TRIM(LEADING c FROM s)", &["s", "c"]);
        test("EXP(x)", &["x"]);
        test("EXTRACT(YEAR FROM d)", &["d"]);
        test("LN(x)", &["x"]);
        test("LOG(a, b)", &["a", "b"]);
        test("LOG2(x)", &["x"]);
        test("LOG10(x)", &["x"]);
        test("DIV(a, b)", &["a", "b"]);
        test("MOD(a, b)", &["a", "b"]);
        test("GCD(a, b)", &["a", "b"]);
        test("LCM(a, b)", &["a", "b"]);
        test("SIN(x)", &["x"]);
        test("COS(x)", &["x"]);
        test("TAN(x)", &["x"]);
        test("SQRT(x)", &["x"]);
        test("POWER(a, b)", &["a", "b"]);
        test("RADIANS(x)", &["x"]);
        test("DEGREES(x)", &["x"]);
        test("NOW()", &[]);
        test("CURRENT_DATE()", &[]);
        test("CURRENT_TIME()", &[]);
        test("CURRENT_TIMESTAMP()", &[]);
        test("PI()", &[]);
        test("LAST_DAY(d)", &["d"]);
        test("LTRIM(s)", &["s"]);
        test("LTRIM(s, c)", &["s", "c"]);
        test("RTRIM(s)", &["s"]);
        test("RTRIM(s, c)", &["s", "c"]);
        test("REVERSE(s)", &["s"]);
        test("REPEAT(s, n)", &["s", "n"]);
        test("SIGN(x)", &["x"]);
        test("SUBSTR(s, i)", &["s", "i"]);
        test("SUBSTR(s, i, n)", &["s", "i", "n"]);
        test("UNWRAP(m, k)", &["m", "k"]);
        test("GENERATE_UUID()", &[]);
        test("GREATEST(a, b, c)", &["a", "b", "c"]);
        test("FORMAT(d, f)", &["d", "f"]);
        test("TO_DATE(s, f)", &["s", "f"]);
        test("TO_TIMESTAMP(s, f)", &["s", "f"]);
        test("TO_TIME(s, f)", &["s", "f"]);
        test("POSITION(a IN b)", &["b", "a"]);
        test("FIND_IDX(a, b)", &["a", "b"]);
        test("FIND_IDX(a, b, n)", &["a", "b", "n"]);
        test("ASCII(s)", &["s"]);
        test("CHR(n)", &["n"]);
        test("MD5(s)", &["s"]);
        test("HEX(x)", &["x"]);
        test("APPEND(l, v)", &["l", "v"]);
        test("SORT(l)", &["l"]);
        test("SORT(l, o)", &["l", "o"]);
        test("SLICE(l, s, n)", &["l", "s", "n"]);
        test("PREPEND(l, v)", &["l", "v"]);
        test("SKIP(l, n)", &["l", "n"]);
        test("TAKE(l, n)", &["l", "n"]);
        test("GET_X(p)", &["p"]);
        test("GET_Y(p)", &["p"]);
        test("POINT(x, y)", &["x", "y"]);
        test(
            "CALC_DISTANCE(POINT(a, b), POINT(c, d))",
            &["a", "b", "c", "d"],
        );
        test("IS_EMPTY(l)", &["l"]);
        test("LENGTH(s)", &["s"]);
        test("ENTRIES(m)", &["m"]);
        test("KEYS(m)", &["m"]);
        test("VALUES(m)", &["m"]);
        test("SPLICE(l, a, b)", &["l", "a", "b"]);
        test("SPLICE(l, a, b, v)", &["l", "a", "b", "v"]);
        test("DEDUP(l)", &["l"]);
        test("custom_func()", &[]);
        test("custom_func(a, b)", &["a", "b"]);
    }

    #[test]
    fn try_visit_function_short_circuits_after_error() {
        let cases: &[(&str, &[&str])] = &[
            ("ABS(a)", &["a"]),
            ("ADD_MONTH(a, b)", &["a", "b"]),
            ("LPAD(a, b)", &["a", "b"]),
            ("LPAD(a, b, c)", &["a", "b", "c"]),
            ("REPLACE(a, b, c)", &["a", "b", "c"]),
            ("CAST(a AS INT)", &["a"]),
            ("CONCAT(a, b)", &["a", "b"]),
            ("custom_func(a, b)", &["a", "b"]),
            ("CONCAT_WS(a, b, c)", &["a", "b", "c"]),
            ("IFNULL(a, b)", &["a", "b"]),
            ("NULLIF(a, b)", &["a", "b"]),
            ("RAND(a)", &["a"]),
            ("TRIM(a)", &["a"]),
            ("TRIM(b FROM a)", &["a", "b"]),
            ("LOG(a, b)", &["a", "b"]),
            ("DIV(a, b)", &["a", "b"]),
            ("GCD(a, b)", &["a", "b"]),
            ("POWER(a, b)", &["a", "b"]),
            ("LTRIM(a)", &["a"]),
            ("LTRIM(a, b)", &["a", "b"]),
            ("SUBSTR(a, b)", &["a", "b"]),
            ("SUBSTR(a, b, c)", &["a", "b", "c"]),
            ("UNWRAP(a, b)", &["a", "b"]),
            ("FORMAT(a, b)", &["a", "b"]),
            ("POSITION(a IN b)", &["b", "a"]),
            ("FIND_IDX(a, b)", &["a", "b"]),
            ("FIND_IDX(a, b, c)", &["a", "b", "c"]),
            ("APPEND(a, b)", &["a", "b"]),
            ("SORT(a)", &["a"]),
            ("SORT(a, b)", &["a", "b"]),
            ("SLICE(a, b, c)", &["a", "b", "c"]),
            ("POINT(a, b)", &["a", "b"]),
            ("CALC_DISTANCE(a, b)", &["a", "b"]),
            ("SPLICE(a, b, c)", &["a", "b", "c"]),
            ("SPLICE(a, b, c, d)", &["a", "b", "c", "d"]),
        ];
        for &(sql, expected) in cases {
            for end in 0..expected.len() {
                let parsed = parse_expr(sql).expect(sql);
                let expr = ExprPlan::from(translate_expr(&parsed, NO_PARAMS).expect(sql));
                let mut visited = Vec::new();
                let actual = try_visit_expr(&expr, &mut |expr| {
                    if let ExprPlan::Identifier(name) = expr {
                        visited.push(name.clone());
                        if name == expected[end] {
                            return Err("stop");
                        }
                    }
                    Ok(())
                });
                assert_eq!(actual, Err("stop"), "{sql}: stop at {}", expected[end]);
                assert_eq!(visited, expected[..=end], "{sql}");
            }
        }
    }

    #[test]
    fn functions_without_arguments() {
        for sql in [
            "NOW()",
            "CURRENT_DATE()",
            "CURRENT_TIME()",
            "CURRENT_TIMESTAMP()",
            "PI()",
            "GENERATE_UUID()",
            "RAND()",
            "custom_func()",
        ] {
            let parsed = parse_expr(sql).expect(sql);
            let expr = ExprPlan::from(translate_expr(&parsed, NO_PARAMS).expect(sql));
            let expected = vec![expr.clone()];
            let mut visited = Vec::new();
            let result: Result<(), &str> = try_visit_expr(&expr, &mut |expr| {
                visited.push(expr.clone());
                Ok(())
            });
            assert_eq!(result, Ok(()), "{sql}");
            assert_eq!(visited, expected, "{sql}");
        }
    }

    #[test]
    fn planned_custom_function() {
        let expr = ExprPlan::Function(Box::new(FunctionExprPlan::Custom {
            function_id: 7,
            exprs: vec![
                ExprPlan::Identifier("a".to_owned()),
                ExprPlan::Identifier("b".to_owned()),
            ],
        }));
        let mut visited = Vec::new();
        let result: Result<(), &str> = try_visit_expr(&expr, &mut |expr| {
            if let ExprPlan::Identifier(name) = expr {
                visited.push(name.clone());
            }
            Ok(())
        });
        assert_eq!(result, Ok(()));
        assert_eq!(visited, ["a", "b"]);
    }
}
