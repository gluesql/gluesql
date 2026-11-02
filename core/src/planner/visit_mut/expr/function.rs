use {super::super::VisitorMut, crate::plan::FunctionExprPlan};

pub fn walk_function_mut<V: VisitorMut + ?Sized>(
    visitor: &mut V,
    function: &mut FunctionExprPlan,
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
        super::super::{try_visit_mut_expr, visit_mut_expr},
        crate::{
            parse_sql::parse_expr,
            plan::{ExprPlan, FunctionExprPlan},
            translate::{NO_PARAMS, translate_expr},
        },
    };

    fn test(input: &str, expected: &str) {
        let parsed = parse_expr(input).expect(input);
        let mut expr = ExprPlan::from(translate_expr(&parsed, NO_PARAMS).expect(input));
        visit_mut_expr(&mut expr, &mut |e| {
            if let ExprPlan::Identifier(ident) = e {
                *e = ExprPlan::Identifier(format!("_{ident}"));
            }
        });

        let expected_parsed = parse_expr(expected).expect(expected);
        let expected = ExprPlan::from(translate_expr(&expected_parsed, NO_PARAMS).expect(expected));

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
        test("custom_func()", "custom_func()");
        test("custom_func(a, b)", "custom_func(_a, _b)");
    }

    #[test]
    fn try_visit_mut_function_short_circuits_after_error() {
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
                let mut expr = ExprPlan::from(translate_expr(&parsed, NO_PARAMS).expect(sql));
                let mut visited = Vec::new();
                let actual = try_visit_mut_expr(&mut expr, &mut |expr| {
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
            let mut expr = ExprPlan::from(translate_expr(&parsed, NO_PARAMS).expect(sql));
            let expected = vec![expr.clone()];
            let mut visited = Vec::new();
            visit_mut_expr(&mut expr, &mut |expr| visited.push(expr.clone()));
            assert_eq!(visited, expected, "{sql}");
        }
    }

    #[test]
    fn planned_custom_function() {
        let mut expr = ExprPlan::Function(Box::new(FunctionExprPlan::Custom {
            function_id: 7,
            exprs: vec![
                ExprPlan::Identifier("a".to_owned()),
                ExprPlan::Identifier("b".to_owned()),
            ],
        }));
        let expected = ExprPlan::Function(Box::new(FunctionExprPlan::Custom {
            function_id: 7,
            exprs: vec![
                ExprPlan::Identifier("_a".to_owned()),
                ExprPlan::Identifier("_b".to_owned()),
            ],
        }));
        visit_mut_expr(&mut expr, &mut |expr| {
            if let ExprPlan::Identifier(name) = expr {
                *name = format!("_{name}");
            }
        });
        assert_eq!(expr, expected);
    }
}
