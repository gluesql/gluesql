use {super::super::VisitorMut, crate::plan::FunctionPlan};

pub fn walk_function_mut<V: VisitorMut + ?Sized>(
    visitor: &mut V,
    function: &mut FunctionPlan,
) -> Result<(), V::Error> {
    match function {
        FunctionPlan::Abs(expr)
        | FunctionPlan::Lower(expr)
        | FunctionPlan::Initcap(expr)
        | FunctionPlan::Upper(expr)
        | FunctionPlan::Asin(expr)
        | FunctionPlan::Acos(expr)
        | FunctionPlan::Atan(expr)
        | FunctionPlan::Ceil(expr)
        | FunctionPlan::Round(expr)
        | FunctionPlan::Trunc(expr)
        | FunctionPlan::Floor(expr)
        | FunctionPlan::Exp(expr)
        | FunctionPlan::Ln(expr)
        | FunctionPlan::Log2(expr)
        | FunctionPlan::Log10(expr)
        | FunctionPlan::Sin(expr)
        | FunctionPlan::Cos(expr)
        | FunctionPlan::Tan(expr)
        | FunctionPlan::Sqrt(expr)
        | FunctionPlan::Radians(expr)
        | FunctionPlan::Degrees(expr)
        | FunctionPlan::LastDay(expr)
        | FunctionPlan::Reverse(expr)
        | FunctionPlan::Sign(expr)
        | FunctionPlan::Ascii(expr)
        | FunctionPlan::Chr(expr)
        | FunctionPlan::Md5(expr)
        | FunctionPlan::Hex(expr)
        | FunctionPlan::IsEmpty(expr)
        | FunctionPlan::Length(expr)
        | FunctionPlan::Entries(expr)
        | FunctionPlan::Keys(expr)
        | FunctionPlan::Values(expr)
        | FunctionPlan::Dedup(expr)
        | FunctionPlan::GetX(expr)
        | FunctionPlan::GetY(expr) => visitor.visit_expr(expr)?,
        FunctionPlan::AddMonth { expr, size }
        | FunctionPlan::Left { expr, size }
        | FunctionPlan::Right { expr, size }
        | FunctionPlan::Repeat { expr, num: size }
        | FunctionPlan::Skip { expr, size }
        | FunctionPlan::Take { expr, size } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(size)?;
        }
        FunctionPlan::Lpad { expr, size, fill } | FunctionPlan::Rpad { expr, size, fill } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(size)?;
            if let Some(e) = fill {
                visitor.visit_expr(e)?;
            }
        }
        FunctionPlan::Replace { expr, old, new } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(old)?;
            visitor.visit_expr(new)?;
        }
        FunctionPlan::Cast { expr, .. } | FunctionPlan::Extract { expr, .. } => {
            visitor.visit_expr(expr)?;
        }
        FunctionPlan::Coalesce(exprs)
        | FunctionPlan::Concat(exprs)
        | FunctionPlan::Greatest(exprs)
        | FunctionPlan::UnplannedCustom { exprs, .. }
        | FunctionPlan::Custom { exprs, .. } => {
            for e in exprs {
                visitor.visit_expr(e)?;
            }
        }
        FunctionPlan::ConcatWs { separator, exprs } => {
            visitor.visit_expr(separator)?;
            for e in exprs {
                visitor.visit_expr(e)?;
            }
        }
        FunctionPlan::IfNull { expr, then } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(then)?;
        }
        FunctionPlan::NullIf { expr1, expr2 } => {
            visitor.visit_expr(expr1)?;
            visitor.visit_expr(expr2)?;
        }
        FunctionPlan::Rand(expr) => {
            if let Some(e) = expr {
                visitor.visit_expr(e)?;
            }
        }
        FunctionPlan::Trim {
            expr, filter_chars, ..
        } => {
            visitor.visit_expr(expr)?;
            if let Some(e) = filter_chars {
                visitor.visit_expr(e)?;
            }
        }
        FunctionPlan::Log { antilog, base } => {
            visitor.visit_expr(antilog)?;
            visitor.visit_expr(base)?;
        }
        FunctionPlan::Div { dividend, divisor } | FunctionPlan::Mod { dividend, divisor } => {
            visitor.visit_expr(dividend)?;
            visitor.visit_expr(divisor)?;
        }
        FunctionPlan::Gcd { left, right } | FunctionPlan::Lcm { left, right } => {
            visitor.visit_expr(left)?;
            visitor.visit_expr(right)?;
        }
        FunctionPlan::Power { expr, power } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(power)?;
        }
        FunctionPlan::Ltrim { expr, chars } | FunctionPlan::Rtrim { expr, chars } => {
            visitor.visit_expr(expr)?;
            if let Some(e) = chars {
                visitor.visit_expr(e)?;
            }
        }
        FunctionPlan::Substr { expr, start, count } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(start)?;
            if let Some(e) = count {
                visitor.visit_expr(e)?;
            }
        }
        FunctionPlan::Unwrap { expr, selector } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(selector)?;
        }
        FunctionPlan::Format { expr, format }
        | FunctionPlan::ToDate { expr, format }
        | FunctionPlan::ToTimestamp { expr, format }
        | FunctionPlan::ToTime { expr, format } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(format)?;
        }
        FunctionPlan::Position {
            from_expr,
            sub_expr,
        } => {
            visitor.visit_expr(from_expr)?;
            visitor.visit_expr(sub_expr)?;
        }
        FunctionPlan::FindIdx {
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
        FunctionPlan::Append { expr, value } | FunctionPlan::Prepend { expr, value } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(value)?;
        }
        FunctionPlan::Sort { expr, order } => {
            visitor.visit_expr(expr)?;
            if let Some(e) = order {
                visitor.visit_expr(e)?;
            }
        }
        FunctionPlan::Slice {
            expr,
            start,
            length,
        } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(start)?;
            visitor.visit_expr(length)?;
        }
        FunctionPlan::Point { x, y } => {
            visitor.visit_expr(x)?;
            visitor.visit_expr(y)?;
        }
        FunctionPlan::CalcDistance {
            geometry1,
            geometry2,
        } => {
            visitor.visit_expr(geometry1)?;
            visitor.visit_expr(geometry2)?;
        }
        FunctionPlan::Splice {
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
        FunctionPlan::Now()
        | FunctionPlan::CurrentDate()
        | FunctionPlan::CurrentTime()
        | FunctionPlan::CurrentTimestamp()
        | FunctionPlan::Pi()
        | FunctionPlan::GenerateUuid() => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::super::{try_visit_mut_expr, visit_mut_expr},
        crate::{
            parse_sql::parse_expr,
            plan::{ExprPlan, FunctionPlan},
            translate::{NO_PARAMS, translate_expr},
        },
    };

    fn test(input: &str, expected: &str) {
        let parsed = parse_expr(input).expect(input);
        let mut expr = ExprPlan::from(translate_expr(&parsed, NO_PARAMS).expect(input));
        visit_mut_expr(&mut expr, &mut |e| {
            if let ExprPlan::Identifier(ident) = e {
                *e = ExprPlan::Identifier(format!("__{ident}__"));
            }
        });

        let expected_parsed = parse_expr(expected).expect(expected);
        let expected = ExprPlan::from(translate_expr(&expected_parsed, NO_PARAMS).expect(expected));

        assert_eq!(expr, expected, "\ninput: {input}\nexpected: {expected:?}");
    }

    #[test]
    fn visit_mut_function_variants() {
        test("ABS(x)", "ABS(__x__)");
        test("ADD_MONTH(d, n)", "ADD_MONTH(__d__, __n__)");
        test("LOWER(s)", "LOWER(__s__)");
        test("INITCAP(s)", "INITCAP(__s__)");
        test("UPPER(s)", "UPPER(__s__)");
        test("LEFT(s, n)", "LEFT(__s__, __n__)");
        test("RIGHT(s, n)", "RIGHT(__s__, __n__)");
        test("ASIN(x)", "ASIN(__x__)");
        test("ACOS(x)", "ACOS(__x__)");
        test("ATAN(x)", "ATAN(__x__)");
        test("LPAD(s, n)", "LPAD(__s__, __n__)");
        test("LPAD(s, n, f)", "LPAD(__s__, __n__, __f__)");
        test("RPAD(s, n)", "RPAD(__s__, __n__)");
        test("RPAD(s, n, f)", "RPAD(__s__, __n__, __f__)");
        test("REPLACE(s, o, n)", "REPLACE(__s__, __o__, __n__)");
        test("CAST(x AS INTEGER)", "CAST(__x__ AS INTEGER)");
        test("CEIL(x)", "CEIL(__x__)");
        test("COALESCE(a, b, c)", "COALESCE(__a__, __b__, __c__)");
        test("CONCAT(a, b)", "CONCAT(__a__, __b__)");
        test("CONCAT_WS(sep, a, b)", "CONCAT_WS(__sep__, __a__, __b__)");
        test("IFNULL(a, b)", "IFNULL(__a__, __b__)");
        test("NULLIF(a, b)", "NULLIF(__a__, __b__)");
        test("RAND()", "RAND()");
        test("RAND(x)", "RAND(__x__)");
        test("ROUND(x)", "ROUND(__x__)");
        test("TRUNC(x)", "TRUNC(__x__)");
        test("FLOOR(x)", "FLOOR(__x__)");
        test("TRIM(s)", "TRIM(__s__)");
        test("TRIM(LEADING c FROM s)", "TRIM(LEADING __c__ FROM __s__)");
        test("EXP(x)", "EXP(__x__)");
        test("EXTRACT(YEAR FROM d)", "EXTRACT(YEAR FROM __d__)");
        test("LN(x)", "LN(__x__)");
        test("LOG(a, b)", "LOG(__a__, __b__)");
        test("LOG2(x)", "LOG2(__x__)");
        test("LOG10(x)", "LOG10(__x__)");
        test("DIV(a, b)", "DIV(__a__, __b__)");
        test("MOD(a, b)", "MOD(__a__, __b__)");
        test("GCD(a, b)", "GCD(__a__, __b__)");
        test("LCM(a, b)", "LCM(__a__, __b__)");
        test("SIN(x)", "SIN(__x__)");
        test("COS(x)", "COS(__x__)");
        test("TAN(x)", "TAN(__x__)");
        test("SQRT(x)", "SQRT(__x__)");
        test("POWER(a, b)", "POWER(__a__, __b__)");
        test("RADIANS(x)", "RADIANS(__x__)");
        test("DEGREES(x)", "DEGREES(__x__)");
        test("NOW()", "NOW()");
        test("CURRENT_DATE()", "CURRENT_DATE()");
        test("CURRENT_TIME()", "CURRENT_TIME()");
        test("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP()");
        test("PI()", "PI()");
        test("LAST_DAY(d)", "LAST_DAY(__d__)");
        test("LTRIM(s)", "LTRIM(__s__)");
        test("LTRIM(s, c)", "LTRIM(__s__, __c__)");
        test("RTRIM(s)", "RTRIM(__s__)");
        test("RTRIM(s, c)", "RTRIM(__s__, __c__)");
        test("REVERSE(s)", "REVERSE(__s__)");
        test("REPEAT(s, n)", "REPEAT(__s__, __n__)");
        test("SIGN(x)", "SIGN(__x__)");
        test("SUBSTR(s, i)", "SUBSTR(__s__, __i__)");
        test("SUBSTR(s, i, n)", "SUBSTR(__s__, __i__, __n__)");
        test("UNWRAP(m, k)", "UNWRAP(__m__, __k__)");
        test("GENERATE_UUID()", "GENERATE_UUID()");
        test("GREATEST(a, b, c)", "GREATEST(__a__, __b__, __c__)");
        test("FORMAT(d, f)", "FORMAT(__d__, __f__)");
        test("TO_DATE(s, f)", "TO_DATE(__s__, __f__)");
        test("TO_TIMESTAMP(s, f)", "TO_TIMESTAMP(__s__, __f__)");
        test("TO_TIME(s, f)", "TO_TIME(__s__, __f__)");
        test("POSITION(a IN b)", "POSITION(__a__ IN __b__)");
        test("FIND_IDX(a, b)", "FIND_IDX(__a__, __b__)");
        test("FIND_IDX(a, b, n)", "FIND_IDX(__a__, __b__, __n__)");
        test("ASCII(s)", "ASCII(__s__)");
        test("CHR(n)", "CHR(__n__)");
        test("MD5(s)", "MD5(__s__)");
        test("HEX(x)", "HEX(__x__)");
        test("APPEND(l, v)", "APPEND(__l__, __v__)");
        test("SORT(l)", "SORT(__l__)");
        test("SORT(l, o)", "SORT(__l__, __o__)");
        test("SLICE(l, s, n)", "SLICE(__l__, __s__, __n__)");
        test("PREPEND(l, v)", "PREPEND(__l__, __v__)");
        test("SKIP(l, n)", "SKIP(__l__, __n__)");
        test("TAKE(l, n)", "TAKE(__l__, __n__)");
        test("GET_X(p)", "GET_X(__p__)");
        test("GET_Y(p)", "GET_Y(__p__)");
        test("POINT(x, y)", "POINT(__x__, __y__)");
        test(
            "CALC_DISTANCE(POINT(a, b), POINT(c, d))",
            "CALC_DISTANCE(POINT(__a__, __b__), POINT(__c__, __d__))",
        );
        test("IS_EMPTY(l)", "IS_EMPTY(__l__)");
        test("LENGTH(s)", "LENGTH(__s__)");
        test("ENTRIES(m)", "ENTRIES(__m__)");
        test("KEYS(m)", "KEYS(__m__)");
        test("VALUES(m)", "VALUES(__m__)");
        test("SPLICE(l, a, b)", "SPLICE(__l__, __a__, __b__)");
        test("SPLICE(l, a, b, v)", "SPLICE(__l__, __a__, __b__, __v__)");
        test("DEDUP(l)", "DEDUP(__l__)");
        test("custom_func()", "custom_func()");
        test("custom_func(a, b)", "custom_func(__a__, __b__)");
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
        let mut expr = ExprPlan::Function(Box::new(FunctionPlan::Custom {
            function_id: 7,
            exprs: vec![
                ExprPlan::Identifier("a".to_owned()),
                ExprPlan::Identifier("b".to_owned()),
            ],
        }));
        let expected = ExprPlan::Function(Box::new(FunctionPlan::Custom {
            function_id: 7,
            exprs: vec![
                ExprPlan::Identifier("__a__".to_owned()),
                ExprPlan::Identifier("__b__".to_owned()),
            ],
        }));
        visit_mut_expr(&mut expr, &mut |expr| {
            if let ExprPlan::Identifier(name) = expr {
                *name = format!("__{name}__");
            }
        });
        assert_eq!(expr, expected);
    }
}
