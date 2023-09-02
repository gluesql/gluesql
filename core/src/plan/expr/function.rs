use {
    crate::ast::{Expr, Function},
    std::iter::{empty, once},
};

impl Function {
    pub fn as_exprs(&self) -> impl Iterator<Item = &Expr> {
        #[derive(iter_enum::Iterator)]
        enum Exprs<I0, I1, I2, I3, I4, I5, I6> {
            Empty(I0),
            Single(I1),
            Double(I2),
            Triple(I3),
            VariableArgs(I4),
            VariableArgsWithSingle(I5),
            Quadruple(I6),
        }

        match self {
            Self::Now() | Function::Pi() | Function::GenerateUuid() | Self::Rand(None) => {
                Exprs::Empty(empty())
            }
            Self::Lower(expr)
            | Self::Length(expr)
            | Self::Initcap(expr)
            | Self::Upper(expr)
            | Self::Sin(expr)
            | Self::Cos(expr)
            | Self::Tan(expr)
            | Self::Asin(expr)
            | Self::Acos(expr)
            | Self::Atan(expr)
            | Self::Radians(expr)
            | Self::Degrees(expr)
            | Self::Ceil(expr)
            | Self::Rand(Some(expr))
            | Self::Round(expr)
            | Self::Floor(expr)
            | Self::Exp(expr)
            | Self::Ln(expr)
            | Self::Log2(expr)
            | Self::Log10(expr)
            | Self::Sqrt(expr)
            | Self::Abs(expr)
            | Self::Sign(expr)
            | Self::Ascii(expr)
            | Self::Chr(expr)
            | Self::Md5(expr)
            | Self::LastDay(expr)
            | Self::Ltrim { expr, chars: None }
            | Self::Rtrim { expr, chars: None }
            | Self::Trim {
                expr,
                filter_chars: None,
                ..
            }
            | Self::Reverse(expr)
            | Self::Cast { expr, .. }
            | Self::Extract { expr, .. }
            | Self::GetX(expr)
            | Self::GetY(expr)
            | Self::IsEmpty(expr)
            | Self::Sort { expr, order: None }
            | Self::Values(expr) => Exprs::Single([expr].into_iter()),
            Self::Left { expr, size: expr2 }
            | Self::Right { expr, size: expr2 }
            | Self::Lpad {
                expr,
                size: expr2,
                fill: None,
            }
            | Self::Rpad {
                expr,
                size: expr2,
                fill: None,
            }
            | Self::Trim {
                expr,
                filter_chars: Some(expr2),
                ..
            }
            | Self::Log {
                antilog: expr,
                base: expr2,
            }
            | Self::Div {
                dividend: expr,
                divisor: expr2,
            }
            | Self::Mod {
                dividend: expr,
                divisor: expr2,
            }
            | Self::Gcd {
                left: expr,
                right: expr2,
            }
            | Self::Lcm {
                left: expr,
                right: expr2,
            }
            | Self::Format {
                expr,
                format: expr2,
            }
            | Self::ToDate {
                expr,
                format: expr2,
            }
            | Self::ToTimestamp {
                expr,
                format: expr2,
            }
            | Self::ToTime {
                expr,
                format: expr2,
            }
            | Self::Power { expr, power: expr2 }
            | Self::Ltrim {
                expr,
                chars: Some(expr2),
            }
            | Self::Rtrim {
                expr,
                chars: Some(expr2),
            }
            | Self::Repeat { expr, num: expr2 }
            | Self::Substr {
                expr,
                start: expr2,
                count: None,
            }
            | Self::IfNull { expr, then: expr2 }
            | Self::Unwrap {
                expr,
                selector: expr2,
            }
            | Self::Position {
                from_expr: expr2,
                sub_expr: expr,
            }
            | Self::FindIdx {
                from_expr: expr,
                sub_expr: expr2,
                start: None,
            }
            | Self::Append { expr, value: expr2 }
            | Self::Prepend { expr, value: expr2 }
            | Self::Skip { expr, size: expr2 }
            | Self::Sort {
                expr,
                order: Some(expr2),
            }
            | Self::Take { expr, size: expr2 }
            | Self::Point { x: expr, y: expr2 }
            | Self::CalcDistance {
                geometry1: expr,
                geometry2: expr2,
            }
            | Self::AddMonth { expr, size: expr2 } => Exprs::Double([expr, expr2].into_iter()),

            Self::Lpad {
                expr,
                size: expr2,
                fill: Some(expr3),
            }
            | Self::Rpad {
                expr,
                size: expr2,
                fill: Some(expr3),
            }
            | Self::Substr {
                expr,
                start: expr2,
                count: Some(expr3),
            }
            | Self::Replace {
                expr,
                old: expr2,
                new: expr3,
            }
            | Self::Slice {
                expr,
                start: expr2,
                length: expr3,
            }
            | Self::FindIdx {
                from_expr: expr,
                sub_expr: expr2,
                start: Some(expr3),
            }
            | Self::Splice {
                list_data: expr,
                begin_index: expr2,
                end_index: expr3,
                values: None,
            } => Exprs::Triple([expr, expr2, expr3].into_iter()),
            Self::Custom { name: _, exprs } => Exprs::VariableArgs(exprs.iter()),
            Self::Coalesce(exprs) => Exprs::VariableArgs(exprs.iter()),
            Self::Concat(exprs) => Exprs::VariableArgs(exprs.iter()),
            Self::ConcatWs { separator, exprs } => {
                Exprs::VariableArgsWithSingle(once(separator).chain(exprs.iter()))
            }
            Self::Greatest(exprs) => Exprs::VariableArgs(exprs.iter()),
            Self::Entries(expr) => Exprs::Single([expr].into_iter()),
            Self::Splice {
                list_data: expr,
                begin_index: expr2,
                end_index: expr3,
                values: Some(expr4),
            } => Exprs::Quadruple([expr, expr2, expr3, expr4].into_iter()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{ast::Expr, parse_sql::parse_expr, translate::translate_expr};

    fn expr(sql: &str) -> Expr {
        let parsed = parse_expr(sql).expect(sql);

        translate_expr(&parsed).expect(sql)
    }

    fn test(sql: &str, expected: &[&str]) {
        let function = match expr(sql) {
            Expr::Function(function) => *function,
            _ => unreachable!("only for function tests"),
        };
        let actual = function.as_exprs();
        let actual = actual.collect::<Vec<_>>();

        assert_eq!(actual.len(), expected.len(), "{sql}");

        for (expected, actual) in expected.iter().zip(actual.into_iter()) {
            assert_eq!(actual, &expr(expected), "{sql}");
        }
    }

    #[test]
    fn as_exprs() {
        // Empty
        test("NOW()", &[]);
        test("PI()", &[]);
        test("GENERATE_UUID()", &[]);
        test("RAND()", &[]);
        test("CUSTOM_FUNC()", &[]);

        // Single
        test("LOWER(id)", &["id"]);
        test("INITCAP(id)", &["id"]);
        test(r#"UPPER("Hello")"#, &[r#""Hello""#]);
        test("SIN(3.14)", &["3.14"]);
        test("COS(3.14)", &["3.14"]);
        test("TAN(3.14)", &["3.14"]);
        test("ASIN(3.14)", &["3.14"]);
        test("ACOS(3.14)", &["3.14"]);
        test("ATAN(3.14)", &["3.14"]);
        test("RADIANS(180)", &["180"]);
        test("DEGREES(3.14)", &["3.14"]);
        test("CEIL(1.23)", &["1.23"]);
        test("Rand(1.23)", &["1.23"]);
        test("ROUND(1.23)", &["1.23"]);
        test("FLOOR(1.23)", &["1.23"]);
        test("EXP(1.23)", &["1.23"]);
        test("LN(col + 1)", &["col + 1"]);
        test("LOG2(16)", &["16"]);
        test("LOG10(150 - 50)", &["150 - 50"]);
        test("SQRT(144)", &["144"]);
        test("LASTDAY(DATE '2020-01-01')", &[r#"DATE '2020-01-01'"#]);
        test(r#"LTRIM("  hello")"#, &[r#""  hello""#]);
        test(r#"RTRIM("world  ")"#, &[r#""world  ""#]);
        test(r#"TRIM("  rust  ")"#, &[r#""  rust  ""#]);
        test(r#"REVERSE("abcde")"#, &[r#""abcde""#]);
        test(r#"CAST(1 AS BOOLEAN)"#, &["1"]);
        test(r#"IS_EMPTY(col)"#, &["col"]);
        test(r#"VALUES(col)"#, &["col"]);

        test(r#"ABS(1)"#, &["1"]);
        test(r#"ABS(-1)"#, &["-1"]);
        test(r#"ABS(2)"#, &["2"]);
        test(r#"ABS(-2)"#, &["-2"]);
        test(r#"ABS(3.0)"#, &["3.0"]);
        test(r#"ABS(-3.0)"#, &["-3.0"]);

        test(r#"SIGN(1)"#, &["1"]);
        test(r#"SIGN(-1)"#, &["-1"]);
        test(r#"SIGN(2)"#, &["2"]);
        test(r#"SIGN(-2)"#, &["-2"]);
        test(r#"SIGN(3.0)"#, &["3.0"]);
        test(r#"SIGN(-3.0)"#, &["-3.0"]);

        // Double
        test(r#"LEFT("hello", 2)"#, &[r#""hello""#, "2"]);
        test(r#"RIGHT("hello", 2)"#, &[r#""hello""#, "2"]);
        test(r#"FIND_IDX("Calzone", "zone")"#, &[r#"Calzone"#, r#"zone"#]);
        test(r#"TAKE(list, 3)"#, &[r#"list"#, r#"3"#]);
        test(r#"LPAD(value, 5)"#, &["value", "5"]);
        test(r#"RPAD(value, 5)"#, &["value", "5"]);
        test(
            r#"TRIM(LEADING "_" FROM "__hello")"#,
            &[r#""__hello""#, r#""_""#],
        );
        test("LOG(rate, 2)", &["rate", "2"]);
        test("DIV(6, 2)", &["6", "2"]);
        test("MOD(6, 2)", &["6", "2"]);
        test("GCD(6, 2)", &["6", "2"]);
        test("LCM(6, 2)", &["6", "2"]);
        test("POWER(base, 10)", &["base", "10"]);
        test(r#"LTRIM(name, "xyz")"#, &["name", r#""xyz""#]);
        test(r#"RTRIM(name, "xyz")"#, &["name", r#""xyz""#]);
        test("REPEAT(col || col2, 3)", &["col || col2", "3"]);
        test("REPEAT(column, 2)", &["column", "2"]);
        test(r#"UNWRAP(field, "foo.1")"#, &["field", r#""foo.1""#]);
        test(r#"SKIP(list, 2)"#, &[r#""list""#, r#"2"#]);

        // Triple
        test(
            r#"LPAD(name, 20, '>")++++<')"#,
            &["name", "20", r#"'>")++++<'"#],
        );
        test(
            r#"RPAD(name, 20, '>")++++<')"#,
            &["name", "20", r#"'>")++++<'"#],
        );
        test(
            r#"SUBSTR('   >++++("<   ', 3, 11)"#,
            &[r#"'   >++++("<   '"#, "3", "11"],
        );
        test(r#"SPLICE(list, 2, 4)"#, &["list", "2", "4"]);

        // Quadruple
        test(
            r#"SPLICE(list, 3, 5, values)"#,
            &["list", "3", "5", "values"],
        );

        //VariableArgs
        test(r#"COALESCE("test")"#, &[r#""test""#]);

        test(r#"COALESCE(NULL, "test")"#, &["NULL", r#""test""#]);

        test(r#"CONCAT("abc")"#, &[r#""abc""#]);

        test(r#"CONCAT("abc", "123")"#, &[r#""abc""#, r#""123""#]);

        test(r#"CONCAT("a", "b", "c")"#, &[r#""a""#, r#""b""#, r#""c""#]);

        test(
            r#"CUSTOM_FUNC("a", "b", "c")"#,
            &[r#""a""#, r#""b""#, r#""c""#],
        );

        test(
            r#"CONCAT("gluesql", " ", "is", " ", "cool")"#,
            &[r#""gluesql""#, r#"" ""#, r#""is""#, r#"" ""#, r#""cool""#],
        );

        test(r#"POSITION("men" IN "ramen")"#, &[r#""men""#, r#""ramen""#]);
        test(r#"POSITION("men" IN ramen)"#, &[r#""men""#, "ramen"]);

        //TypedStringVariableArgs
        test(
            r#"CONCAT_WS(",", "gluesql", "is", "cool")"#,
            &[r#"",""#, r#""gluesql""#, r#""is""#, r#""cool""#],
        );
    }
}
