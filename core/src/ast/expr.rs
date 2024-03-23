use {
    super::{
        Aggregate, AstLiteral, BinaryOperator, DataType, DateTimeField, Function, Query, ToSql,
        ToSqlUnquoted, UnaryOperator,
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Expr {
    Identifier(String),
    CompoundIdentifier {
        alias: String,
        ident: String,
    },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    Like {
        expr: Box<Expr>,
        negated: bool,
        pattern: Box<Expr>,
    },
    ILike {
        expr: Box<Expr>,
        negated: bool,
        pattern: Box<Expr>,
    },
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    Nested(Box<Expr>),
    Literal(AstLiteral),
    TypedString {
        data_type: DataType,
        value: String,
    },
    Function(Box<Function>),
    Aggregate(Box<Aggregate>),
    Exists {
        subquery: Box<Query>,
        negated: bool,
    },
    Subquery(Box<Query>),
    Case {
        operand: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    ArrayIndex {
        obj: Box<Expr>,
        indexes: Vec<Expr>,
    },
    Interval {
        expr: Box<Expr>,
        leading_field: Option<DateTimeField>,
        last_field: Option<DateTimeField>,
    },
    Array {
        elem: Vec<Expr>,
    },
}

impl ToSql for Expr {
    fn to_sql(&self) -> String {
        self.to_sql_with(true)
    }
}

impl ToSqlUnquoted for Expr {
    fn to_sql_unquoted(&self) -> String {
        self.to_sql_with(false)
    }
}

impl Expr {
    fn to_sql_with(&self, quoted: bool) -> String {
        match self {
            Expr::Identifier(s) => match quoted {
                true => format! {r#""{s}""#},
                false => s.to_owned(),
            },
            Expr::BinaryOp { left, op, right } => {
                format!(
                    "{} {} {}",
                    left.to_sql_with(quoted),
                    op.to_sql(),
                    right.to_sql_with(quoted),
                )
            }
            Expr::CompoundIdentifier { alias, ident } => match quoted {
                true => format!(r#""{alias}"."{ident}""#),
                false => format!("{alias}.{ident}"),
            },
            Expr::IsNull(s) => format!("{} IS NULL", s.to_sql_with(quoted)),
            Expr::IsNotNull(s) => format!("{} IS NOT NULL", s.to_sql_with(quoted)),
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = expr.to_sql_with(quoted);
                let list = list
                    .iter()
                    .map(|expr| expr.to_sql_with(quoted))
                    .collect::<Vec<_>>()
                    .join(", ");

                match negated {
                    true => format!("{expr} NOT IN ({list})"),
                    false => format!("{expr} IN ({list})"),
                }
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = expr.to_sql_with(quoted);
                let low = low.to_sql_with(quoted);
                let high = high.to_sql_with(quoted);

                match negated {
                    true => format!("{expr} NOT BETWEEN {low} AND {high}"),
                    false => format!("{expr} BETWEEN {low} AND {high}"),
                }
            }
            Expr::Like {
                expr,
                negated,
                pattern,
            } => {
                let expr = expr.to_sql_with(quoted);
                let pattern = pattern.to_sql_with(quoted);

                match negated {
                    true => format!("{expr} NOT LIKE {pattern}"),
                    false => format!("{expr} LIKE {pattern}"),
                }
            }
            Expr::ILike {
                expr,
                negated,
                pattern,
            } => {
                let expr = expr.to_sql_with(quoted);
                let pattern = pattern.to_sql_with(quoted);

                match negated {
                    true => format!("{expr} NOT ILIKE {pattern}"),
                    false => format!("{expr} ILIKE {pattern}"),
                }
            }
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Factorial => {
                    format!("{}{}", expr.to_sql_with(quoted), op.to_sql())
                }
                _ => format!("{}{}", op.to_sql(), expr.to_sql_with(quoted)),
            },
            Expr::Nested(expr) => format!("({})", expr.to_sql_with(quoted)),
            Expr::Literal(s) => s.to_sql(),
            Expr::TypedString { data_type, value } => format!("{data_type} '{value}'"),
            Expr::Case {
                operand,
                when_then,
                else_result,
            } => {
                let operand = match operand {
                    Some(operand) => format!("CASE {}", operand.to_sql_with(quoted)),
                    None => "CASE".to_owned(),
                };

                let when_then = when_then
                    .iter()
                    .map(|(when, then)| {
                        format!(
                            "WHEN {} THEN {}",
                            when.to_sql_with(quoted),
                            then.to_sql_with(quoted)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                let else_result = else_result
                    .as_ref()
                    .map(|else_result| format!("ELSE {}", else_result.to_sql_with(quoted)));

                match else_result {
                    Some(else_result) => {
                        [operand, when_then, else_result, "END".to_owned()].join("\n")
                    }
                    None => [operand, when_then, "END".to_owned()].join("\n"),
                }
            }
            Expr::Aggregate(a) => a.to_sql(),
            Expr::Function(func) => func.to_sql(),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => match negated {
                true => format!(
                    "{} NOT IN ({})",
                    expr.to_sql_with(quoted),
                    subquery.to_sql()
                ),
                false => format!("{} IN ({})", expr.to_sql_with(quoted), subquery.to_sql()),
            },
            Expr::Exists { subquery, negated } => match negated {
                true => format!("NOT EXISTS({})", subquery.to_sql()),
                false => format!("EXISTS({})", subquery.to_sql()),
            },
            Expr::ArrayIndex { obj, indexes } => {
                let obj = obj.to_sql_with(quoted);
                let indexes = indexes
                    .iter()
                    .map(|index| format!("[{}]", index.to_sql_with(quoted)))
                    .collect::<Vec<_>>()
                    .join("");
                format!("{obj}{indexes}")
            }
            Expr::Array { elem } => {
                let elem = elem
                    .iter()
                    .map(|e| e.to_sql_with(quoted))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("[{}]", elem)
            }
            Expr::Subquery(query) => format!("({})", query.to_sql()),
            Expr::Interval {
                expr,
                leading_field,
                last_field,
            } => {
                let expr = expr.to_sql_with(quoted);
                let leading_field = leading_field
                    .as_ref()
                    .map(|field| field.to_string())
                    .unwrap_or_else(|| "".to_owned());

                match last_field {
                    Some(last_field) => format!("INTERVAL {expr} {leading_field} TO {last_field}"),
                    None => format!("INTERVAL {expr} {leading_field}"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use {
        crate::ast::{
            AstLiteral, BinaryOperator, DataType, DateTimeField, Expr, Query, Select, SelectItem,
            SetExpr, TableFactor, TableWithJoins, ToSql, ToSqlUnquoted, UnaryOperator,
        },
        bigdecimal::BigDecimal,
        regex::Regex,
        std::str::FromStr,
    };

    #[test]
    fn to_sql() {
        let re = Regex::new(r"\n\s+").unwrap();
        let trim = |s: &str| re.replace_all(s.trim(), "\n").into_owned();

        assert_eq!(r#""id""#, Expr::Identifier("id".to_owned()).to_sql());

        assert_eq!(
            r#""id" + "num""#,
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier("id".to_owned())),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Identifier("num".to_owned()))
            }
            .to_sql()
        );
        assert_eq!(
            r#"-"id""#,
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Identifier("id".to_owned())),
            }
            .to_sql(),
        );

        assert_eq!(
            r#""alias"."column""#,
            Expr::CompoundIdentifier {
                alias: "alias".into(),
                ident: "column".into()
            }
            .to_sql()
        );

        assert_eq!(
            "alias.column",
            Expr::CompoundIdentifier {
                alias: "alias".into(),
                ident: "column".into()
            }
            .to_sql_unquoted()
        );

        let id_expr: Box<Expr> = Box::new(Expr::Identifier("id".to_owned()));
        assert_eq!(r#""id" IS NULL"#, Expr::IsNull(id_expr).to_sql());

        let id_expr: Box<Expr> = Box::new(Expr::Identifier("id".to_owned()));
        assert_eq!(r#""id" IS NOT NULL"#, Expr::IsNotNull(id_expr).to_sql());

        assert_eq!(
            "INT '1'",
            Expr::TypedString {
                data_type: DataType::Int,
                value: "1".to_owned()
            }
            .to_sql()
        );

        assert_eq!(
            r#"("id")"#,
            Expr::Nested(Box::new(Expr::Identifier("id".to_owned()))).to_sql(),
        );

        assert_eq!(
            r#""id" BETWEEN "low" AND "high""#,
            Expr::Between {
                expr: Box::new(Expr::Identifier("id".to_owned())),
                negated: false,
                low: Box::new(Expr::Identifier("low".to_owned())),
                high: Box::new(Expr::Identifier("high".to_owned()))
            }
            .to_sql()
        );

        assert_eq!(
            r#""id" NOT BETWEEN "low" AND "high""#,
            Expr::Between {
                expr: Box::new(Expr::Identifier("id".to_owned())),
                negated: true,
                low: Box::new(Expr::Identifier("low".to_owned())),
                high: Box::new(Expr::Identifier("high".to_owned()))
            }
            .to_sql()
        );

        assert_eq!(
            r#""id" LIKE '%abc'"#,
            Expr::Like {
                expr: Box::new(Expr::Identifier("id".to_owned())),
                negated: false,
                pattern: Box::new(Expr::Literal(AstLiteral::QuotedString("%abc".to_owned()))),
            }
            .to_sql()
        );

        assert_eq!(
            r#""id" NOT LIKE '%abc'"#,
            Expr::Like {
                expr: Box::new(Expr::Identifier("id".to_owned())),
                negated: true,
                pattern: Box::new(Expr::Literal(AstLiteral::QuotedString("%abc".to_owned()))),
            }
            .to_sql()
        );

        assert_eq!(
            r#""id" ILIKE '%abc_'"#,
            Expr::ILike {
                expr: Box::new(Expr::Identifier("id".to_owned())),
                negated: false,
                pattern: Box::new(Expr::Literal(AstLiteral::QuotedString("%abc_".to_owned()))),
            }
            .to_sql()
        );

        assert_eq!(
            r#""id" NOT ILIKE '%abc_'"#,
            Expr::ILike {
                expr: Box::new(Expr::Identifier("id".to_owned())),
                negated: true,
                pattern: Box::new(Expr::Literal(AstLiteral::QuotedString("%abc_".to_owned()))),
            }
            .to_sql()
        );

        assert_eq!(
            r#""id" IN ('a', 'b', 'c')"#,
            Expr::InList {
                expr: Box::new(Expr::Identifier("id".to_owned())),
                list: vec![
                    Expr::Literal(AstLiteral::QuotedString("a".to_owned())),
                    Expr::Literal(AstLiteral::QuotedString("b".to_owned())),
                    Expr::Literal(AstLiteral::QuotedString("c".to_owned()))
                ],
                negated: false
            }
            .to_sql()
        );

        assert_eq!(
            r#""id" NOT IN ('a', 'b', 'c')"#,
            Expr::InList {
                expr: Box::new(Expr::Identifier("id".to_owned())),
                list: vec![
                    Expr::Literal(AstLiteral::QuotedString("a".to_owned())),
                    Expr::Literal(AstLiteral::QuotedString("b".to_owned())),
                    Expr::Literal(AstLiteral::QuotedString("c".to_owned()))
                ],
                negated: true
            }
            .to_sql()
        );

        assert_eq!(
            r#""id" IN (SELECT * FROM "FOO")"#,
            Expr::InSubquery {
                expr: Box::new(Expr::Identifier("id".to_owned())),
                subquery: Box::new(Query {
                    body: SetExpr::Select(Box::new(Select {
                        projection: vec![SelectItem::Wildcard],
                        from: TableWithJoins {
                            relation: TableFactor::Table {
                                name: "FOO".to_owned(),
                                alias: None,
                                index: None,
                            },
                            joins: Vec::new(),
                        },
                        selection: None,
                        group_by: Vec::new(),
                        having: None,
                    })),
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                }),
                negated: false
            }
            .to_sql()
        );

        assert_eq!(
            r#""id" NOT IN (SELECT * FROM "FOO")"#,
            Expr::InSubquery {
                expr: Box::new(Expr::Identifier("id".to_owned())),
                subquery: Box::new(Query {
                    body: SetExpr::Select(Box::new(Select {
                        projection: vec![SelectItem::Wildcard],
                        from: TableWithJoins {
                            relation: TableFactor::Table {
                                name: "FOO".to_owned(),
                                alias: None,
                                index: None,
                            },
                            joins: Vec::new(),
                        },
                        selection: None,
                        group_by: Vec::new(),
                        having: None,
                    })),
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                }),
                negated: true
            }
            .to_sql()
        );

        assert_eq!(
            r#"EXISTS(SELECT * FROM "FOO")"#,
            Expr::Exists {
                subquery: Box::new(Query {
                    body: SetExpr::Select(Box::new(Select {
                        projection: vec![SelectItem::Wildcard],
                        from: TableWithJoins {
                            relation: TableFactor::Table {
                                name: "FOO".to_owned(),
                                alias: None,
                                index: None,
                            },
                            joins: Vec::new(),
                        },
                        selection: None,
                        group_by: Vec::new(),
                        having: None,
                    })),
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                }),
                negated: false,
            }
            .to_sql(),
        );

        assert_eq!(
            r#"NOT EXISTS(SELECT * FROM "FOO")"#,
            Expr::Exists {
                subquery: Box::new(Query {
                    body: SetExpr::Select(Box::new(Select {
                        projection: vec![SelectItem::Wildcard],
                        from: TableWithJoins {
                            relation: TableFactor::Table {
                                name: "FOO".to_owned(),
                                alias: None,
                                index: None,
                            },
                            joins: Vec::new(),
                        },
                        selection: None,
                        group_by: Vec::new(),
                        having: None,
                    })),
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                }),
                negated: true,
            }
            .to_sql(),
        );

        assert_eq!(
            r#"(SELECT * FROM "FOO")"#,
            Expr::Subquery(Box::new(Query {
                body: SetExpr::Select(Box::new(Select {
                    projection: vec![SelectItem::Wildcard],
                    from: TableWithJoins {
                        relation: TableFactor::Table {
                            name: "FOO".to_owned(),
                            alias: None,
                            index: None,
                        },
                        joins: Vec::new(),
                    },
                    selection: None,
                    group_by: Vec::new(),
                    having: None,
                })),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            }))
            .to_sql()
        );

        assert_eq!(
            trim(
                r#"CASE "id"
                  WHEN 1 THEN 'a'
                  WHEN 2 THEN 'b'
                  ELSE 'c'
                END"#,
            ),
            Expr::Case {
                operand: Some(Box::new(Expr::Identifier("id".to_owned()))),
                when_then: vec![
                    (
                        Expr::Literal(AstLiteral::Number(BigDecimal::from_str("1").unwrap())),
                        Expr::Literal(AstLiteral::QuotedString("a".to_owned()))
                    ),
                    (
                        Expr::Literal(AstLiteral::Number(BigDecimal::from_str("2").unwrap())),
                        Expr::Literal(AstLiteral::QuotedString("b".to_owned()))
                    )
                ],
                else_result: Some(Box::new(Expr::Literal(AstLiteral::QuotedString(
                    "c".to_owned()
                ))))
            }
            .to_sql()
        );

        assert_eq!(
            trim(
                r#"CASE
                  WHEN "id" = 1 THEN 'a'
                  WHEN "id" = 2 THEN 'b'
                END"#,
            ),
            Expr::Case {
                operand: None,
                when_then: vec![
                    (
                        Expr::BinaryOp {
                            left: Box::new(Expr::Identifier("id".to_owned())),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Literal(AstLiteral::Number(
                                BigDecimal::from_str("1").unwrap()
                            )))
                        },
                        Expr::Literal(AstLiteral::QuotedString("a".to_owned()))
                    ),
                    (
                        Expr::BinaryOp {
                            left: Box::new(Expr::Identifier("id".to_owned())),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Literal(AstLiteral::Number(
                                BigDecimal::from_str("2").unwrap()
                            )))
                        },
                        Expr::Literal(AstLiteral::QuotedString("b".to_owned()))
                    )
                ],
                else_result: None,
            }
            .to_sql()
        );

        assert_eq!(
            trim(
                r#"CASE "id"
                  WHEN 1 THEN 'a'
                  WHEN 2 THEN 'b'
                END"#,
            ),
            Expr::Case {
                operand: Some(Box::new(Expr::Identifier("id".to_owned()))),
                when_then: vec![
                    (
                        Expr::Literal(AstLiteral::Number(BigDecimal::from_str("1").unwrap())),
                        Expr::Literal(AstLiteral::QuotedString("a".to_owned()))
                    ),
                    (
                        Expr::Literal(AstLiteral::Number(BigDecimal::from_str("2").unwrap())),
                        Expr::Literal(AstLiteral::QuotedString("b".to_owned()))
                    )
                ],
                else_result: None,
            }
            .to_sql()
        );

        assert_eq!(
            r#""choco"[1][2]"#,
            Expr::ArrayIndex {
                obj: Box::new(Expr::Identifier("choco".to_owned())),
                indexes: vec![
                    Expr::Literal(AstLiteral::Number(BigDecimal::from_str("1").unwrap())),
                    Expr::Literal(AstLiteral::Number(BigDecimal::from_str("2").unwrap()))
                ]
            }
            .to_sql()
        );

        assert_eq!(
            r#"['GlueSQL', 'Rust']"#,
            Expr::Array {
                elem: vec![
                    Expr::Literal(AstLiteral::QuotedString("GlueSQL".to_owned())),
                    Expr::Literal(AstLiteral::QuotedString("Rust".to_owned()))
                ]
            }
            .to_sql()
        );

        assert_eq!(
            r#"INTERVAL "col1" + 3 DAY"#,
            &Expr::Interval {
                expr: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("col1".to_owned())),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expr::Literal(AstLiteral::Number(3.into()))),
                }),
                leading_field: Some(DateTimeField::Day),
                last_field: None,
            }
            .to_sql()
        );

        assert_eq!(
            "INTERVAL '3-5' HOUR TO MINUTE",
            &Expr::Interval {
                expr: Box::new(Expr::Literal(AstLiteral::QuotedString("3-5".to_owned()))),
                leading_field: Some(DateTimeField::Hour),
                last_field: Some(DateTimeField::Minute),
            }
            .to_sql()
        );
    }
}
