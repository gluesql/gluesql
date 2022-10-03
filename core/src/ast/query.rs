use {
    super::{Expr, IndexOperator},
    crate::ast::ToSql,
    itertools::Itertools,
    serde::{Deserialize, Serialize},
    strum_macros::Display,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Query {
    pub body: SetExpr,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SetExpr {
    Select(Box<Select>),
    Values(Values),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Select {
    pub projection: Vec<SelectItem>,
    pub from: TableWithJoins,
    /// WHERE
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SelectItem {
    /// An expression
    Expr { expr: Expr, label: String },
    /// `alias.*` or even `schema.table.*`
    QualifiedWildcard(String),
    /// An unqualified `*`
    Wildcard,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableWithJoins {
    pub relation: TableFactor,
    pub joins: Vec<Join>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexItem {
    PrimaryKey(Expr),
    NonClustered {
        name: String,
        asc: Option<bool>,
        cmp_expr: Option<(IndexOperator, Expr)>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableFactor {
    Table {
        name: String,
        alias: Option<TableAlias>,
        /// Query planner result
        index: Option<IndexItem>,
    },
    Derived {
        subquery: Query,
        alias: TableAlias,
    },
    Series {
        alias: TableAlias,
        size: Expr,
    },
    Dictionary {
        dict: Dictionary,
        alias: TableAlias,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum Dictionary {
    GlueTables,
    GlueTableColumns,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableAlias {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Join {
    pub relation: TableFactor,
    pub join_operator: JoinOperator,
    pub join_executor: JoinExecutor,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinExecutor {
    NestedLoop,
    Hash {
        key_expr: Expr,
        value_expr: Expr,
        where_clause: Option<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinOperator {
    Inner(JoinConstraint),
    LeftOuter(JoinConstraint),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinConstraint {
    On(Expr),
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Values(pub Vec<Vec<Expr>>);

impl ToSql for Query {
    fn to_sql(&self) -> String {
        let Query {
            body,
            order_by,
            limit,
            offset,
        } = self;

        let order_by = if order_by.is_empty() {
            "".to_owned()
        } else {
            format!(
                "ORDER BY {}",
                order_by.iter().map(|expr| expr.to_sql()).join(" ")
            )
        };

        let limit = match limit {
            Some(expr) => format!("LIMIT {}", expr.to_sql()),
            _ => "".to_owned(),
        };

        let offset = match offset {
            Some(expr) => format!("OFFSET {}", expr.to_sql()),
            _ => "".to_owned(),
        };

        let string = vec![order_by, limit, offset]
            .iter()
            .filter(|sql| !sql.is_empty())
            .join(" ");

        if string.is_empty() {
            body.to_sql()
        } else {
            format!("{} {}", body.to_sql(), string)
        }
    }
}

impl ToSql for SetExpr {
    fn to_sql(&self) -> String {
        match self {
            SetExpr::Select(expr) => expr.to_sql(),
            SetExpr::Values(_value) => "(..value..)".to_owned(),
        }
    }
}

impl ToSql for Select {
    fn to_sql(&self) -> String {
        let Select {
            projection,
            from,
            selection,
            group_by,
            having,
        } = self;
        let projection = projection.iter().map(|item| item.to_sql()).join(", ");

        let selection = match selection {
            Some(expr) => format!("WHERE {}", expr.to_sql()),
            None => "".to_owned(),
        };

        let group_by = if group_by.is_empty() {
            "".to_owned()
        } else {
            format!(
                "GROUP BY {}",
                group_by.iter().map(|item| item.to_sql()).join(", ")
            )
        };

        let having = match having {
            Some(having) => format!("HAVING {}", having.to_sql()),
            None => "".to_owned(),
        };

        let condition = vec![selection, group_by, having]
            .iter()
            .filter(|sql| !sql.is_empty())
            .join(" ");

        if condition.is_empty() {
            format!("SELECT {projection} FROM {}", from.to_sql())
        } else {
            format!("SELECT {projection} FROM {} {condition}", from.to_sql())
        }
    }
}

impl ToSql for SelectItem {
    fn to_sql(&self) -> String {
        match self {
            SelectItem::Expr { expr, label } => {
                let expr = expr.to_sql();
                format!("{} AS {}", expr, label)
            }
            SelectItem::QualifiedWildcard(obj) => format!("{}.*", obj),
            SelectItem::Wildcard => "*".to_owned(),
        }
    }
}

impl ToSql for TableWithJoins {
    fn to_sql(&self) -> String {
        let TableWithJoins { relation, joins } = self;

        if joins.is_empty() {
            relation.to_sql()
        } else {
            format!("{} (..join..)", relation.to_sql())
        }
    }
}

impl ToSql for TableFactor {
    fn to_sql(&self) -> String {
        match self {
            TableFactor::Table { name, alias, .. } => match alias {
                Some(alias) => format!("{} {}", name, alias.to_sql()),
                None => name.to_owned(),
            },
            TableFactor::Derived { subquery, alias } => {
                format!("({}) {}", subquery.to_sql(), alias.to_sql())
            }
            TableFactor::Series { alias, size } => {
                format!("SERIES({}) {}", size.to_sql(), alias.to_sql())
            }
            TableFactor::Dictionary { dict, alias } => {
                format!("{dict} {}", alias.to_sql())
            }
        }
    }
}

impl ToSql for TableAlias {
    fn to_sql(&self) -> String {
        let TableAlias { name, .. } = self;

        format!("AS {}", name)
    }
}

impl ToSql for OrderByExpr {
    fn to_sql(&self) -> String {
        let OrderByExpr { expr, asc } = self;
        let expr = expr.to_sql();

        match asc {
            Some(true) => format!("{} ASC", expr),
            Some(false) => format!("{} DESC", expr),
            None => expr,
        }
    }
}

#[cfg(test)]
mod tests {

    use {
        crate::ast::{
            AstLiteral, BinaryOperator, Dictionary, Expr, OrderByExpr, Query, Select, SelectItem,
            SetExpr, TableAlias, TableFactor, TableWithJoins, ToSql,
        },
        bigdecimal::BigDecimal,
        std::str::FromStr,
    };

    #[test]
    fn to_sql_query() {
        let order_by = vec![OrderByExpr {
            expr: Expr::Identifier("name".to_owned()),
            asc: Some(true),
        }];
        let actual = "SELECT * FROM FOO AS F ORDER BY name ASC LIMIT 10 OFFSET 3".to_owned();
        let expected = Query {
            body: SetExpr::Select(Box::new(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: "FOO".to_owned(),
                        alias: Some(TableAlias {
                            name: "F".to_owned(),
                            columns: Vec::new(),
                        }),
                        index: None,
                    },
                    joins: Vec::new(),
                },
                selection: None,
                group_by: Vec::new(),
                having: None,
            })),
            order_by,
            limit: Some(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_str("10").unwrap(),
            ))),
            offset: Some(Expr::Literal(AstLiteral::Number(
                BigDecimal::from_str("3").unwrap(),
            ))),
        }
        .to_sql();
        assert_eq!(actual, expected);
    }

    #[test]
    fn to_sql_select() {
        let actual = "SELECT * FROM FOO AS F GROUP BY \"name\" HAVING name = \"glue\"";
        let expected = Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "FOO".to_owned(),
                    alias: Some(TableAlias {
                        name: "F".to_owned(),
                        columns: Vec::new(),
                    }),
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: None,
            group_by: vec![Expr::Literal(AstLiteral::QuotedString("name".to_owned()))],
            having: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("name".to_owned())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Literal(AstLiteral::QuotedString("glue".to_owned()))),
            }),
        }
        .to_sql();
        assert_eq!(actual, expected);

        let actual = "SELECT * FROM FOO WHERE name = \"glue\"";
        let expected = Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "FOO".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("name".to_owned())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Literal(AstLiteral::QuotedString("glue".to_owned()))),
            }),
            group_by: Vec::new(),
            having: None,
        }
        .to_sql();
        assert_eq!(actual, expected);
    }

    #[test]
    fn to_sql_select_item() {
        let actual = "name AS n".to_owned();
        let expected = SelectItem::Expr {
            expr: Expr::Identifier("name".to_owned()),
            label: "n".to_owned(),
        }
        .to_sql();
        assert_eq!(actual, expected);

        let actual = "foo.*".to_owned();
        let expected = SelectItem::QualifiedWildcard("foo".to_owned()).to_sql();
        assert_eq!(actual, expected);

        let actual = "*".to_owned();
        let expected = SelectItem::Wildcard.to_sql();
        assert_eq!(actual, expected);
    }

    #[test]
    fn to_sql_table_with_joins() {
        let actual = "FOO AS F";
        let expected = TableWithJoins {
            relation: TableFactor::Table {
                name: "FOO".to_owned(),
                alias: Some(TableAlias {
                    name: "F".to_owned(),
                    columns: Vec::new(),
                }),
                index: None,
            },
            joins: Vec::new(),
        }
        .to_sql();
        assert_eq!(actual, expected);
    }

    #[test]
    fn to_sql_table_factor() {
        let actual = "FOO AS F";
        let expected = TableFactor::Table {
            name: "FOO".to_owned(),
            alias: Some(TableAlias {
                name: "F".to_owned(),
                columns: Vec::new(),
            }),
            index: None,
        }
        .to_sql();
        assert_eq!(actual, expected);

        let actual = "(SELECT * FROM FOO) AS F";
        let expected = TableFactor::Derived {
            subquery: Query {
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
            },
            alias: TableAlias {
                name: "F".to_owned(),
                columns: Vec::new(),
            },
        }
        .to_sql();
        assert_eq!(actual, expected);

        let actual = "SERIES(3) AS S";
        let expected = TableFactor::Series {
            alias: TableAlias {
                name: "S".to_owned(),
                columns: Vec::new(),
            },
            size: Expr::Literal(AstLiteral::Number(BigDecimal::from_str("3").unwrap())),
        }
        .to_sql();
        assert_eq!(actual, expected);

        let actual = "GLUE_TABLES AS glue";
        let expected = TableFactor::Dictionary {
            dict: Dictionary::GlueTables,
            alias: TableAlias {
                name: "glue".to_owned(),
                columns: Vec::new(),
            },
        }
        .to_sql();
        assert_eq!(actual, expected);
    }

    #[test]
    fn to_sql_table_alias() {
        let actual = "AS F";
        let expected = TableAlias {
            name: "F".to_owned(),
            columns: Vec::new(),
        }
        .to_sql();
        assert_eq!(actual, expected);
    }

    #[test]
    fn to_sql_order_by_expr() {
        let actual = "foo ASC".to_owned();
        let expected = OrderByExpr {
            expr: Expr::Identifier("foo".to_owned()),
            asc: Some(true),
        }
        .to_sql();
        assert_eq!(actual, expected);

        let actual = "foo DESC".to_owned();
        let expected = OrderByExpr {
            expr: Expr::Identifier("foo".to_owned()),
            asc: Some(false),
        }
        .to_sql();
        assert_eq!(actual, expected);

        let actual = "foo".to_owned();
        let expected = OrderByExpr {
            expr: Expr::Identifier("foo".to_owned()),
            asc: None,
        }
        .to_sql();
        assert_eq!(actual, expected);
    }
}
