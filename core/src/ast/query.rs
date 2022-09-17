use {
    super::{Expr, IndexOperator, ObjectName},
    crate::ast::ToSql,
    itertools::Itertools,
    serde::{Deserialize, Serialize},
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
    QualifiedWildcard(ObjectName),
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
        name: ObjectName,
        alias: Option<TableAlias>,
        /// Query planner result
        index: Option<IndexItem>,
    },
    Derived {
        subquery: Query,
        alias: TableAlias,
    },
    Series {
        name: ObjectName,
        alias: Option<TableAlias>,
        size: Expr,
    },
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

impl ToSql for OrderByExpr {
    fn to_sql(&self) -> String {
        let OrderByExpr { expr, asc } = self;

        let result = match asc {
            Some(true) => " ASC".to_string(),
            Some(false) => " DESC".to_string(),
            None => "".to_string(),
        };

        format!("{}{}", expr.to_sql(), result)
    }
}

impl ToSql for Values {
    fn to_sql(&self) -> String {
        match self {
            Values(exprs) => {
                let expr = exprs
                    .iter()
                    .map(|expr| {
                        expr.iter()
                            .enumerate()
                            .map(|(index, item)| check_sql(index, item))
                            .join(", ")
                    })
                    .join("");

                fn check_sql(index: usize, item: &Expr) -> String {
                    let sql = item.to_sql();

                    if sql.is_empty() {
                        return format!("({}, NULL)", index + 1);
                    }
                    return format!("({}, '{}')", index + 1, sql);
                }

                let mut value = String::from("VALUES ");
                value.push_str(&expr);
                value
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{Expr, OrderByExpr, ToSql, Values};

    #[test]
    fn to_sql_order_by_expr() {
        let actual = "foo ASC".to_string();
        let expected = OrderByExpr {
            expr: Expr::Identifier("foo".to_string()),
            asc: Some(true),
        }
        .to_sql();
        assert_eq!(actual, expected);

        let actual = "foo DESC".to_string();
        let expected = OrderByExpr {
            expr: Expr::Identifier("foo".to_string()),
            asc: Some(false),
        }
        .to_sql();
        assert_eq!(actual, expected);

        let actual = "foo".to_string();
        let expected = OrderByExpr {
            expr: Expr::Identifier("foo".to_string()),
            asc: None,
        }
        .to_sql();
        assert_eq!(actual, expected);
    }

    #[test]
    fn to_sql_values() {
        let value = vec![vec![
            Expr::Identifier("foo".to_string()),
            Expr::Identifier("bar".to_string()),
            Expr::Identifier("oh".to_string()),
        ]];

        let actual = "VALUES (1, 'foo'), (2, 'bar'), (3, 'oh')".to_string();
        let expected = Values(value).to_sql();

        assert_eq!(actual, expected);
    }
}
