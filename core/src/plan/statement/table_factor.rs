use {
    super::{ExprPlan, JoinPlan, QueryPlan},
    crate::ast,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableWithJoinsPlan {
    pub relation: TableFactorPlan,
    pub joins: Vec<JoinPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexItemPlan {
    PrimaryKey(ExprPlan),
    NonClustered {
        name: String,
        asc: Option<bool>,
        cmp_expr: Option<(ast::IndexOperator, ExprPlan)>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableFactorPlan {
    Table {
        name: String,
        alias: Option<TableAliasPlan>,
        index: Option<IndexItemPlan>,
    },
    Derived {
        subquery: QueryPlan,
        alias: TableAliasPlan,
    },
    Series {
        alias: TableAliasPlan,
        size: ExprPlan,
    },
    Dictionary {
        dict: ast::Dictionary,
        alias: TableAliasPlan,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableAliasPlan {
    pub name: String,
    pub columns: Vec<String>,
}

impl TableFactorPlan {
    pub fn alias_name(&self) -> &str {
        match self {
            Self::Table {
                name, alias: None, ..
            }
            | Self::Table {
                alias: Some(TableAliasPlan { name, .. }),
                ..
            }
            | Self::Derived {
                alias: TableAliasPlan { name, .. },
                ..
            }
            | Self::Series {
                alias: TableAliasPlan { name, .. },
                ..
            }
            | Self::Dictionary {
                alias: TableAliasPlan { name, .. },
                ..
            } => name.as_str(),
        }
    }

    pub fn index(&self) -> Option<&IndexItemPlan> {
        match self {
            Self::Table { index, .. } => index.as_ref(),
            Self::Derived { .. } | Self::Series { .. } | Self::Dictionary { .. } => None,
        }
    }
}

impl From<ast::TableWithJoins> for TableWithJoinsPlan {
    fn from(table_with_joins: ast::TableWithJoins) -> Self {
        let ast::TableWithJoins { relation, joins } = table_with_joins;

        Self {
            relation: relation.into(),
            joins: joins.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<ast::TableFactor> for TableFactorPlan {
    fn from(table_factor: ast::TableFactor) -> Self {
        match table_factor {
            ast::TableFactor::Table { name, alias } => Self::Table {
                name,
                alias: alias.map(Into::into),
                index: None,
            },
            ast::TableFactor::Derived { subquery, alias } => Self::Derived {
                subquery: subquery.into(),
                alias: alias.into(),
            },
            ast::TableFactor::Series { alias, size } => Self::Series {
                alias: alias.into(),
                size: size.into(),
            },
            ast::TableFactor::Dictionary { dict, alias } => Self::Dictionary {
                dict,
                alias: alias.into(),
            },
        }
    }
}

impl From<ast::TableAlias> for TableAliasPlan {
    fn from(alias: ast::TableAlias) -> Self {
        let ast::TableAlias { name, columns } = alias;

        Self { name, columns }
    }
}
