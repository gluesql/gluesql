mod derived;
mod dictionary;
mod series;
mod table;

pub use {
    derived::DerivedSourcePlan,
    dictionary::DictionarySourcePlan,
    series::SeriesSourcePlan,
    table::{IndexPredicatePlan, TableAccessPlan, TableSourcePlan},
};

use {
    crate::ast,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SourcePlan {
    Table(TableSourcePlan),
    Derived(DerivedSourcePlan),
    Series(SeriesSourcePlan),
    Dictionary(DictionarySourcePlan),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableAliasPlan {
    pub name: String,
    pub columns: Vec<String>,
}

impl SourcePlan {
    pub fn alias_name(&self) -> &str {
        match self {
            Self::Table(TableSourcePlan { name, alias, .. }) => alias
                .as_ref()
                .map_or(name.as_str(), |alias| alias.name.as_str()),
            Self::Derived(DerivedSourcePlan {
                alias: TableAliasPlan { name, .. },
                ..
            })
            | Self::Series(SeriesSourcePlan {
                alias: TableAliasPlan { name, .. },
                ..
            })
            | Self::Dictionary(DictionarySourcePlan {
                alias: TableAliasPlan { name, .. },
                ..
            }) => name.as_str(),
        }
    }
}

impl From<ast::TableFactor> for SourcePlan {
    fn from(source: ast::TableFactor) -> Self {
        match source {
            ast::TableFactor::Table { name, alias } => Self::Table(TableSourcePlan {
                name,
                alias: alias.map(Into::into),
                access: TableAccessPlan::FullScan,
            }),
            ast::TableFactor::Derived { subquery, alias } => Self::Derived(DerivedSourcePlan {
                query: Box::new(subquery.into()),
                alias: alias.into(),
            }),
            ast::TableFactor::Series { alias, size } => Self::Series(SeriesSourcePlan {
                alias: alias.into(),
                size: size.into(),
            }),
            ast::TableFactor::Dictionary { dict, alias } => {
                Self::Dictionary(DictionarySourcePlan {
                    dictionary: dict,
                    alias: alias.into(),
                })
            }
        }
    }
}

impl From<ast::TableAlias> for TableAliasPlan {
    fn from(alias: ast::TableAlias) -> Self {
        let ast::TableAlias { name, columns } = alias;

        Self { name, columns }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            DerivedSourcePlan, DictionarySourcePlan, SeriesSourcePlan, SourcePlan, TableAccessPlan,
            TableAliasPlan, TableSourcePlan,
        },
        crate::{
            ast::{self, Dictionary, Expr, Literal, Query, SetExpr, Values},
            plan::{ExprPlan, QueryPlan},
        },
        pretty_assertions::assert_eq,
    };

    fn alias(name: &str) -> ast::TableAlias {
        ast::TableAlias {
            name: name.to_owned(),
            columns: Vec::new(),
        }
    }

    #[test]
    fn converts_each_ast_source_to_typed_plan() {
        let actual = SourcePlan::from(ast::TableFactor::Table {
            name: "Item".to_owned(),
            alias: Some(alias("i")),
        });
        let expected = SourcePlan::Table(TableSourcePlan {
            name: "Item".to_owned(),
            alias: Some(TableAliasPlan {
                name: "i".to_owned(),
                columns: Vec::new(),
            }),
            access: TableAccessPlan::FullScan,
        });
        assert_eq!(actual, expected);

        let query = Query {
            body: SetExpr::Values(Values(vec![vec![Expr::Literal(Literal::Number(1.into()))]])),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };
        let actual = SourcePlan::from(ast::TableFactor::Derived {
            subquery: query.clone(),
            alias: alias("derived"),
        });
        let expected = SourcePlan::Derived(DerivedSourcePlan {
            query: Box::new(QueryPlan::from(query)),
            alias: TableAliasPlan {
                name: "derived".to_owned(),
                columns: Vec::new(),
            },
        });
        assert_eq!(actual, expected);

        let actual = SourcePlan::from(ast::TableFactor::Series {
            alias: alias("series"),
            size: Expr::Literal(Literal::Number(3.into())),
        });
        let expected = SourcePlan::Series(SeriesSourcePlan {
            alias: TableAliasPlan {
                name: "series".to_owned(),
                columns: Vec::new(),
            },
            size: ExprPlan::Literal(Literal::Number(3.into())),
        });
        assert_eq!(actual, expected);

        let actual = SourcePlan::from(ast::TableFactor::Dictionary {
            dict: Dictionary::GlueTables,
            alias: alias("tables"),
        });
        let expected = SourcePlan::Dictionary(DictionarySourcePlan {
            dictionary: Dictionary::GlueTables,
            alias: TableAliasPlan {
                name: "tables".to_owned(),
                columns: Vec::new(),
            },
        });
        assert_eq!(actual, expected);
    }
}
