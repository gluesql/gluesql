use {
    super::ExprPlan,
    crate::ast,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProjectionPlan {
    SelectItems(Vec<SelectItemPlan>),
    SchemalessMap,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SelectItemPlan {
    Expr { expr: ExprPlan, label: String },
    QualifiedWildcard(String),
    Wildcard,
}

impl From<ast::Projection> for ProjectionPlan {
    fn from(projection: ast::Projection) -> Self {
        match projection {
            ast::Projection::SelectItems(items) => {
                Self::SelectItems(items.into_iter().map(Into::into).collect())
            }
            ast::Projection::SchemalessMap => Self::SchemalessMap,
        }
    }
}

impl From<ast::SelectItem> for SelectItemPlan {
    fn from(select_item: ast::SelectItem) -> Self {
        match select_item {
            ast::SelectItem::Expr { expr, label } => Self::Expr {
                expr: expr.into(),
                label,
            },
            ast::SelectItem::QualifiedWildcard(table_alias) => Self::QualifiedWildcard(table_alias),
            ast::SelectItem::Wildcard => Self::Wildcard,
        }
    }
}
