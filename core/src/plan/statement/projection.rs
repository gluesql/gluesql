use {
    super::ExprPlan,
    crate::{
        ast,
        plan::explain::{Explain, ExplainContext},
    },
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

impl Explain for ProjectionPlan {
    type Output = String;

    fn explain(&self, context: &mut ExplainContext) -> String {
        let mut output = String::new();
        match self {
            ProjectionPlan::SchemalessMap => output.push_str("map"),
            ProjectionPlan::SelectItems(items) => {
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        output.push_str(", ");
                    }
                    output.push_str(&item.explain(context));
                }
            }
        }
        output
    }
}

impl Explain for SelectItemPlan {
    type Output = String;

    fn explain(&self, context: &mut ExplainContext) -> String {
        match self {
            Self::Wildcard => "*".to_owned(),
            Self::QualifiedWildcard(alias) => format!("{alias}.*"),
            Self::Expr { expr, label } => {
                let mut output = expr.explain(context);
                let natural_label = match expr {
                    ExprPlan::Identifier(ident) | ExprPlan::CompoundIdentifier { ident, .. } => {
                        Some(ident.as_str())
                    }
                    _ => None,
                };
                if !label.is_empty() && natural_label != Some(label.as_str()) {
                    output.push_str(" AS ");
                    output.push_str(label);
                }
                output
            }
        }
    }
}

impl From<ast::Projection> for ProjectionPlan {
    fn from(projection: ast::Projection) -> Self {
        match projection {
            ast::Projection::SelectItems(items) => {
                Self::SelectItems(items.into_iter().map(Into::into).collect())
            }
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

#[cfg(test)]
mod tests {
    use {
        super::{ProjectionPlan, SelectItemPlan},
        crate::{
            ast::Literal,
            plan::{
                ExprPlan,
                explain::{Explain, ExplainContext},
            },
        },
    };

    #[test]
    fn explain() {
        let actual = ProjectionPlan::SchemalessMap;
        let expected = "map";
        assert_eq!(actual.explain(&mut ExplainContext::default()), expected);

        let actual = ProjectionPlan::SelectItems(vec![
            SelectItemPlan::Wildcard,
            SelectItemPlan::QualifiedWildcard("Player".to_owned()),
            SelectItemPlan::Expr {
                expr: ExprPlan::Identifier("team_id".to_owned()),
                label: "team_id".to_owned(),
            },
            SelectItemPlan::Expr {
                expr: ExprPlan::Identifier("name".to_owned()),
                label: "player_name".to_owned(),
            },
            SelectItemPlan::Expr {
                expr: ExprPlan::CompoundIdentifier {
                    alias: "Player".to_owned(),
                    ident: "id".to_owned(),
                },
                label: "id".to_owned(),
            },
            SelectItemPlan::Expr {
                expr: ExprPlan::Literal(Literal::Number(1.into())),
                label: String::new(),
            },
        ]);
        let expected = "*, Player.*, team_id, name AS player_name, Player.id, 1";

        assert_eq!(actual.explain(&mut ExplainContext::default()), expected);
    }
}
