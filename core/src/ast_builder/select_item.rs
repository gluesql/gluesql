use {
    super::ExprNode,
    crate::{
        ast::{SelectItem, ToSqlUnquoted},
        ast_builder::{AstBuilderError, ExprWithAliasNode},
        parse_sql::parse_select_item,
        plan::SelectItemPlan,
        result::{Error, Result},
        translate::{NO_PARAMS, translate_select_item},
    },
};

#[derive(Clone, Debug)]
pub enum SelectItemNode<'a> {
    SelectItem(SelectItem),
    Expr(ExprNode<'a>),
    Text(String),
    ExprWithAliasNode(ExprWithAliasNode<'a>),
}

impl From<SelectItem> for SelectItemNode<'_> {
    fn from(select_item: SelectItem) -> Self {
        Self::SelectItem(select_item)
    }
}

impl<'a> From<ExprNode<'a>> for SelectItemNode<'a> {
    fn from(expr_node: ExprNode<'a>) -> Self {
        Self::Expr(expr_node)
    }
}

impl From<&str> for SelectItemNode<'_> {
    fn from(select_item: &str) -> Self {
        Self::Text(select_item.to_owned())
    }
}

impl<'a> From<ExprWithAliasNode<'a>> for SelectItemNode<'a> {
    fn from(expr_node: ExprWithAliasNode<'a>) -> Self {
        Self::ExprWithAliasNode(expr_node)
    }
}

impl SelectItemNode<'_> {
    pub(super) fn build_select_item_plan(self) -> Result<SelectItemPlan> {
        match self {
            SelectItemNode::SelectItem(select_item) => Ok(select_item.into()),
            SelectItemNode::Text(select_item) => parse_select_item(select_item)
                .and_then(|item| translate_select_item(&item, NO_PARAMS).map(Into::into)),
            SelectItemNode::Expr(expr_node) => {
                let expr = expr_node
                    .clone()
                    .build_expr()
                    .map_err(|error| match error {
                        Error::AstBuilder(
                            AstBuilderError::HashJoinExecutorRequiresPlan
                            | AstBuilderError::IndexByRequiresPlan,
                        ) => AstBuilderError::ProjectionLabelRequiresAlias.into(),
                        error => error,
                    })?;
                let label = expr.to_sql_unquoted();
                let expr = expr_node.build_expr_plan()?;

                Ok(SelectItemPlan::Expr { expr, label })
            }
            SelectItemNode::ExprWithAliasNode(alias_node) => {
                let (expr, label) = alias_node.build_expr_with_alias_plan()?;

                Ok(SelectItemPlan::Expr { expr, label })
            }
        }
    }

    pub(super) fn build_select_item(self) -> Result<SelectItem> {
        match self {
            SelectItemNode::SelectItem(select_item) => Ok(select_item),
            SelectItemNode::Text(select_item) => parse_select_item(select_item)
                .and_then(|item| translate_select_item(&item, NO_PARAMS)),
            SelectItemNode::Expr(expr_node) => {
                let expr = expr_node.build_expr()?;
                let label = expr.to_sql_unquoted();

                Ok(SelectItem::Expr { expr, label })
            }
            SelectItemNode::ExprWithAliasNode(alias_node) => {
                let (expr, label) = alias_node.build_expr_with_alias()?;

                Ok(SelectItem::Expr { expr, label })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast::SelectItem,
            ast_builder::{AstBuilderError, SelectItemNode, col, primary_key, subquery, table},
            parse_sql::parse_select_item,
            plan::SelectItemPlan,
            result::Error,
            translate::{NO_PARAMS, translate_select_item},
        },
        pretty_assertions::assert_eq,
    };

    fn test(actual: SelectItemNode, expected: &str) {
        let parsed = &parse_select_item(expected).expect(expected);
        let expected = translate_select_item(parsed, NO_PARAMS).map(SelectItemPlan::from);
        assert_eq!(actual.build_select_item_plan(), expected);
    }

    #[test]
    fn select_item() {
        let actual = SelectItem::Wildcard.into();
        let expected = "*";
        test(actual, expected);

        let actual = "Foo.*".into();
        let expected = "Foo.*";
        test(actual, expected);

        let actual = "id as hello".into();
        let expected = "id as hello";
        test(actual, expected);

        let actual = col("id").into();
        let expected = "id";
        test(actual, expected);
    }

    #[test]
    fn plan_only_projection_expr_requires_alias_for_label() {
        let actual: SelectItemNode = subquery(
            table("Player")
                .select()
                .join("PlayerItem")
                .hash_executor("PlayerItem.user_id", "Player.id"),
        )
        .into();

        assert_eq!(
            actual.build_select_item_plan(),
            Err(Error::AstBuilder(
                AstBuilderError::ProjectionLabelRequiresAlias
            ))
        );

        let actual: SelectItemNode = subquery(
            table("Player")
                .select()
                .join("PlayerItem")
                .hash_executor("PlayerItem.user_id", "Player.id"),
        )
        .alias_as("matched")
        .into();

        assert!(matches!(
            actual.build_select_item_plan(),
            Ok(SelectItemPlan::Expr { label, .. }) if label == "matched"
        ));

        let actual: SelectItemNode =
            subquery(table("Player").index_by(primary_key().eq("1")).select()).into();

        assert_eq!(
            actual.build_select_item_plan(),
            Err(Error::AstBuilder(
                AstBuilderError::ProjectionLabelRequiresAlias
            ))
        );

        let actual: SelectItemNode = subquery(
            table("Player")
                .index_by(primary_key().eq("1"))
                .select()
                .project("id"),
        )
        .alias_as("indexed")
        .into();

        assert!(matches!(
            actual.build_select_item_plan(),
            Ok(SelectItemPlan::Expr { label, .. }) if label == "indexed"
        ));
    }
}
