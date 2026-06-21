use {
    super::{ExprNode, SelectItemNode},
    crate::{
        ast::SelectItem,
        parse_sql::parse_select_items,
        plan::SelectItemPlan,
        query_builder::ExprWithAliasNode,
        result::Result,
        translate::{NO_PARAMS, translate_select_item},
    },
};

#[derive(Clone, Debug)]
pub enum SelectItemList<'a> {
    Text(String),
    SelectItems(Vec<SelectItemNode<'a>>),
}

impl From<&str> for SelectItemList<'_> {
    fn from(exprs: &str) -> Self {
        SelectItemList::Text(exprs.to_owned())
    }
}

impl From<Vec<&str>> for SelectItemList<'_> {
    fn from(select_items: Vec<&str>) -> Self {
        SelectItemList::SelectItems(select_items.into_iter().map(Into::into).collect())
    }
}

impl<'a> From<ExprNode<'a>> for SelectItemList<'a> {
    fn from(expr_node: ExprNode<'a>) -> Self {
        SelectItemList::SelectItems(vec![expr_node.into()])
    }
}

impl<'a> From<Vec<ExprNode<'a>>> for SelectItemList<'a> {
    fn from(expr_nodes: Vec<ExprNode<'a>>) -> Self {
        SelectItemList::SelectItems(expr_nodes.into_iter().map(Into::into).collect())
    }
}

impl<'a> From<ExprWithAliasNode<'a>> for SelectItemList<'a> {
    fn from(expr_node: ExprWithAliasNode<'a>) -> Self {
        SelectItemList::SelectItems(vec![expr_node.into()])
    }
}

impl<'a> From<Vec<ExprWithAliasNode<'a>>> for SelectItemList<'a> {
    fn from(expr_nodes: Vec<ExprWithAliasNode<'a>>) -> Self {
        SelectItemList::SelectItems(expr_nodes.into_iter().map(Into::into).collect())
    }
}

impl SelectItemList<'_> {
    pub(super) fn build_select_items_plan(self) -> Result<Vec<SelectItemPlan>> {
        match self {
            SelectItemList::Text(items) => parse_select_items(items)?
                .iter()
                .map(|item| translate_select_item(item, NO_PARAMS).map(Into::into))
                .collect::<Result<Vec<_>>>(),
            SelectItemList::SelectItems(items) => items
                .into_iter()
                .map(SelectItemNode::build_select_item_plan)
                .collect::<Result<Vec<_>>>(),
        }
    }

    pub(super) fn build_select_items(self) -> Result<Vec<SelectItem>> {
        match self {
            SelectItemList::Text(items) => parse_select_items(items)?
                .iter()
                .map(|item| translate_select_item(item, NO_PARAMS))
                .collect::<Result<Vec<_>>>(),
            SelectItemList::SelectItems(items) => items
                .into_iter()
                .map(SelectItemNode::build_select_item)
                .collect::<Result<Vec<_>>>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            parse_sql::parse_select_items,
            plan::SelectItemPlan,
            query_builder::{SelectItemList, col, expr},
            result::Result,
            translate::{NO_PARAMS, translate_select_item},
        },
        pretty_assertions::assert_eq,
    };

    fn test(actual: SelectItemList, expected: &str) {
        let parsed = parse_select_items(expected).expect(expected);
        let expected = parsed
            .iter()
            .map(|item| translate_select_item(item, NO_PARAMS).map(SelectItemPlan::from))
            .collect::<Result<Vec<SelectItemPlan>>>();

        assert_eq!(actual.build_select_items_plan(), expected);
    }

    #[test]
    fn select_item_list() {
        let actual = "id, name".into();
        let expected = "id, name";
        test(actual, expected);

        let actual = vec!["id", "name"].into();
        let expected = "id, name";
        test(actual, expected);

        let actual = col("id").into();
        let expected = "id";
        test(actual, expected);

        let actual = vec![col("id"), "name".into()].into();
        let expected = "id, name";
        test(actual, expected);

        let actual = col("id").sub(1).alias_as("new_id").into();
        let expected = "id - 1 AS new_id";
        test(actual, expected);

        let actual = vec![
            col("age").avg().alias_as("avg_age"),
            expr("name || ':foo'").alias_as("res"),
        ]
        .into();
        let expected = "AVG(age) AS avg_age, name || ':foo' AS res";
        test(actual, expected);
    }
}
