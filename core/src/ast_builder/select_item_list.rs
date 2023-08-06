use {
    super::{ExprNode, SelectItemNode},
    crate::{
        ast::SelectItem,
        ast_builder::ExprWithAliasNode,
        parse_sql::parse_select_items,
        result::{Error, Result},
        translate::translate_select_item,
    },
};

#[derive(Clone, Debug)]
pub enum SelectItemList<'a> {
    Text(String),
    SelectItems(Vec<SelectItemNode<'a>>),
}

impl<'a> From<&str> for SelectItemList<'a> {
    fn from(exprs: &str) -> Self {
        SelectItemList::Text(exprs.to_owned())
    }
}

impl<'a> From<Vec<&str>> for SelectItemList<'a> {
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

impl<'a> TryFrom<SelectItemList<'a>> for Vec<SelectItem> {
    type Error = Error;

    fn try_from(select_items: SelectItemList<'a>) -> Result<Self> {
        match select_items {
            SelectItemList::Text(items) => parse_select_items(items)?
                .iter()
                .map(translate_select_item)
                .collect::<Result<Vec<_>>>(),
            SelectItemList::SelectItems(items) => items
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>>>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast::SelectItem,
            ast_builder::{col, expr, SelectItemList},
            parse_sql::parse_select_items,
            result::Result,
            translate::translate_select_item,
        },
        pretty_assertions::assert_eq,
    };

    fn test(actual: SelectItemList, expected: &str) {
        let parsed = parse_select_items(expected).expect(expected);
        let expected = parsed
            .iter()
            .map(translate_select_item)
            .collect::<Result<Vec<SelectItem>>>();

        assert_eq!(actual.try_into(), expected);
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
