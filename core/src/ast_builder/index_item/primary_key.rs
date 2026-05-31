use {super::IndexItemNode, crate::ast_builder::ExprNode};

#[derive(Clone, Debug)]
pub struct PrimaryKeyNode;

impl<'a> PrimaryKeyNode {
    pub fn eq<T: Into<ExprNode<'a>>>(self, expr: T) -> IndexItemNode<'a> {
        IndexItemNode::PrimaryKey(expr.into())
    }
}

/// Entry point function to Primary Key
pub fn primary_key() -> PrimaryKeyNode {
    PrimaryKeyNode
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::Literal,
        ast_builder::primary_key,
        plan::{ExprPlan, IndexItemPlan},
    };

    #[test]
    fn test() {
        let actual = primary_key().eq("1").build_index_item_plan().unwrap();
        let expected = IndexItemPlan::PrimaryKey(ExprPlan::Literal(Literal::Number(1.into())));
        assert_eq!(actual, expected);
    }
}
