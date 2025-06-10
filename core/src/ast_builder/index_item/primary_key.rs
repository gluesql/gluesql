use {
    super::IndexItemNode,
    crate::ast_builder::{DataTypeNode, ExprNode},
};

#[derive(Clone, Debug)]
pub struct PrimaryKeyNode {
    data_type: DataTypeNode,
}

impl PrimaryKeyNode {
    pub fn new<T: Into<DataTypeNode>>(data_type: T) -> Self {
        Self {
            data_type: data_type.into(),
        }
    }
}

impl<'a> PrimaryKeyNode {
    pub fn eq<T: Into<ExprNode<'a>>>(self, expr: T) -> IndexItemNode<'a> {
        IndexItemNode::PrimaryKey {
            data_type: self.data_type,
            expr: expr.into(),
        }
    }
}

/// Entry point function to Primary Key
pub fn primary_key<T: Into<DataTypeNode>>(data_type: T) -> PrimaryKeyNode {
    PrimaryKeyNode::new(data_type)
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{AstLiteral, DataType, Expr},
        ast_builder::{index_item::IndexItem, primary_key, select::Prebuild},
    };

    #[test]
    fn test() {
        let actual = primary_key("INTEGER").eq("1").prebuild().unwrap();
        let expected = IndexItem::PrimaryKey {
            data_type: DataType::Int,
            expr: Expr::Literal(AstLiteral::Number(1.into())),
        };
        assert_eq!(actual, expected);
    }
}
