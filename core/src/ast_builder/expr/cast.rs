use {super::ExprNode, crate::ast_builder::DataTypeNode};

impl ExprNode {
    pub fn cast<T: Into<DataTypeNode>>(self, data_type: T) -> Self {
        Self::Cast {
            expr: Box::new(self),
            data_type: data_type.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::DataType,
        ast_builder::{col, test_expr},
    };

    #[test]
    fn cast() {
        let actual = col("date").cast(DataType::Int);
        let expected = "CAST(date AS INTEGER)";
        test_expr(actual, expected);

        let actual = col("date").cast("INTEGER");
        let expected = "CAST(date AS INTEGER)";
        test_expr(actual, expected);
    }
}
