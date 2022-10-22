use super::ExprNode;

impl<'a> ExprNode<'a> {
    pub fn like<T: Into<Self>>(self, pattern: T) -> Self {
        Self::Like {
            expr: Box::new(self),
            negated: false,
            pattern: Box::new(pattern.into()),
        }
    }

    pub fn ilike<T: Into<Self>>(self, pattern: T) -> Self {
        Self::ILike {
            expr: Box::new(self),
            negated: false,
            pattern: Box::new(pattern.into()),
        }
    }

    pub fn not_like<T: Into<Self>>(self, pattern: T) -> Self {
        Self::Like {
            expr: Box::new(self),
            negated: true,
            pattern: Box::new(pattern.into()),
        }
    }

    pub fn not_ilike<T: Into<Self>>(self, pattern: T) -> Self {
        Self::ILike {
            expr: Box::new(self),
            negated: true,
            pattern: Box::new(pattern.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{col, test_expr, text};

    #[test]
    fn like_ilike() {
        let actual = col("name").like(text("a%"));
        let expected = "name LIKE 'a%'";
        test_expr(actual, expected);

        let actual = col("name").ilike(text("a%"));
        let expected = "name ILIKE 'a%'";
        test_expr(actual, expected);

        let actual = col("name").not_like(text("a%"));
        let expected = "name NOT LIKE 'a%'";
        test_expr(actual, expected);

        let actual = col("name").not_ilike(text("a%"));
        let expected = "name NOT ILIKE 'a%'";
        test_expr(actual, expected);
    }
}
