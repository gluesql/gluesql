use super::ExprNode;

impl ExprNode<'_> {
    #[must_use]
    pub fn regex<T: Into<Self>>(self, pattern: T) -> Self {
        Self::Regex {
            expr: Box::new(self),
            negated: false,
            pattern: Box::new(pattern.into()),
            case_sensitive: true,
        }
    }

    #[must_use]
    pub fn iregex<T: Into<Self>>(self, pattern: T) -> Self {
        Self::Regex {
            expr: Box::new(self),
            negated: false,
            pattern: Box::new(pattern.into()),
            case_sensitive: false,
        }
    }

    #[must_use]
    pub fn not_regex<T: Into<Self>>(self, pattern: T) -> Self {
        Self::Regex {
            expr: Box::new(self),
            negated: true,
            pattern: Box::new(pattern.into()),
            case_sensitive: true,
        }
    }

    #[must_use]
    pub fn not_iregex<T: Into<Self>>(self, pattern: T) -> Self {
        Self::Regex {
            expr: Box::new(self),
            negated: true,
            pattern: Box::new(pattern.into()),
            case_sensitive: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::query_builder::{col, test_expr, text};

    #[test]
    fn regex() {
        test_expr(col("name").regex(text("a")), "name ~ 'a'");
        test_expr(col("name").iregex(text("a")), "name ~* 'a'");
        test_expr(col("name").not_regex(text("a")), "name !~ 'a'");
        test_expr(col("name").not_iregex(text("a")), "name !~* 'a'");
    }
}
