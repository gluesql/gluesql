use {
    crate::ast::ToSql,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
    Factorial,
}

impl ToSql for UnaryOperator {
    fn to_sql(&self) -> String {
        match self {
            UnaryOperator::Plus => "+".to_string(),
            UnaryOperator::Minus => "-".to_string(),
            UnaryOperator::Not => "NOT ".to_string(),
            UnaryOperator::Factorial => "!".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    StringConcat,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Eq,
    NotEq,
    And,
    Or,
    Xor,
    Like,
    ILike,
    NotLike,
    NotILike,
}

impl ToSql for BinaryOperator {
    fn to_sql(&self) -> String {
        match self {
            BinaryOperator::Plus => "+".to_string(),
            BinaryOperator::Minus => "-".to_string(),
            BinaryOperator::Multiply => "*".to_string(),
            BinaryOperator::Divide => "/".to_string(),
            BinaryOperator::Modulo => "%".to_string(),
            BinaryOperator::StringConcat => "+".to_string(),
            BinaryOperator::Gt => ">".to_string(),
            BinaryOperator::Lt => "<".to_string(),
            BinaryOperator::GtEq => ">=".to_string(),
            BinaryOperator::LtEq => "<=".to_string(),
            BinaryOperator::Eq => "=".to_string(),
            BinaryOperator::NotEq => "<>".to_string(),
            BinaryOperator::And => "AND".to_string(),
            BinaryOperator::Or => "OR".to_string(),
            BinaryOperator::Xor => "XOR".to_string(),
            BinaryOperator::Like => "LIKE".to_string(),
            BinaryOperator::ILike => "ILIKE".to_string(),
            BinaryOperator::NotLike => "NOT LIKE".to_string(),
            BinaryOperator::NotILike => "NOT ILIKE".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexOperator {
    Gt,
    Lt,
    GtEq,
    LtEq,
    Eq,
}

impl IndexOperator {
    pub fn reverse(self) -> Self {
        use IndexOperator::*;

        match self {
            Gt => Lt,
            Lt => Gt,
            GtEq => LtEq,
            LtEq => GtEq,
            Eq => Eq,
        }
    }
}

impl From<IndexOperator> for BinaryOperator {
    fn from(index_op: IndexOperator) -> Self {
        match index_op {
            IndexOperator::Gt => BinaryOperator::Gt,
            IndexOperator::Lt => BinaryOperator::Lt,
            IndexOperator::GtEq => BinaryOperator::GtEq,
            IndexOperator::LtEq => BinaryOperator::LtEq,
            IndexOperator::Eq => BinaryOperator::Eq,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{BinaryOperator, Expr, ToSql, UnaryOperator};
    #[test]
    fn to_sql() {
        assert_eq!(
            "1 + 2",
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier(1.to_string())),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Identifier(2.to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            "100 - 10",
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier(100.to_string())),
                op: BinaryOperator::Minus,
                right: Box::new(Expr::Identifier(10.to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            "1024 * 1024",
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier(1024.to_string())),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Identifier(1024.to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            "1024 / 8",
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier(1024.to_string())),
                op: BinaryOperator::Divide,
                right: Box::new(Expr::Identifier(8.to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            "1024 % 4",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier(1024.to_string())),
                op: BinaryOperator::Modulo,
                right: Box::new(Expr::Identifier(4.to_string()))
            }
            .to_sql()
        );

        assert_eq!(
            "Glue + SQL",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier("Glue".to_string())),
                op: BinaryOperator::StringConcat,
                right: Box::new(Expr::Identifier("SQL".to_string()))
            }
            .to_sql()
        );
        assert_eq!(
            "1024 > 4",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier(1024.to_string())),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Identifier(4.to_string()))
            }
            .to_sql()
        );
        assert_eq!(
            "8 < 1024",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier(8.to_string())),
                op: BinaryOperator::Lt,
                right: Box::new(Expr::Identifier(1024.to_string()))
            }
            .to_sql()
        );
        assert_eq!(
            "1024 >= 1024",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier(1024.to_string())),
                op: BinaryOperator::GtEq,
                right: Box::new(Expr::Identifier(1024.to_string()))
            }
            .to_sql()
        );
        assert_eq!(
            "8 <= 8",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier(8.to_string())),
                op: BinaryOperator::LtEq,
                right: Box::new(Expr::Identifier(8.to_string()))
            }
            .to_sql()
        );
        assert_eq!(
            "1024 = 1024",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier(1024.to_string())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Identifier(1024.to_string()))
            }
            .to_sql()
        );
        assert_eq!(
            "1024 <> 1024",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier(1024.to_string())),
                op: BinaryOperator::NotEq,
                right: Box::new(Expr::Identifier(1024.to_string()))
            }
            .to_sql()
        );
        assert_eq!(
            "condition_0 AND condition_1",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier("condition_0".to_string())),
                op: BinaryOperator::And,
                right: Box::new(Expr::Identifier("condition_1".to_string()))
            }
            .to_sql()
        );
        assert_eq!(
            "condition_0 OR condition_1",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier("condition_0".to_string())),
                op: BinaryOperator::Or,
                right: Box::new(Expr::Identifier("condition_1".to_string()))
            }
            .to_sql()
        );
        assert_eq!(
            "condition_0 XOR condition_1",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier("condition_0".to_string())),
                op: BinaryOperator::Xor,
                right: Box::new(Expr::Identifier("condition_1".to_string()))
            }
            .to_sql()
        );
        assert_eq!(
            "column_0 LIKE pattern",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier("column_0".to_string())),
                op: BinaryOperator::Like,
                right: Box::new(Expr::Identifier("pattern".to_string()))
            }
            .to_sql()
        );
        assert_eq!(
            "column_0 ILIKE pattern",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier("column_0".to_string())),
                op: BinaryOperator::ILike,
                right: Box::new(Expr::Identifier("pattern".to_string()))
            }
            .to_sql()
        );
        assert_eq!(
            "column_0 NOT LIKE pattern",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier("column_0".to_string())),
                op: BinaryOperator::NotLike,
                right: Box::new(Expr::Identifier("pattern".to_string()))
            }
            .to_sql()
        );
        assert_eq!(
            "column_0 NOT ILIKE num",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier("column_0".to_string())),
                op: BinaryOperator::NotILike,
                right: Box::new(Expr::Identifier("num".to_string()))
            }
            .to_sql()
        );
        assert_eq!(
            "+8",
            Expr::UnaryOp {
                op: UnaryOperator::Plus,
                expr: Box::new(Expr::Identifier("8".to_owned())),
            }
            .to_sql(),
        );

        assert_eq!(
            "-8",
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Identifier("8".to_owned())),
            }
            .to_sql(),
        );

        assert_eq!(
            "NOT id",
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(Expr::Identifier("id".to_owned())),
            }
            .to_sql(),
        );

        assert_eq!(
            "5!",
            Expr::UnaryOp {
                op: UnaryOperator::Factorial,
                expr: Box::new(Expr::Identifier("5".to_owned())),
            }
            .to_sql(),
        )
    }
}
