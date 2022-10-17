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
            UnaryOperator::Plus => "+".to_owned(),
            UnaryOperator::Minus => "-".to_owned(),
            UnaryOperator::Not => "NOT ".to_owned(),
            UnaryOperator::Factorial => "!".to_owned(),
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
}

impl ToSql for BinaryOperator {
    fn to_sql(&self) -> String {
        match self {
            BinaryOperator::Plus => "+".to_owned(),
            BinaryOperator::Minus => "-".to_owned(),
            BinaryOperator::Multiply => "*".to_owned(),
            BinaryOperator::Divide => "/".to_owned(),
            BinaryOperator::Modulo => "%".to_owned(),
            BinaryOperator::StringConcat => "+".to_owned(),
            BinaryOperator::Gt => ">".to_owned(),
            BinaryOperator::Lt => "<".to_owned(),
            BinaryOperator::GtEq => ">=".to_owned(),
            BinaryOperator::LtEq => "<=".to_owned(),
            BinaryOperator::Eq => "=".to_owned(),
            BinaryOperator::NotEq => "<>".to_owned(),
            BinaryOperator::And => "AND".to_owned(),
            BinaryOperator::Or => "OR".to_owned(),
            BinaryOperator::Xor => "XOR".to_owned(),
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
                left: Box::new(Expr::Identifier("Glue".to_owned())),
                op: BinaryOperator::StringConcat,
                right: Box::new(Expr::Identifier("SQL".to_owned()))
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
                left: Box::new(Expr::Identifier("condition_0".to_owned())),
                op: BinaryOperator::And,
                right: Box::new(Expr::Identifier("condition_1".to_owned()))
            }
            .to_sql()
        );
        assert_eq!(
            "condition_0 OR condition_1",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier("condition_0".to_owned())),
                op: BinaryOperator::Or,
                right: Box::new(Expr::Identifier("condition_1".to_owned()))
            }
            .to_sql()
        );
        assert_eq!(
            "condition_0 XOR condition_1",
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier("condition_0".to_owned())),
                op: BinaryOperator::Xor,
                right: Box::new(Expr::Identifier("condition_1".to_owned()))
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
