use serde::{Deserialize, Serialize};
//use core::fmt::Formatter;
use strum_macros::Display;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
pub enum UnaryOperator {
    #[strum(to_string = "+")]
    Plus,
    #[strum(to_string = "-")]
    Minus,
    #[strum(to_string = "~")]
    Not,
    // #[strum(to_string="factorial")]
    Factorial, // why is this here and not in the function enum?
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
pub enum BinaryOperator {
    #[strum(to_string = "+")]
    Plus,
    #[strum(to_string = "-")]
    Minus,
    #[strum(to_string = "*")]
    Multiply,
    #[strum(to_string = "/")]
    Divide,
    #[strum(to_string = "%")]
    Modulo,
    StringConcat,
    #[strum(to_string = ">")]
    Gt,
    #[strum(to_string = "<")]
    Lt,
    #[strum(to_string = ">=")]
    GtEq,
    #[strum(to_string = "<=")]
    LtEq,
    #[strum(to_string = "==")]
    Eq,
    #[strum(to_string = "<>")]
    NotEq,
    #[strum(to_string = "AND")]
    And,
    #[strum(to_string = "OR")]
    Or,
    #[strum(to_string = "XOR")]
    Xor,
    Like,
    ILike,
    NotLike,
    NotILike,
}

/*
impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plus => writeln!(f, "+"),
            Self::Minus => writeln!(f, "-"),
            Self::Multiply => writeln!(f, "*"),
            Self::Divide => writeln!(f, "/"),
            Self::Modulo => writeln!(f, "%"),
            _ => Err(ExecuteError::UnimplementedDisplay)
        }
    }
}
*/

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
