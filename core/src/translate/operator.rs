use {
    super::TranslateError,
    crate::{
        ast::{BinaryOperator, UnaryOperator},
        result::Result,
    },
    sqlparser::ast::{BinaryOperator as SqlBinaryOperator, UnaryOperator as SqlUnaryOperator},
};

pub fn translate_unary_operator(sql_unary_operator: &SqlUnaryOperator) -> Result<UnaryOperator> {
    match sql_unary_operator {
        SqlUnaryOperator::Plus => Ok(UnaryOperator::Plus),
        SqlUnaryOperator::Minus => Ok(UnaryOperator::Minus),
        SqlUnaryOperator::Not => Ok(UnaryOperator::Not),
        SqlUnaryOperator::PGPostfixFactorial => Ok(UnaryOperator::Factorial),
        SqlUnaryOperator::PGBitwiseNot => Ok(UnaryOperator::BitwiseNot),
        _ => Err(TranslateError::UnreachableUnaryOperator(sql_unary_operator.to_string()).into()),
    }
}

pub fn translate_binary_operator(
    sql_binary_operator: &SqlBinaryOperator,
) -> Result<BinaryOperator> {
    match sql_binary_operator {
        SqlBinaryOperator::Plus => Ok(BinaryOperator::Plus),
        SqlBinaryOperator::Minus => Ok(BinaryOperator::Minus),
        SqlBinaryOperator::Multiply => Ok(BinaryOperator::Multiply),
        SqlBinaryOperator::Divide => Ok(BinaryOperator::Divide),
        SqlBinaryOperator::Modulo => Ok(BinaryOperator::Modulo),
        SqlBinaryOperator::StringConcat => Ok(BinaryOperator::StringConcat),
        SqlBinaryOperator::Gt => Ok(BinaryOperator::Gt),
        SqlBinaryOperator::Lt => Ok(BinaryOperator::Lt),
        SqlBinaryOperator::GtEq => Ok(BinaryOperator::GtEq),
        SqlBinaryOperator::LtEq => Ok(BinaryOperator::LtEq),
        SqlBinaryOperator::Eq => Ok(BinaryOperator::Eq),
        SqlBinaryOperator::NotEq => Ok(BinaryOperator::NotEq),
        SqlBinaryOperator::And => Ok(BinaryOperator::And),
        SqlBinaryOperator::Or => Ok(BinaryOperator::Or),
        SqlBinaryOperator::Xor => Ok(BinaryOperator::Xor),
        SqlBinaryOperator::BitwiseAnd => Ok(BinaryOperator::BitwiseAnd),
        SqlBinaryOperator::PGBitwiseShiftLeft => Ok(BinaryOperator::BitwiseShiftLeft),
        SqlBinaryOperator::PGBitwiseShiftRight => Ok(BinaryOperator::BitwiseShiftRight),
        _ => Err(TranslateError::UnsupportedBinaryOperator(sql_binary_operator.to_string()).into()),
    }
}
