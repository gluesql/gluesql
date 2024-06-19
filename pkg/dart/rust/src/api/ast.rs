use flutter_rust_bridge::frb;
pub use gluesql_core::ast::{
    Aggregate, AstLiteral, BinaryOperator, DataType, DateTimeField, Expr, Function, Query,
    UnaryOperator,
};

#[frb(mirror(BinaryOperator), non_opaque)]
pub enum _BinaryOperator {
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
    BitwiseAnd,
    BitwiseShiftLeft,
    BitwiseShiftRight,
}

// #[frb(mirror(Expr), non_opaque)]
// pub enum _Expr {
//     Identifier(String),
//     CompoundIdentifier {
//         alias: String,
//         ident: String,
//     },
//     IsNull(Box<Expr>),
//     IsNotNull(Box<Expr>),
//     InList {
//         expr: Box<Expr>,
//         list: Vec<Expr>,
//         negated: bool,
//     },
//     InSubquery {
//         expr: Box<Expr>,
//         subquery: Box<Query>,
//         negated: bool,
//     },
//     Between {
//         expr: Box<Expr>,
//         negated: bool,
//         low: Box<Expr>,
//         high: Box<Expr>,
//     },
//     Like {
//         expr: Box<Expr>,
//         negated: bool,
//         pattern: Box<Expr>,
//     },
//     ILike {
//         expr: Box<Expr>,
//         negated: bool,
//         pattern: Box<Expr>,
//     },
//     BinaryOp {
//         left: Box<Expr>,
//         op: BinaryOperator,
//         right: Box<Expr>,
//     },
//     UnaryOp {
//         op: UnaryOperator,
//         expr: Box<Expr>,
//     },
//     Nested(Box<Expr>),
//     Literal(AstLiteral),
//     TypedString {
//         data_type: DataType,
//         value: String,
//     },
//     Function(Box<Function>),
//     Aggregate(Box<Aggregate>),
//     Exists {
//         subquery: Box<Query>,
//         negated: bool,
//     },
//     Subquery(Box<Query>),
//     Case {
//         operand: Option<Box<Expr>>,
//         when_then: Vec<(Expr, Expr)>,
//         else_result: Option<Box<Expr>>,
//     },
//     ArrayIndex {
//         obj: Box<Expr>,
//         indexes: Vec<Expr>,
//     },
//     Interval {
//         expr: Box<Expr>,
//         leading_field: Option<DateTimeField>,
//         last_field: Option<DateTimeField>,
//     },
//     Array {
//         elem: Vec<Expr>,
//     },
// }

#[frb(mirror(DateTimeField), non_opaque)]
// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
// #[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum _DateTimeField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}
