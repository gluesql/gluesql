use {
    super::{FunctionPlan, QueryPlan},
    crate::{
        ast::{self, BinaryOperator, DataType, DateTimeField, Literal, UnaryOperator},
        data::Value,
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExprPlan {
    Identifier(String),
    CompoundIdentifier {
        alias: String,
        ident: String,
    },
    IsNull(Box<ExprPlan>),
    IsNotNull(Box<ExprPlan>),
    InList {
        expr: Box<ExprPlan>,
        list: Vec<ExprPlan>,
        negated: bool,
    },
    InSubquery {
        expr: Box<ExprPlan>,
        subquery: Box<QueryPlan>,
        negated: bool,
    },
    Between {
        expr: Box<ExprPlan>,
        negated: bool,
        low: Box<ExprPlan>,
        high: Box<ExprPlan>,
    },
    Like {
        expr: Box<ExprPlan>,
        negated: bool,
        pattern: Box<ExprPlan>,
    },
    ILike {
        expr: Box<ExprPlan>,
        negated: bool,
        pattern: Box<ExprPlan>,
    },
    Regex {
        expr: Box<ExprPlan>,
        negated: bool,
        pattern: Box<ExprPlan>,
        case_sensitive: bool,
    },
    BinaryOp {
        left: Box<ExprPlan>,
        op: BinaryOperator,
        right: Box<ExprPlan>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<ExprPlan>,
    },
    Nested(Box<ExprPlan>),
    Literal(Literal),
    Value(Value),
    TypedString {
        data_type: DataType,
        value: String,
    },
    Function(Box<FunctionPlan>),
    Aggregate(Box<AggregateExprPlan>),
    Exists {
        subquery: Box<QueryPlan>,
        negated: bool,
    },
    Subquery(Box<QueryPlan>),
    Case {
        operand: Option<Box<ExprPlan>>,
        when_then: Vec<(ExprPlan, ExprPlan)>,
        else_result: Option<Box<ExprPlan>>,
    },
    ArrayIndex {
        obj: Box<ExprPlan>,
        indexes: Vec<ExprPlan>,
    },
    Interval {
        expr: Box<ExprPlan>,
        leading_field: Option<DateTimeField>,
        last_field: Option<DateTimeField>,
    },
    Array {
        elem: Vec<ExprPlan>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AggregateExprPlan {
    pub func: AggregateFunctionPlan,
    pub distinct: bool,
    pub slot: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregateFunctionPlan {
    Count(CountArgExprPlan),
    Sum(ExprPlan),
    Max(ExprPlan),
    Min(ExprPlan),
    Avg(ExprPlan),
    Variance(ExprPlan),
    Stdev(ExprPlan),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CountArgExprPlan {
    Wildcard,
    Expr(ExprPlan),
}

pub fn plan_scalar_expr(expr: ast::Expr) -> ExprPlan {
    expr.into()
}

impl From<ast::Expr> for ExprPlan {
    fn from(expr: ast::Expr) -> Self {
        match expr {
            ast::Expr::Identifier(ident) => Self::Identifier(ident),
            ast::Expr::CompoundIdentifier { alias, ident } => {
                Self::CompoundIdentifier { alias, ident }
            }
            ast::Expr::IsNull(expr) => Self::IsNull(Box::new((*expr).into())),
            ast::Expr::IsNotNull(expr) => Self::IsNotNull(Box::new((*expr).into())),
            ast::Expr::InList {
                expr,
                list,
                negated,
            } => Self::InList {
                expr: Box::new((*expr).into()),
                list: list.into_iter().map(Into::into).collect(),
                negated,
            },
            ast::Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => Self::InSubquery {
                expr: Box::new((*expr).into()),
                subquery: Box::new((*subquery).into()),
                negated,
            },
            ast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Self::Between {
                expr: Box::new((*expr).into()),
                negated,
                low: Box::new((*low).into()),
                high: Box::new((*high).into()),
            },
            ast::Expr::Like {
                expr,
                negated,
                pattern,
            } => Self::Like {
                expr: Box::new((*expr).into()),
                negated,
                pattern: Box::new((*pattern).into()),
            },
            ast::Expr::ILike {
                expr,
                negated,
                pattern,
            } => Self::ILike {
                expr: Box::new((*expr).into()),
                negated,
                pattern: Box::new((*pattern).into()),
            },
            ast::Expr::Regex {
                expr,
                negated,
                pattern,
                case_sensitive,
            } => Self::Regex {
                expr: Box::new((*expr).into()),
                negated,
                pattern: Box::new((*pattern).into()),
                case_sensitive,
            },
            ast::Expr::BinaryOp { left, op, right } => Self::BinaryOp {
                left: Box::new((*left).into()),
                op,
                right: Box::new((*right).into()),
            },
            ast::Expr::UnaryOp { op, expr } => Self::UnaryOp {
                op,
                expr: Box::new((*expr).into()),
            },
            ast::Expr::Nested(expr) => Self::Nested(Box::new((*expr).into())),
            ast::Expr::Literal(literal) => Self::Literal(literal),
            ast::Expr::Value(value) => Self::Value(value),
            ast::Expr::TypedString { data_type, value } => Self::TypedString { data_type, value },
            ast::Expr::Function(function) => Self::Function(Box::new((*function).into())),
            ast::Expr::Aggregate(aggregate) => Self::Aggregate(Box::new((*aggregate).into())),
            ast::Expr::Exists { subquery, negated } => Self::Exists {
                subquery: Box::new((*subquery).into()),
                negated,
            },
            ast::Expr::Subquery(query) => Self::Subquery(Box::new((*query).into())),
            ast::Expr::Case {
                operand,
                when_then,
                else_result,
            } => Self::Case {
                operand: operand.map(|expr| Box::new((*expr).into())),
                when_then: when_then
                    .into_iter()
                    .map(|(when, then)| (when.into(), then.into()))
                    .collect(),
                else_result: else_result.map(|expr| Box::new((*expr).into())),
            },
            ast::Expr::ArrayIndex { obj, indexes } => Self::ArrayIndex {
                obj: Box::new((*obj).into()),
                indexes: indexes.into_iter().map(Into::into).collect(),
            },
            ast::Expr::Interval {
                expr,
                leading_field,
                last_field,
            } => Self::Interval {
                expr: Box::new((*expr).into()),
                leading_field,
                last_field,
            },
            ast::Expr::Array { elem } => Self::Array {
                elem: elem.into_iter().map(Into::into).collect(),
            },
        }
    }
}

impl From<ast::Aggregate> for AggregateExprPlan {
    fn from(aggregate: ast::Aggregate) -> Self {
        let ast::Aggregate { func, distinct } = aggregate;

        Self {
            func: func.into(),
            distinct,
            slot: None,
        }
    }
}

impl From<ast::AggregateFunction> for AggregateFunctionPlan {
    fn from(func: ast::AggregateFunction) -> Self {
        match func {
            ast::AggregateFunction::Count(expr) => Self::Count(expr.into()),
            ast::AggregateFunction::Sum(expr) => Self::Sum(expr.into()),
            ast::AggregateFunction::Max(expr) => Self::Max(expr.into()),
            ast::AggregateFunction::Min(expr) => Self::Min(expr.into()),
            ast::AggregateFunction::Avg(expr) => Self::Avg(expr.into()),
            ast::AggregateFunction::Variance(expr) => Self::Variance(expr.into()),
            ast::AggregateFunction::Stdev(expr) => Self::Stdev(expr.into()),
        }
    }
}

impl From<ast::CountArgExpr> for CountArgExprPlan {
    fn from(expr: ast::CountArgExpr) -> Self {
        match expr {
            ast::CountArgExpr::Wildcard => Self::Wildcard,
            ast::CountArgExpr::Expr(expr) => Self::Expr(expr.into()),
        }
    }
}
