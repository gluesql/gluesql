use {
    super::QueryPlan,
    crate::{
        ast::{
            self, BinaryOperator, DataType, DateTimeField, Literal, TrimWhereField, UnaryOperator,
        },
        data::Value,
    },
    serde::{Deserialize, Serialize},
    strum_macros::Display,
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
    Aggregate(Box<AggregatePlan>),
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum FunctionPlan {
    Abs(ExprPlan),
    AddMonth {
        expr: ExprPlan,
        size: ExprPlan,
    },
    Lower(ExprPlan),
    Initcap(ExprPlan),
    Upper(ExprPlan),
    Left {
        expr: ExprPlan,
        size: ExprPlan,
    },
    Right {
        expr: ExprPlan,
        size: ExprPlan,
    },
    Asin(ExprPlan),
    Acos(ExprPlan),
    Atan(ExprPlan),
    Lpad {
        expr: ExprPlan,
        size: ExprPlan,
        fill: Option<ExprPlan>,
    },
    Rpad {
        expr: ExprPlan,
        size: ExprPlan,
        fill: Option<ExprPlan>,
    },
    Replace {
        expr: ExprPlan,
        old: ExprPlan,
        new: ExprPlan,
    },
    Cast {
        expr: ExprPlan,
        data_type: DataType,
    },
    Ceil(ExprPlan),
    Coalesce(Vec<ExprPlan>),
    Concat(Vec<ExprPlan>),
    ConcatWs {
        separator: ExprPlan,
        exprs: Vec<ExprPlan>,
    },
    Custom {
        name: String,
        exprs: Vec<ExprPlan>,
    },
    IfNull {
        expr: ExprPlan,
        then: ExprPlan,
    },
    NullIf {
        expr1: ExprPlan,
        expr2: ExprPlan,
    },
    Rand(Option<ExprPlan>),
    Round(ExprPlan),
    Trunc(ExprPlan),
    Floor(ExprPlan),
    Trim {
        expr: ExprPlan,
        filter_chars: Option<ExprPlan>,
        trim_where_field: Option<TrimWhereField>,
    },
    Exp(ExprPlan),
    Extract {
        field: DateTimeField,
        expr: ExprPlan,
    },
    Ln(ExprPlan),
    Log {
        antilog: ExprPlan,
        base: ExprPlan,
    },
    Log2(ExprPlan),
    Log10(ExprPlan),
    Div {
        dividend: ExprPlan,
        divisor: ExprPlan,
    },
    Mod {
        dividend: ExprPlan,
        divisor: ExprPlan,
    },
    Gcd {
        left: ExprPlan,
        right: ExprPlan,
    },
    Lcm {
        left: ExprPlan,
        right: ExprPlan,
    },
    Sin(ExprPlan),
    Cos(ExprPlan),
    Tan(ExprPlan),
    Sqrt(ExprPlan),
    Power {
        expr: ExprPlan,
        power: ExprPlan,
    },
    Radians(ExprPlan),
    Degrees(ExprPlan),
    Now(),
    CurrentDate(),
    CurrentTime(),
    CurrentTimestamp(),
    Pi(),
    LastDay(ExprPlan),
    Ltrim {
        expr: ExprPlan,
        chars: Option<ExprPlan>,
    },
    Rtrim {
        expr: ExprPlan,
        chars: Option<ExprPlan>,
    },
    Reverse(ExprPlan),
    Repeat {
        expr: ExprPlan,
        num: ExprPlan,
    },
    Sign(ExprPlan),
    Substr {
        expr: ExprPlan,
        start: ExprPlan,
        count: Option<ExprPlan>,
    },
    Unwrap {
        expr: ExprPlan,
        selector: ExprPlan,
    },
    GenerateUuid(),
    Greatest(Vec<ExprPlan>),
    Format {
        expr: ExprPlan,
        format: ExprPlan,
    },
    ToDate {
        expr: ExprPlan,
        format: ExprPlan,
    },
    ToTimestamp {
        expr: ExprPlan,
        format: ExprPlan,
    },
    ToTime {
        expr: ExprPlan,
        format: ExprPlan,
    },
    Position {
        from_expr: ExprPlan,
        sub_expr: ExprPlan,
    },
    FindIdx {
        from_expr: ExprPlan,
        sub_expr: ExprPlan,
        start: Option<ExprPlan>,
    },
    Ascii(ExprPlan),
    Chr(ExprPlan),
    Md5(ExprPlan),
    Hex(ExprPlan),
    Append {
        expr: ExprPlan,
        value: ExprPlan,
    },
    Sort {
        expr: ExprPlan,
        order: Option<ExprPlan>,
    },
    Slice {
        expr: ExprPlan,
        start: ExprPlan,
        length: ExprPlan,
    },
    Prepend {
        expr: ExprPlan,
        value: ExprPlan,
    },
    Skip {
        expr: ExprPlan,
        size: ExprPlan,
    },
    Take {
        expr: ExprPlan,
        size: ExprPlan,
    },
    GetX(ExprPlan),
    GetY(ExprPlan),
    Point {
        x: ExprPlan,
        y: ExprPlan,
    },
    CalcDistance {
        geometry1: ExprPlan,
        geometry2: ExprPlan,
    },
    IsEmpty(ExprPlan),
    Length(ExprPlan),
    Entries(ExprPlan),
    Keys(ExprPlan),
    Values(ExprPlan),
    Splice {
        list_data: ExprPlan,
        begin_index: ExprPlan,
        end_index: ExprPlan,
        values: Option<ExprPlan>,
    },
    Dedup(ExprPlan),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AggregatePlan {
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

impl From<ast::Function> for FunctionPlan {
    fn from(function: ast::Function) -> Self {
        match function {
            ast::Function::Abs(expr) => Self::Abs(expr.into()),
            ast::Function::AddMonth { expr, size } => Self::AddMonth {
                expr: expr.into(),
                size: size.into(),
            },
            ast::Function::Lower(expr) => Self::Lower(expr.into()),
            ast::Function::Initcap(expr) => Self::Initcap(expr.into()),
            ast::Function::Upper(expr) => Self::Upper(expr.into()),
            ast::Function::Left { expr, size } => Self::Left {
                expr: expr.into(),
                size: size.into(),
            },
            ast::Function::Right { expr, size } => Self::Right {
                expr: expr.into(),
                size: size.into(),
            },
            ast::Function::Asin(expr) => Self::Asin(expr.into()),
            ast::Function::Acos(expr) => Self::Acos(expr.into()),
            ast::Function::Atan(expr) => Self::Atan(expr.into()),
            ast::Function::Lpad { expr, size, fill } => Self::Lpad {
                expr: expr.into(),
                size: size.into(),
                fill: fill.map(Into::into),
            },
            ast::Function::Rpad { expr, size, fill } => Self::Rpad {
                expr: expr.into(),
                size: size.into(),
                fill: fill.map(Into::into),
            },
            ast::Function::Replace { expr, old, new } => Self::Replace {
                expr: expr.into(),
                old: old.into(),
                new: new.into(),
            },
            ast::Function::Cast { expr, data_type } => Self::Cast {
                expr: expr.into(),
                data_type,
            },
            ast::Function::Ceil(expr) => Self::Ceil(expr.into()),
            ast::Function::Coalesce(exprs) => {
                Self::Coalesce(exprs.into_iter().map(Into::into).collect())
            }
            ast::Function::Concat(exprs) => {
                Self::Concat(exprs.into_iter().map(Into::into).collect())
            }
            ast::Function::ConcatWs { separator, exprs } => Self::ConcatWs {
                separator: separator.into(),
                exprs: exprs.into_iter().map(Into::into).collect(),
            },
            ast::Function::Custom { name, exprs } => Self::Custom {
                name,
                exprs: exprs.into_iter().map(Into::into).collect(),
            },
            ast::Function::IfNull { expr, then } => Self::IfNull {
                expr: expr.into(),
                then: then.into(),
            },
            ast::Function::NullIf { expr1, expr2 } => Self::NullIf {
                expr1: expr1.into(),
                expr2: expr2.into(),
            },
            ast::Function::Rand(expr) => Self::Rand(expr.map(Into::into)),
            ast::Function::Round(expr) => Self::Round(expr.into()),
            ast::Function::Trunc(expr) => Self::Trunc(expr.into()),
            ast::Function::Floor(expr) => Self::Floor(expr.into()),
            ast::Function::Trim {
                expr,
                filter_chars,
                trim_where_field,
            } => Self::Trim {
                expr: expr.into(),
                filter_chars: filter_chars.map(Into::into),
                trim_where_field,
            },
            ast::Function::Exp(expr) => Self::Exp(expr.into()),
            ast::Function::Extract { field, expr } => Self::Extract {
                field,
                expr: expr.into(),
            },
            ast::Function::Ln(expr) => Self::Ln(expr.into()),
            ast::Function::Log { antilog, base } => Self::Log {
                antilog: antilog.into(),
                base: base.into(),
            },
            ast::Function::Log2(expr) => Self::Log2(expr.into()),
            ast::Function::Log10(expr) => Self::Log10(expr.into()),
            ast::Function::Div { dividend, divisor } => Self::Div {
                dividend: dividend.into(),
                divisor: divisor.into(),
            },
            ast::Function::Mod { dividend, divisor } => Self::Mod {
                dividend: dividend.into(),
                divisor: divisor.into(),
            },
            ast::Function::Gcd { left, right } => Self::Gcd {
                left: left.into(),
                right: right.into(),
            },
            ast::Function::Lcm { left, right } => Self::Lcm {
                left: left.into(),
                right: right.into(),
            },
            ast::Function::Sin(expr) => Self::Sin(expr.into()),
            ast::Function::Cos(expr) => Self::Cos(expr.into()),
            ast::Function::Tan(expr) => Self::Tan(expr.into()),
            ast::Function::Sqrt(expr) => Self::Sqrt(expr.into()),
            ast::Function::Power { expr, power } => Self::Power {
                expr: expr.into(),
                power: power.into(),
            },
            ast::Function::Radians(expr) => Self::Radians(expr.into()),
            ast::Function::Degrees(expr) => Self::Degrees(expr.into()),
            ast::Function::Now() => Self::Now(),
            ast::Function::CurrentDate() => Self::CurrentDate(),
            ast::Function::CurrentTime() => Self::CurrentTime(),
            ast::Function::CurrentTimestamp() => Self::CurrentTimestamp(),
            ast::Function::Pi() => Self::Pi(),
            ast::Function::LastDay(expr) => Self::LastDay(expr.into()),
            ast::Function::Ltrim { expr, chars } => Self::Ltrim {
                expr: expr.into(),
                chars: chars.map(Into::into),
            },
            ast::Function::Rtrim { expr, chars } => Self::Rtrim {
                expr: expr.into(),
                chars: chars.map(Into::into),
            },
            ast::Function::Reverse(expr) => Self::Reverse(expr.into()),
            ast::Function::Repeat { expr, num } => Self::Repeat {
                expr: expr.into(),
                num: num.into(),
            },
            ast::Function::Sign(expr) => Self::Sign(expr.into()),
            ast::Function::Substr { expr, start, count } => Self::Substr {
                expr: expr.into(),
                start: start.into(),
                count: count.map(Into::into),
            },
            ast::Function::Unwrap { expr, selector } => Self::Unwrap {
                expr: expr.into(),
                selector: selector.into(),
            },
            ast::Function::GenerateUuid() => Self::GenerateUuid(),
            ast::Function::Greatest(exprs) => {
                Self::Greatest(exprs.into_iter().map(Into::into).collect())
            }
            ast::Function::Format { expr, format } => Self::Format {
                expr: expr.into(),
                format: format.into(),
            },
            ast::Function::ToDate { expr, format } => Self::ToDate {
                expr: expr.into(),
                format: format.into(),
            },
            ast::Function::ToTimestamp { expr, format } => Self::ToTimestamp {
                expr: expr.into(),
                format: format.into(),
            },
            ast::Function::ToTime { expr, format } => Self::ToTime {
                expr: expr.into(),
                format: format.into(),
            },
            ast::Function::Position {
                from_expr,
                sub_expr,
            } => Self::Position {
                from_expr: from_expr.into(),
                sub_expr: sub_expr.into(),
            },
            ast::Function::FindIdx {
                from_expr,
                sub_expr,
                start,
            } => Self::FindIdx {
                from_expr: from_expr.into(),
                sub_expr: sub_expr.into(),
                start: start.map(Into::into),
            },
            ast::Function::Ascii(expr) => Self::Ascii(expr.into()),
            ast::Function::Chr(expr) => Self::Chr(expr.into()),
            ast::Function::Md5(expr) => Self::Md5(expr.into()),
            ast::Function::Hex(expr) => Self::Hex(expr.into()),
            ast::Function::Append { expr, value } => Self::Append {
                expr: expr.into(),
                value: value.into(),
            },
            ast::Function::Sort { expr, order } => Self::Sort {
                expr: expr.into(),
                order: order.map(Into::into),
            },
            ast::Function::Slice {
                expr,
                start,
                length,
            } => Self::Slice {
                expr: expr.into(),
                start: start.into(),
                length: length.into(),
            },
            ast::Function::Prepend { expr, value } => Self::Prepend {
                expr: expr.into(),
                value: value.into(),
            },
            ast::Function::Skip { expr, size } => Self::Skip {
                expr: expr.into(),
                size: size.into(),
            },
            ast::Function::Take { expr, size } => Self::Take {
                expr: expr.into(),
                size: size.into(),
            },
            ast::Function::GetX(expr) => Self::GetX(expr.into()),
            ast::Function::GetY(expr) => Self::GetY(expr.into()),
            ast::Function::Point { x, y } => Self::Point {
                x: x.into(),
                y: y.into(),
            },
            ast::Function::CalcDistance {
                geometry1,
                geometry2,
            } => Self::CalcDistance {
                geometry1: geometry1.into(),
                geometry2: geometry2.into(),
            },
            ast::Function::IsEmpty(expr) => Self::IsEmpty(expr.into()),
            ast::Function::Length(expr) => Self::Length(expr.into()),
            ast::Function::Entries(expr) => Self::Entries(expr.into()),
            ast::Function::Keys(expr) => Self::Keys(expr.into()),
            ast::Function::Values(expr) => Self::Values(expr.into()),
            ast::Function::Splice {
                list_data,
                begin_index,
                end_index,
                values,
            } => Self::Splice {
                list_data: list_data.into(),
                begin_index: begin_index.into(),
                end_index: end_index.into(),
                values: values.map(Into::into),
            },
            ast::Function::Dedup(expr) => Self::Dedup(expr.into()),
        }
    }
}

impl From<ast::Aggregate> for AggregatePlan {
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
