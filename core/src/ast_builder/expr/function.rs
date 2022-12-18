use {
    super::ExprNode,
    crate::{
        ast::{DateTimeField, Function},
        ast_builder::{DataTypeNode, ExprList},
        result::{Error, Result},
    },
};

#[derive(Clone, Debug)]
pub enum FunctionNode<'a> {
    Abs(ExprNode<'a>),
    Upper(ExprNode<'a>),
    IfNull {
        expr: ExprNode<'a>,
        then: ExprNode<'a>,
    },
    Ceil(ExprNode<'a>),
    Round(ExprNode<'a>),
    Floor(ExprNode<'a>),
    Asin(ExprNode<'a>),
    Acos(ExprNode<'a>),
    Atan(ExprNode<'a>),
    Sin(ExprNode<'a>),
    Cos(ExprNode<'a>),
    Tan(ExprNode<'a>),
    Pi,
    Now,
    Left {
        expr: ExprNode<'a>,
        size: ExprNode<'a>,
    },
    Log {
        antilog: ExprNode<'a>,
        base: ExprNode<'a>,
    },
    Log2(ExprNode<'a>),
    Log10(ExprNode<'a>),
    Ln(ExprNode<'a>),
    Right {
        expr: ExprNode<'a>,
        size: ExprNode<'a>,
    },
    Reverse(ExprNode<'a>),
    Sign(ExprNode<'a>),
    Power {
        expr: ExprNode<'a>,
        power: ExprNode<'a>,
    },
    Sqrt(ExprNode<'a>),
    Gcd {
        left: ExprNode<'a>,
        right: ExprNode<'a>,
    },
    Lcm {
        left: ExprNode<'a>,
        right: ExprNode<'a>,
    },
    GenerateUuid,
    Repeat {
        expr: ExprNode<'a>,
        num: ExprNode<'a>,
    },
    Exp(ExprNode<'a>),
    Lpad {
        expr: ExprNode<'a>,
        size: ExprNode<'a>,
        fill: Option<ExprNode<'a>>,
    },
    Rpad {
        expr: ExprNode<'a>,
        size: ExprNode<'a>,
        fill: Option<ExprNode<'a>>,
    },
    Degrees(ExprNode<'a>),
    Radians(ExprNode<'a>),
    Concat(ExprList<'a>),
    ConcatWs {
        separator: ExprNode<'a>,
        exprs: ExprList<'a>,
    },
    Substr {
        expr: ExprNode<'a>,
        start: ExprNode<'a>,
        count: Option<ExprNode<'a>>,
    },
    Ltrim {
        expr: ExprNode<'a>,
        chars: Option<ExprNode<'a>>,
    },
    Rtrim {
        expr: ExprNode<'a>,
        chars: Option<ExprNode<'a>>,
    },
    Div {
        dividend: ExprNode<'a>,
        divisor: ExprNode<'a>,
    },
    Mod {
        dividend: ExprNode<'a>,
        divisor: ExprNode<'a>,
    },
    Format {
        expr: ExprNode<'a>,
        format: ExprNode<'a>,
    },
    ToDate {
        expr: ExprNode<'a>,
        format: ExprNode<'a>,
    },
    ToTimestamp {
        expr: ExprNode<'a>,
        format: ExprNode<'a>,
    },
    ToTime {
        expr: ExprNode<'a>,
        format: ExprNode<'a>,
    },
    Lower(ExprNode<'a>),
    Position {
        from_expr: ExprNode<'a>,
        sub_expr: ExprNode<'a>,
    },
    Cast {
        expr: ExprNode<'a>,
        data_type: DataTypeNode,
    },
    Extract {
        field: DateTimeField,
        expr: ExprNode<'a>,
    },
}

impl<'a> TryFrom<FunctionNode<'a>> for Function {
    type Error = Error;

    fn try_from(func_node: FunctionNode<'a>) -> Result<Self> {
        match func_node {
            FunctionNode::Abs(expr_node) => expr_node.try_into().map(Function::Abs),
            FunctionNode::Upper(expr_node) => expr_node.try_into().map(Function::Upper),
            FunctionNode::Lower(expr_node) => expr_node.try_into().map(Function::Lower),
            FunctionNode::IfNull { expr, then } => {
                let expr = expr.try_into()?;
                let then = then.try_into()?;
                Ok(Function::IfNull { expr, then })
            }
            FunctionNode::Ceil(expr_node) => expr_node.try_into().map(Function::Ceil),
            FunctionNode::Round(expr_node) => expr_node.try_into().map(Function::Round),
            FunctionNode::Floor(expr_node) => expr_node.try_into().map(Function::Floor),
            FunctionNode::Asin(expr_node) => expr_node.try_into().map(Function::Asin),
            FunctionNode::Acos(expr_node) => expr_node.try_into().map(Function::Acos),
            FunctionNode::Atan(expr_node) => expr_node.try_into().map(Function::Atan),
            FunctionNode::Sin(expr_node) => expr_node.try_into().map(Function::Sin),
            FunctionNode::Cos(expr_node) => expr_node.try_into().map(Function::Cos),
            FunctionNode::Tan(expr_node) => expr_node.try_into().map(Function::Tan),
            FunctionNode::Pi => Ok(Function::Pi()),
            FunctionNode::Now => Ok(Function::Now()),
            FunctionNode::Left { expr, size } => {
                let expr = expr.try_into()?;
                let size = size.try_into()?;
                Ok(Function::Left { expr, size })
            }
            FunctionNode::Log { antilog, base } => {
                let antilog = antilog.try_into()?;
                let base = base.try_into()?;
                Ok(Function::Log { antilog, base })
            }
            FunctionNode::Log2(expr_node) => expr_node.try_into().map(Function::Log2),
            FunctionNode::Log10(expr_node) => expr_node.try_into().map(Function::Log10),
            FunctionNode::Ln(expr_node) => expr_node.try_into().map(Function::Ln),
            FunctionNode::Right { expr, size } => {
                let expr = expr.try_into()?;
                let size = size.try_into()?;
                Ok(Function::Right { expr, size })
            }
            FunctionNode::Reverse(expr_node) => expr_node.try_into().map(Function::Reverse),
            FunctionNode::Sign(expr_node) => expr_node.try_into().map(Function::Sign),
            FunctionNode::Power { expr, power } => {
                let expr = expr.try_into()?;
                let power = power.try_into()?;
                Ok(Function::Power { expr, power })
            }
            FunctionNode::Sqrt(expr_node) => expr_node.try_into().map(Function::Sqrt),
            FunctionNode::Gcd { left, right } => {
                let left = left.try_into()?;
                let right = right.try_into()?;
                Ok(Function::Gcd { left, right })
            }
            FunctionNode::Lcm { left, right } => {
                let left = left.try_into()?;
                let right = right.try_into()?;
                Ok(Function::Lcm { left, right })
            }
            FunctionNode::GenerateUuid => Ok(Function::GenerateUuid()),
            FunctionNode::Repeat { expr, num } => {
                let expr = expr.try_into()?;
                let num = num.try_into()?;
                Ok(Function::Repeat { expr, num })
            }
            FunctionNode::Lpad { expr, size, fill } => {
                let fill = fill.map(TryInto::try_into).transpose()?;
                let expr = expr.try_into()?;
                let size = size.try_into()?;
                Ok(Function::Lpad { expr, size, fill })
            }
            FunctionNode::Rpad { expr, size, fill } => {
                let fill = fill.map(TryInto::try_into).transpose()?;
                let expr = expr.try_into()?;
                let size = size.try_into()?;
                Ok(Function::Rpad { expr, size, fill })
            }
            FunctionNode::Concat(expr_list) => expr_list.try_into().map(Function::Concat),
            FunctionNode::ConcatWs { separator, exprs } => {
                let separator = separator.try_into()?;
                let exprs = exprs.try_into()?;
                Ok(Function::ConcatWs { separator, exprs })
            }
            FunctionNode::Degrees(expr) => expr.try_into().map(Function::Degrees),
            FunctionNode::Radians(expr) => expr.try_into().map(Function::Radians),
            FunctionNode::Exp(expr) => expr.try_into().map(Function::Exp),
            FunctionNode::Substr { expr, start, count } => {
                let count = count.map(TryInto::try_into).transpose()?;
                let expr = expr.try_into()?;
                let start = start.try_into()?;
                Ok(Function::Substr { expr, start, count })
            }
            FunctionNode::Ltrim { expr, chars } => {
                let chars = chars.map(TryInto::try_into).transpose()?;
                let expr = expr.try_into()?;
                Ok(Function::Ltrim { expr, chars })
            }
            FunctionNode::Rtrim { expr, chars } => {
                let chars = chars.map(TryInto::try_into).transpose()?;
                let expr = expr.try_into()?;
                Ok(Function::Rtrim { expr, chars })
            }
            FunctionNode::Div { dividend, divisor } => {
                let dividend = dividend.try_into()?;
                let divisor = divisor.try_into()?;
                Ok(Function::Div { dividend, divisor })
            }
            FunctionNode::Mod { dividend, divisor } => {
                let dividend = dividend.try_into()?;
                let divisor = divisor.try_into()?;
                Ok(Function::Mod { dividend, divisor })
            }
            FunctionNode::Format { expr, format } => {
                let expr = expr.try_into()?;
                let format = format.try_into()?;
                Ok(Function::Format { expr, format })
            }
            FunctionNode::ToDate { expr, format } => {
                let expr = expr.try_into()?;
                let format = format.try_into()?;
                Ok(Function::ToDate { expr, format })
            }
            FunctionNode::ToTimestamp { expr, format } => {
                let expr = expr.try_into()?;
                let format = format.try_into()?;
                Ok(Function::ToTimestamp { expr, format })
            }
            FunctionNode::ToTime { expr, format } => {
                let expr = expr.try_into()?;
                let format = format.try_into()?;
                Ok(Function::ToTime { expr, format })
            }
            FunctionNode::Position {
                from_expr,
                sub_expr,
            } => {
                let from_expr = from_expr.try_into()?;
                let sub_expr = sub_expr.try_into()?;
                Ok(Function::Position {
                    from_expr,
                    sub_expr,
                })
            }
            FunctionNode::Cast { expr, data_type } => {
                let expr = expr.try_into()?;
                let data_type = data_type.try_into()?;
                Ok(Function::Cast { expr, data_type })
            }
            FunctionNode::Extract { field, expr } => {
                let expr = expr.try_into()?;
                Ok(Function::Extract { field, expr })
            }
        }
    }
}

impl<'a> ExprNode<'a> {
    pub fn abs(self) -> ExprNode<'a> {
        abs(self)
    }
    pub fn upper(self) -> ExprNode<'a> {
        upper(self)
    }
    pub fn lower(self) -> ExprNode<'a> {
        lower(self)
    }
    pub fn ifnull<T: Into<ExprNode<'a>>>(self, another: T) -> ExprNode<'a> {
        ifnull(self, another)
    }
    pub fn ceil(self) -> ExprNode<'a> {
        ceil(self)
    }
    pub fn round(self) -> ExprNode<'a> {
        round(self)
    }
    pub fn floor(self) -> ExprNode<'a> {
        floor(self)
    }
    pub fn asin(self) -> ExprNode<'a> {
        asin(self)
    }
    pub fn acos(self) -> ExprNode<'a> {
        acos(self)
    }
    pub fn atan(self) -> ExprNode<'a> {
        atan(self)
    }
    pub fn sin(self) -> ExprNode<'a> {
        sin(self)
    }
    pub fn cos(self) -> ExprNode<'a> {
        cos(self)
    }
    pub fn tan(self) -> ExprNode<'a> {
        tan(self)
    }
    pub fn left<T: Into<ExprNode<'a>>>(self, size: T) -> Self {
        left(self, size)
    }
    pub fn log<T: Into<ExprNode<'a>>>(self, base: T) -> ExprNode<'a> {
        log(self, base)
    }
    pub fn log2(self) -> ExprNode<'a> {
        log2(self)
    }
    pub fn log10(self) -> ExprNode<'a> {
        log10(self)
    }
    pub fn ln(self) -> ExprNode<'a> {
        ln(self)
    }
    pub fn right<T: Into<ExprNode<'a>>>(self, size: T) -> Self {
        right(self, size)
    }

    pub fn reverse(self) -> ExprNode<'a> {
        reverse(self)
    }

    pub fn sign(self) -> ExprNode<'a> {
        sign(self)
    }

    pub fn power<T: Into<ExprNode<'a>>>(self, pwr: T) -> ExprNode<'a> {
        power(self, pwr)
    }

    pub fn sqrt(self) -> ExprNode<'a> {
        sqrt(self)
    }
    pub fn gcd<T: Into<ExprNode<'a>>>(self, right: T) -> ExprNode<'a> {
        gcd(self, right)
    }
    pub fn lcm<T: Into<ExprNode<'a>>>(self, right: T) -> ExprNode<'a> {
        lcm(self, right)
    }
    pub fn repeat<T: Into<ExprNode<'a>>>(self, num: T) -> ExprNode<'a> {
        repeat(self, num)
    }
    pub fn degrees(self) -> ExprNode<'a> {
        degrees(self)
    }
    pub fn radians(self) -> ExprNode<'a> {
        radians(self)
    }
    pub fn lpad<T: Into<ExprNode<'a>>>(self, size: T, fill: Option<ExprNode<'a>>) -> ExprNode<'a> {
        lpad(self, size, fill)
    }
    pub fn rpad<T: Into<ExprNode<'a>>>(self, size: T, fill: Option<ExprNode<'a>>) -> ExprNode<'a> {
        rpad(self, size, fill)
    }
    pub fn exp(self) -> ExprNode<'a> {
        exp(self)
    }
    pub fn substr<T: Into<ExprNode<'a>>>(
        self,
        start: T,
        count: Option<ExprNode<'a>>,
    ) -> ExprNode<'a> {
        substr(self, start, count)
    }
    pub fn rtrim(self, chars: Option<ExprNode<'a>>) -> ExprNode<'a> {
        rtrim(self, chars)
    }
    pub fn ltrim(self, chars: Option<ExprNode<'a>>) -> ExprNode<'a> {
        ltrim(self, chars)
    }
    pub fn format<T: Into<ExprNode<'a>>>(self, fmt: T) -> ExprNode<'a> {
        format(self, fmt)
    }
    pub fn to_date<T: Into<ExprNode<'a>>>(self, format: T) -> ExprNode<'a> {
        to_date(self, format)
    }
    pub fn to_timestamp<T: Into<ExprNode<'a>>>(self, format: T) -> ExprNode<'a> {
        to_timestamp(self, format)
    }
    pub fn to_time<T: Into<ExprNode<'a>>>(self, format: T) -> ExprNode<'a> {
        to_time(self, format)
    }
    pub fn position<T: Into<ExprNode<'a>>>(self, format: T) -> ExprNode<'a> {
        position(self, format)
    }
    pub fn cast<T: Into<DataTypeNode>>(self, data_type: T) -> ExprNode<'a> {
        cast(self, data_type)
    }
    pub fn extract(self, field: DateTimeField) -> ExprNode<'a> {
        extract(field, self)
    }
}

pub fn abs<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Abs(expr.into())))
}
pub fn upper<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Upper(expr.into())))
}
pub fn lower<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Lower(expr.into())))
}
pub fn ifnull<'a, T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(expr: T, then: U) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::IfNull {
        expr: expr.into(),
        then: then.into(),
    }))
}
pub fn ceil<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Ceil(expr.into())))
}
pub fn round<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Round(expr.into())))
}
pub fn concat<'a, T: Into<ExprList<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Concat(expr.into())))
}

pub fn concat_ws<'a, T: Into<ExprNode<'a>>, U: Into<ExprList<'a>>>(
    separator: T,
    exprs: U,
) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::ConcatWs {
        separator: separator.into(),
        exprs: exprs.into(),
    }))
}

pub fn floor<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Floor(expr.into())))
}
pub fn asin<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Asin(expr.into())))
}
pub fn acos<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Acos(expr.into())))
}
pub fn atan<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Atan(expr.into())))
}
pub fn sin<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Sin(expr.into())))
}
pub fn cos<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Cos(expr.into())))
}
pub fn tan<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Tan(expr.into())))
}
pub fn pi<'a>() -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Pi))
}
pub fn generate_uuid<'a>() -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::GenerateUuid))
}
pub fn now<'a>() -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Now))
}
pub fn left<'a, T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(expr: T, size: U) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Left {
        expr: expr.into(),
        size: size.into(),
    }))
}
pub fn log<'a, T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(antilog: T, base: U) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Log {
        antilog: antilog.into(),
        base: base.into(),
    }))
}
pub fn log2<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Log2(expr.into())))
}
pub fn log10<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Log10(expr.into())))
}
pub fn ln<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Ln(expr.into())))
}
pub fn right<'a, T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(expr: T, size: U) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Right {
        expr: expr.into(),
        size: size.into(),
    }))
}

pub fn reverse<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Reverse(expr.into())))
}

pub fn sign<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Sign(expr.into())))
}

pub fn power<'a, T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(expr: T, power: U) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Power {
        expr: expr.into(),
        power: power.into(),
    }))
}

pub fn sqrt<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Sqrt(expr.into())))
}

pub fn gcd<'a, T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(left: T, right: U) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Gcd {
        left: left.into(),
        right: right.into(),
    }))
}

pub fn lcm<'a, T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(left: T, right: U) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Lcm {
        left: left.into(),
        right: right.into(),
    }))
}

pub fn repeat<'a, T: Into<ExprNode<'a>>, V: Into<ExprNode<'a>>>(expr: T, num: V) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Repeat {
        expr: expr.into(),
        num: num.into(),
    }))
}

pub fn lpad<'a, T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(
    expr: T,
    size: U,
    fill: Option<ExprNode<'a>>,
) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Lpad {
        expr: expr.into(),
        size: size.into(),
        fill,
    }))
}

pub fn rpad<'a, T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(
    expr: T,
    size: U,
    fill: Option<ExprNode<'a>>,
) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Rpad {
        expr: expr.into(),
        size: size.into(),
        fill,
    }))
}

pub fn degrees<'a, V: Into<ExprNode<'a>>>(expr: V) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Degrees(expr.into())))
}

pub fn radians<'a, V: Into<ExprNode<'a>>>(expr: V) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Radians(expr.into())))
}

pub fn exp<'a, V: Into<ExprNode<'a>>>(expr: V) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Exp(expr.into())))
}
pub fn substr<'a, T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(
    expr: T,
    start: U,
    count: Option<ExprNode<'a>>,
) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Substr {
        expr: expr.into(),
        start: start.into(),
        count,
    }))
}

pub fn ltrim<'a, T: Into<ExprNode<'a>>>(expr: T, chars: Option<ExprNode<'a>>) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Ltrim {
        expr: expr.into(),
        chars,
    }))
}

pub fn rtrim<'a, T: Into<ExprNode<'a>>>(expr: T, chars: Option<ExprNode<'a>>) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Rtrim {
        expr: expr.into(),
        chars,
    }))
}

pub fn divide<'a, T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(
    dividend: T,
    divisor: U,
) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Div {
        dividend: dividend.into(),
        divisor: divisor.into(),
    }))
}

pub fn modulo<'a, T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(
    dividend: T,
    divisor: U,
) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Mod {
        dividend: dividend.into(),
        divisor: divisor.into(),
    }))
}

pub fn format<'a, D: Into<ExprNode<'a>>, T: Into<ExprNode<'a>>>(
    expr: D,
    format: T,
) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Format {
        expr: expr.into(),
        format: format.into(),
    }))
}

pub fn to_date<'a, T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(
    expr: T,
    format: U,
) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::ToDate {
        expr: expr.into(),
        format: format.into(),
    }))
}

pub fn to_timestamp<'a, T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(
    expr: T,
    format: U,
) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::ToTimestamp {
        expr: expr.into(),
        format: format.into(),
    }))
}

pub fn to_time<'a, T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(
    expr: T,
    format: U,
) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::ToTime {
        expr: expr.into(),
        format: format.into(),
    }))
}

pub fn position<'a, T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(
    from_expr: T,
    sub_expr: U,
) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Position {
        from_expr: from_expr.into(),
        sub_expr: sub_expr.into(),
    }))
}

pub fn cast<'a, T: Into<ExprNode<'a>>, U: Into<DataTypeNode>>(
    expr: T,
    data_type: U,
) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Cast {
        expr: expr.into(),
        data_type: data_type.into(),
    }))
}

pub fn extract<'a, T: Into<ExprNode<'a>>>(field: DateTimeField, expr: T) -> ExprNode<'a> {
    ExprNode::Function(Box::new(FunctionNode::Extract {
        field,
        expr: expr.into(),
    }))
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::DateTimeField,
        ast_builder::{
            abs, acos, asin, atan, cast, ceil, col, concat, concat_ws, cos, date, degrees, divide,
            exp, expr, extract, floor, format, gcd, generate_uuid, ifnull, lcm, left, ln, log,
            log10, log2, lower, lpad, ltrim, modulo, now, num, pi, position, power, radians,
            repeat, reverse, right, round, rpad, rtrim, sign, sin, sqrt, substr, tan, test_expr,
            text, time, timestamp, to_date, to_time, to_timestamp, upper,
        },
        prelude::DataType,
    };

    #[test]
    fn function_abs() {
        let actual = abs(col("num"));
        let expected = "ABS(num)";
        test_expr(actual, expected);

        let actual = expr("base - 10").abs();
        let expected = "ABS(base - 10)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_upper() {
        let actual = upper(text("ABC"));
        let expected = "UPPER('ABC')";
        test_expr(actual, expected);

        let actual = expr("HoHo").upper();
        let expected = "UPPER(HoHo)";
        test_expr(actual, expected);
    }
    #[test]
    fn function_ifnull() {
        let actual = ifnull(text("HELLO"), text("WORLD"));
        let expected = "IFNULL('HELLO', 'WORLD')";
        test_expr(actual, expected);

        let actual = col("updated_at").ifnull(col("created_at"));
        let expected = "IFNULL(updated_at, created_at)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_ceil() {
        let actual = ceil(col("num"));
        let expected = "CEIL(num)";
        test_expr(actual, expected);

        let actual = expr("base - 10").ceil();
        let expected = "CEIL(base - 10)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_round() {
        let actual = round(col("num"));
        let expected = "ROUND(num)";
        test_expr(actual, expected);

        let actual = expr("base - 10").round();
        let expected = "ROUND(base - 10)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_floor() {
        let actual = floor(col("num"));
        let expected = "FLOOR(num)";
        test_expr(actual, expected);

        let actual = expr("base - 10").floor();
        let expected = "FLOOR(base - 10)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_trigonometrics() {
        // asin
        let actual = asin(col("num"));
        let expected = "ASIN(num)";
        test_expr(actual, expected);

        let actual = col("num").asin();
        let expected = "ASIN(num)";
        test_expr(actual, expected);

        // acos
        let actual = acos(col("num"));
        let expected = "ACOS(num)";
        test_expr(actual, expected);

        let actual = col("num").acos();
        let expected = "ACOS(num)";
        test_expr(actual, expected);

        // atan
        let actual = atan(col("num"));
        let expected = "ATAN(num)";
        test_expr(actual, expected);

        let actual = col("num").atan();
        let expected = "ATAN(num)";
        test_expr(actual, expected);

        // sin
        let actual = sin(col("num"));
        let expected = "SIN(num)";
        test_expr(actual, expected);

        let actual = col("num").sin();
        let expected = "SIN(num)";
        test_expr(actual, expected);

        // cos
        let actual = cos(col("num"));
        let expected = "COS(num)";
        test_expr(actual, expected);

        let actual = col("num").cos();
        let expected = "COS(num)";
        test_expr(actual, expected);

        // tan
        let actual = tan(col("num"));
        let expected = "TAN(num)";
        test_expr(actual, expected);

        let actual = col("num").tan();
        let expected = "TAN(num)";
        test_expr(actual, expected);

        // pi
        let actual = pi();
        let expected = "PI()";
        test_expr(actual, expected);
    }

    #[test]
    fn function_now() {
        let actual = now();
        let expected = "NOW()";
        test_expr(actual, expected);
    }

    #[test]
    fn function_generate_uuid() {
        let actual = generate_uuid();
        let expected = "GENERATE_UUID()";
        test_expr(actual, expected);
    }

    #[test]
    fn function_left() {
        let actual = left(text("GlueSQL"), num(2));
        let expected = "LEFT('GlueSQL', 2)";
        test_expr(actual, expected);

        let actual = expr("GlueSQL").left(num(2));
        let expected = "LEFT(GlueSQL, 2)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_log() {
        let actual = log(num(64), num(8));
        let expected = "log(64,8)";
        test_expr(actual, expected);

        let actual = num(64).log(num(8));
        let expected = "LOG(64,8)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_log2() {
        let actual = log2(col("num"));
        let expected = "LOG2(num)";
        test_expr(actual, expected);

        let actual = col("num").log2();
        let expected = "LOG2(num)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_log10() {
        let actual = log10(col("num"));
        let expected = "LOG10(num)";
        test_expr(actual, expected);

        let actual = col("num").log10();
        let expected = "LOG10(num)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_ln() {
        let actual = ln(num(2));
        let expected = "LN(2)";
        test_expr(actual, expected);

        let actual = num(2).ln();
        let expected = "LN(2)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_right() {
        let actual = right(text("GlueSQL"), num(2));
        let expected = "RIGHT('GlueSQL', 2)";
        test_expr(actual, expected);

        let actual = expr("GlueSQL").right(num(2));
        let expected = "RIGHT(GlueSQL, 2)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_reverse() {
        let actual = reverse(text("GlueSQL"));
        let expected = "REVERSE('GlueSQL')";
        test_expr(actual, expected);

        let actual = expr("GlueSQL").reverse();
        let expected = "REVERSE(GlueSQL)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_sign() {
        let actual = sign(col("id"));
        let expected = "SIGN(id)";
        test_expr(actual, expected);

        let actual = expr("id").sign();
        let expected = "SIGN(id)";
        test_expr(actual, expected);
    }
    #[test]
    fn function_power() {
        let actual = power(num(2), num(4));
        let expected = "POWER(2,4)";
        test_expr(actual, expected);

        let actual = num(2).power(num(4));
        let expected = "POWER(2,4)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_sqrt() {
        let actual = sqrt(num(9));
        let expected = "SQRT(9)";
        test_expr(actual, expected);

        let actual = num(9).sqrt();
        let expected = "SQRT(9)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_gcd() {
        let actual = gcd(num(64), num(8));
        let expected = "gcd(64,8)";
        test_expr(actual, expected);

        let actual = num(64).gcd(num(8));
        let expected = "GCD(64,8)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_lcm() {
        let actual = lcm(num(64), num(8));
        let expected = "lcm(64,8)";
        test_expr(actual, expected);

        let actual = num(64).lcm(num(8));
        let expected = "LCM(64,8)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_repeat() {
        let actual = repeat(text("GlueSQL"), num(2));
        let expected = "REPEAT('GlueSQL', 2)";
        test_expr(actual, expected);

        let actual = text("GlueSQL").repeat(num(2));
        let expected = "REPEAT('GlueSQL', 2)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_degrees() {
        let actual = degrees(num(1));
        let expected = "DEGREES(1)";
        test_expr(actual, expected);

        let actual = num(1).degrees();
        let expected = "DEGREES(1)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_radians() {
        let actual = radians(num(1));
        let expected = "RADIANS(1)";
        test_expr(actual, expected);

        let actual = num(1).radians();
        let expected = "RADIANS(1)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_concat() {
        let actual = concat(vec![text("Glue"), text("SQL"), text("Go")]);
        let expected = "CONCAT('Glue','SQL','Go')";
        test_expr(actual, expected);

        let actual = concat(vec!["Glue", "SQL", "Go"]);
        let expected = "CONCAT(Glue, SQL, Go)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_concat_ws() {
        let actual = concat_ws(text(","), vec![text("Glue"), text("SQL"), text("Go")]);
        let expected = "CONCAT_WS(',', 'Glue', 'SQL', 'Go')";
        test_expr(actual, expected);

        let actual = concat_ws(text(","), vec!["Glue", "SQL", "Go"]);
        let expected = "CONCAT_WS(',', Glue, SQL, Go)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_lpad() {
        let actual = lpad(text("GlueSQL"), num(10), Some(text("Go")));
        let expected = "LPAD('GlueSQL', 10, 'Go')";
        test_expr(actual, expected);

        let actual = lpad(text("GlueSQL"), num(10), None);
        let expected = "LPAD('GlueSQL', 10)";
        test_expr(actual, expected);

        let actual = text("GlueSQL").lpad(num(10), Some(text("Go")));
        let expected = "LPAD('GlueSQL', 10, 'Go')";
        test_expr(actual, expected);

        let actual = text("GlueSQL").lpad(num(10), None);
        let expected = "LPAD('GlueSQL', 10)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_rpad() {
        let actual = rpad(text("GlueSQL"), num(10), Some(text("Go")));
        let expected = "RPAD('GlueSQL', 10, 'Go')";
        test_expr(actual, expected);

        let actual = rpad(text("GlueSQL"), num(10), None);
        let expected = "RPAD('GlueSQL', 10)";
        test_expr(actual, expected);

        let actual = text("GlueSQL").rpad(num(10), Some(text("Go")));
        let expected = "RPAD('GlueSQL', 10, 'Go')";
        test_expr(actual, expected);

        let actual = text("GlueSQL").rpad(num(10), None);
        let expected = "RPAD('GlueSQL', 10)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_exp() {
        let actual = exp(num(2));
        let expected = "EXP(2)";
        test_expr(actual, expected);

        let actual = num(2).exp();
        let expected = "EXP(2)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_substr() {
        let actual = substr(text("GlueSQL"), num(2), Some(num(4)));
        let expected = "SUBSTR('GlueSQL', 2, 4)";
        test_expr(actual, expected);

        let actual = substr(text("GlueSQL"), num(2), None);
        let expected = "SUBSTR('GlueSQL', 2)";
        test_expr(actual, expected);

        let actual = text("GlueSQL").substr(num(2), Some(num(4)));
        let expected = "SUBSTR('GlueSQL', 2, 4)";
        test_expr(actual, expected);

        let actual = text("GlueSQL").substr(num(2), None);
        let expected = "SUBSTR('GlueSQL', 2)";
        test_expr(actual, expected);

        let actual = text("GlueSQL").substr(num(2), None);
        let expected = "SUBSTR('GlueSQL', 2)";
        test_expr(actual, expected);

        let actual = substr(text("GlueSQL      ").rtrim(None), num(2), None);
        let expected = "SUBSTR(RTRIM('GlueSQL      '), 2)";
        test_expr(actual, expected);

        let actual = text("GlueSQL      ").rtrim(None).substr(num(2), None);
        let expected = "SUBSTR(RTRIM('GlueSQL      '), 2)";
        test_expr(actual, expected);

        let actual = substr(text("      GlueSQL").ltrim(None), num(2), None);
        let expected = "SUBSTR(LTRIM('      GlueSQL'), 2)";
        test_expr(actual, expected);

        let actual = text("      GlueSQL").ltrim(None).substr(num(2), None);
        let expected = "SUBSTR(LTRIM('      GlueSQL'), 2)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_rtrim() {
        let actual = rtrim(text("GlueSQL      "), None);
        let expected = "RTRIM('GlueSQL      ')";
        test_expr(actual, expected);

        let actual = text("GlueSQL      ").rtrim(None);
        let expected = "RTRIM('GlueSQL      ')";
        test_expr(actual, expected);

        let actual = rtrim(text("GlueSQLABC"), Some(text("ABC")));
        let expected = "RTRIM('GlueSQLABC','ABC')";
        test_expr(actual, expected);

        let actual = text("GlueSQLABC").rtrim(Some(text("ABC")));
        let expected = "RTRIM('GlueSQLABC','ABC')";
        test_expr(actual, expected);

        let actual = text("chicken").ltrim(None).rtrim(Some(text("en")));
        let expected = "RTRIM(LTRIM('chicken'),'en')";
        test_expr(actual, expected);

        let actual = rtrim(text("chicken").ltrim(Some(text("chick"))), None);
        let expected = "RTRIM(LTRIM('chicken','chick'))";
        test_expr(actual, expected);
    }

    #[test]
    fn function_ltrim() {
        let actual = ltrim(text("      GlueSQL"), None);
        let expected = "LTRIM('      GlueSQL')";
        test_expr(actual, expected);

        let actual = text("      GlueSQL").ltrim(None);
        let expected = "LTRIM('      GlueSQL')";
        test_expr(actual, expected);

        let actual = ltrim(text("ABCGlueSQL"), Some(text("ABC")));
        let expected = "LTRIM('ABCGlueSQL','ABC')";
        test_expr(actual, expected);

        let actual = text("ABCGlueSQL").ltrim(Some(text("ABC")));
        let expected = "LTRIM('ABCGlueSQL','ABC')";
        test_expr(actual, expected);

        let actual = text("chicken").rtrim(Some(text("en"))).ltrim(None);
        let expected = "LTRIM(RTRIM('chicken','en'))";
        test_expr(actual, expected);

        let actual = text("chicken").rtrim(None).ltrim(Some(text("chick")));
        let expected = "LTRIM(RTRIM('chicken'),'chick')";
        test_expr(actual, expected);
    }

    #[test]
    fn function_mod() {
        let actual = modulo(num(64), num(8));
        let expected = "mod(64,8)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_div() {
        let actual = divide(num(64), num(8));
        let expected = "div(64,8)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_format() {
        let actual = format(date("2017-06-15"), text("%Y-%m"));
        let expected = "FORMAT(DATE'2017-06-15','%Y-%m')";
        test_expr(actual, expected);

        let actual = date("2017-06-15").format(text("%Y-%m"));
        let expected = "FORMAT(DATE '2017-06-15','%Y-%m')";
        test_expr(actual, expected);

        let actual = format(timestamp("2015-09-05 23:56:04"), text("%Y-%m-%d %H:%M:%S"));
        let expected = "FORMAT(TIMESTAMP '2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%S')";
        test_expr(actual, expected);

        let actual = timestamp("2015-09-05 23:56:04").format(text("%Y-%m-%d %H:%M:%S"));
        let expected = "FORMAT(TIMESTAMP '2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%S')";
        test_expr(actual, expected);

        let actual = format(time("23:56:04"), text("%H:%M:%S"));
        let expected = "FORMAT(TIME '23:56:04', '%H:%M:%S')";
        test_expr(actual, expected);

        let actual = time("23:56:04").format(text("%H:%M:%S"));
        let expected = "FORMAT(TIME '23:56:04', '%H:%M:%S')";
        test_expr(actual, expected);
    }

    #[test]
    fn function_to_date() {
        let actual = to_date(text("2017-06-15"), text("%Y-%m-%d"));
        let expected = "TO_DATE('2017-06-15','%Y-%m-%d')";
        test_expr(actual, expected);

        let actual = text("2017-06-15").to_date(text("%Y-%m-%d"));
        let expected = "TO_DATE('2017-06-15','%Y-%m-%d')";
        test_expr(actual, expected);
    }

    #[test]
    fn function_to_timestamp() {
        let actual = to_timestamp(text("2015-09-05 23:56:04"), text("%Y-%m-%d %H:%M:%S"));
        let expected = "TO_TIMESTAMP('2015-09-05 23:56:04','%Y-%m-%d %H:%M:%S')";
        test_expr(actual, expected);

        let actual = text("2015-09-05 23:56:04").to_timestamp(text("%Y-%m-%d %H:%M:%S"));
        let expected = "TO_TIMESTAMP('2015-09-05 23:56:04','%Y-%m-%d %H:%M:%S')";
        test_expr(actual, expected);
    }

    #[test]
    fn function_to_time() {
        let actual = to_time(text("23:56:04"), text("%H:%M:%S"));
        let expected = "TO_TIME('23:56:04','%H:%M:%S')";
        test_expr(actual, expected);

        let actual = text("23:56:04").to_time(text("%H:%M:%S"));
        let expected = "TO_TIME('23:56:04','%H:%M:%S')";
        test_expr(actual, expected);
    }

    #[test]
    fn function_lower() {
        // Lower
        let actual = lower(text("ABC"));
        let expected = "LOWER('ABC')";
        test_expr(actual, expected);

        let actual = expr("HoHo").lower();
        let expected = "LOWER(HoHo)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_position() {
        let actual = position(expr("cake"), text("ke"));
        let expected = "POSITION('ke' IN cake)";
        test_expr(actual, expected);

        let actual = text("rice").position(text("cake"));
        let expected = "POSITION('cake' IN 'rice')";
        test_expr(actual, expected);
    }

    #[test]
    fn function_cast() {
        let actual = col("date").cast(DataType::Int);
        let expected = "CAST(date AS INTEGER)";
        test_expr(actual, expected);

        let actual = cast(expr("date"), "INTEGER");
        let expected = "CAST(date AS INTEGER)";
        test_expr(actual, expected);
    }

    #[test]
    fn function_extract() {
        let actual = col("date").extract(DateTimeField::Year);
        let expected = "EXTRACT(YEAR FROM date)";
        test_expr(actual, expected);

        let actual = extract(DateTimeField::Year, expr("date"));
        let expected = "EXTRACT(YEAR FROM date)";
        test_expr(actual, expected);
    }
}
