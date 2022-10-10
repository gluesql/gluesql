#[macro_export]
macro_rules! impl_try_binary_op {
    ($variant: ident, $primitive: ident) => {
		macro_rules! impl_interval_method {
			(checked_mul, $lhs_variant: ident, $op: ident, $lhs: ident, $rhs: ident) => {
				return Ok(Value::Interval($lhs * $rhs))
			};
			($other: ident, $lhs_variant: ident, $op: ident, $lhs: ident, $rhs: ident) => {
				return Err(ValueError::NonNumericMathOperation {
					lhs: $lhs_variant($lhs),
					operator: $op,
					rhs: Value::Interval($rhs),
				}
				.into())
			};
		}

		macro_rules! impl_method {
			($lhs_variant: ident, $lhs_primitive: ident, $lhs: ident, $method: ident, $op: ident, $rhs: ident) => {{
				match *$rhs {
					I8(rhs) => $lhs.$method($lhs_primitive::try_from($rhs)?).ok_or_else(|| {
						ValueError::BinaryOperationOverflow {
							lhs: $lhs_variant($lhs),
							rhs: I8(rhs),
							operator: $op,
						}
						.into()
					}),
					I16(rhs) => $lhs.$method($lhs_primitive::try_from($rhs)?).ok_or_else(|| {
						ValueError::BinaryOperationOverflow {
							lhs: $lhs_variant($lhs),
							rhs: I16(rhs),
							operator: $op,
						}
						.into()
					}),
					I32(rhs) => $lhs.$method($lhs_primitive::try_from($rhs)?).ok_or_else(|| {
						ValueError::BinaryOperationOverflow {
							lhs: $lhs_variant($lhs),
							rhs: I32(rhs),
							operator: $op,
						}
						.into()
					}),
					I64(rhs) => $lhs.$method($lhs_primitive::try_from($rhs)?).ok_or_else(|| {
						ValueError::BinaryOperationOverflow {
							lhs: $lhs_variant($lhs),
							rhs: I64(rhs),
							operator: $op,
						}
						.into()
					}),
					I128(rhs) => $lhs.$method($lhs_primitive::try_from($rhs)?).ok_or_else(|| {
						ValueError::BinaryOperationOverflow {
							lhs: $lhs_variant($lhs),
							rhs: I128(rhs),
							operator: $op,
						}
						.into()
					}),
					U8(rhs) => $lhs.$method($lhs_primitive::try_from($rhs)?).ok_or_else(|| {
						ValueError::BinaryOperationOverflow {
							lhs: $lhs_variant($lhs),
							rhs: U8(rhs),
							operator: $op,
						}
						.into()
					}),
					F64(rhs) => $lhs.$method($lhs_primitive::try_from($rhs)?).ok_or_else(|| {
						ValueError::BinaryOperationOverflow {
							lhs: $lhs_variant($lhs),
							rhs: F64(rhs),
							operator: $op,
						}
						.into()
					}),
					Decimal(rhs) => $lhs.$method($lhs_primitive::try_from($rhs)?).ok_or_else(|| {
						ValueError::BinaryOperationOverflow {
							lhs: $lhs_variant($lhs),
							rhs: Decimal(rhs),
							operator: $op,
						}
						.into()
					}),
					Null => return Ok(Null),
					Interval(rhs) => {
						impl_interval_method!($method, $lhs_variant, $op, $lhs, rhs);
					}
					_ => Err(ValueError::NonNumericMathOperation {
						lhs: $lhs_variant($lhs),
						operator: $op,
						rhs: $rhs.clone(),
					}
					.into()),
				}
				.map($lhs_variant)
			}};
		}

        use $crate::{
            data::value::{
                error::{NumericBinaryOperator::*, ValueError},
                TryBinaryOperator,
                Value::*,
            },
            result::Result,
        };

        impl TryBinaryOperator for $primitive {
            type Rhs = Value;

            fn try_add(&self, rhs: &Self::Rhs) -> Result<Value> {
                let lhs = *self;
                impl_method!($variant, $primitive, lhs, checked_add, Add, rhs)
            }

            fn try_subtract(&self, rhs: &Self::Rhs) -> Result<Value> {
                let lhs = *self;
                impl_method!($variant, $primitive, lhs, checked_sub, Subtract, rhs)
            }

            fn try_multiply(&self, rhs: &Self::Rhs) -> Result<Value> {
                let lhs = *self;
                impl_method!($variant, $primitive, lhs, checked_mul, Multiply, rhs)
            }

            fn try_divide(&self, rhs: &Self::Rhs) -> Result<Value> {
                let lhs = *self;
                impl_method!($variant, $primitive, lhs, checked_div, Divide, rhs)
            }

            fn try_modulo(&self, rhs: &Self::Rhs) -> Result<Value> {
                let lhs = *self;
                impl_method!($variant, $primitive, lhs, checked_rem, Modulo, rhs)
            }
        }
    };
}
