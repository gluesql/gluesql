use {crate::prelude::Value, std::cmp::Ordering};

super::macros::impl_try_binary_op!(U128, u128);
#[cfg(test)]
super::macros::generate_binary_op_tests!(U128, u128);

super::macros::impl_partial_cmp_ord_method!(u128);
#[cfg(test)]
super::macros::generate_cmp_ord_tests!(u128);
