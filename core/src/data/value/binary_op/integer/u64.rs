use {crate::prelude::Value, std::cmp::Ordering};

super::macros::impl_try_binary_op!(U64, u64);
#[cfg(test)]
super::macros::generate_binary_op_tests!(U64, u64);

super::macros::impl_partial_cmp_ord_method!(u64);
#[cfg(test)]
super::macros::generate_cmp_ord_tests!(u64);
