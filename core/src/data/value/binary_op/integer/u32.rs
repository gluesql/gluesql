use {crate::prelude::Value, std::cmp::Ordering};

super::macros::impl_try_binary_op!(U32, u32);
#[cfg(test)]
super::macros::generate_binary_op_tests!(U32, u32);

super::macros::impl_partial_cmp_ord_method!(u32);
#[cfg(test)]
super::macros::generate_cmp_ord_tests!(u32);
