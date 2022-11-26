use {crate::prelude::Value, std::cmp::Ordering};

super::macros::impl_try_binary_op!(U8, u8);
#[cfg(test)]
super::macros::generate_binary_op_tests!(U8, u8);

super::macros::impl_partial_cmp_ord_method!(u8);
#[cfg(test)]
super::macros::generate_cmp_ord_tests!(u8);
