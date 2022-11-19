use {crate::prelude::Value, std::cmp::Ordering};

super::macros::impl_try_binary_op!(U16, u16);
#[cfg(test)]
super::macros::generate_binary_op_tests!(U16, u16);

super::macros::impl_partial_cmp_ord_method!(u16);
#[cfg(test)]
super::macros::generate_cmp_ord_tests!(u16);
