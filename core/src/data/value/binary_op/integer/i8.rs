use {crate::prelude::Value, std::cmp::Ordering};

super::macros::impl_try_binary_op!(I8, i8);
#[cfg(test)]
super::macros::generate_binary_op_tests!(I8, i8);

super::macros::impl_partial_cmp_ord_method!(i8);
#[cfg(test)]
super::macros::generate_cmp_ord_tests!(i8);
