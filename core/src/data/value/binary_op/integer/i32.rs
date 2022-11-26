use {crate::prelude::Value, std::cmp::Ordering};

super::macros::impl_try_binary_op!(I32, i32);
#[cfg(test)]
super::macros::generate_binary_op_tests!(I32, i32);

super::macros::impl_partial_cmp_ord_method!(i32);
#[cfg(test)]
super::macros::generate_cmp_ord_tests!(i32);
