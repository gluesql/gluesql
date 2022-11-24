use {crate::prelude::Value, std::cmp::Ordering};

super::macros::impl_try_binary_op!(I128, i128);
#[cfg(test)]
super::macros::generate_binary_op_tests!(I128, i128);

super::macros::impl_partial_cmp_ord_method!(i128);
#[cfg(test)]
super::macros::generate_cmp_ord_tests!(i128);
