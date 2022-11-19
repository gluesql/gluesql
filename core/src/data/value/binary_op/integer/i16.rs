use {crate::prelude::Value, std::cmp::Ordering};

super::macros::impl_try_binary_op!(I16, i16);
#[cfg(test)]
super::macros::generate_binary_op_tests!(I16, i16);

super::macros::impl_partial_cmp_ord_method!(i16);
#[cfg(test)]
super::macros::generate_cmp_ord_tests!(i16);
