pub mod aggregate;
pub mod arithmetic;

use crate::build_suite;

build_suite!(functions; cast, left_right, upper_lower);
