pub mod column_options;
pub mod functions;
pub mod generic;
pub mod miscellaneous;
pub mod validate;
pub use functions::{aggregate, arithmetic};

#[cfg(feature = "alter-table")]
pub mod alter_table;

pub mod macros;
mod tester;
pub use tester::*;

#[macro_export]
macro_rules! generate_tests {
    ($test: meta, $storage: ident) => {
        #[cfg(feature = "alter-table")]
        generate_tests!($test, $storage, alter_table);

        generate_tests!($test, $storage,
            generic,
            functions,
            validate,
            aggregate,
            arithmetic,
            miscellaneous,
            column_options
        );
    };
    ($test: meta, $storage: ident, $($macro: ident),+) => {
        $(
            gluesql::$macro!($test, $storage);
        )+
    };
}
