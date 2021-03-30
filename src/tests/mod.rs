pub mod blend;
pub mod default;
pub mod error;
pub mod migrate;
pub mod nullable;
pub mod synthesize;

pub mod functions;
pub mod generic;
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
        generate_tests!($test, $storage,
            blend: blend::blend,
            default: default::default,
            error: error::error,
            //join_blend: join::blend,
            migrate: migrate::migrate,
            nullable: nullable::nullable,
            nullable_text: nullable::nullable_text,
            synthesize: synthesize::synthesize
        );

        #[cfg(feature = "alter-table")]
        generate_tests!($test, $storage, alter_table);

        generate_tests!($test, $storage,
            generic,
            functions,
            validate,
            aggregate,
            arithmetic
        );
    };
    ($test: meta, $storage: ident, $($name: ident: $func: expr),+) => {
        $(
            #[$test]
            async fn $name() {
                let path = stringify!($name);
                let storage = $storage::new(path);

                $func(storage).await;
            }
        )+
    };
    ($test: meta, $storage: ident, $($macro: ident),+) => {
        $(
            gluesql::$macro!($test, $storage);
        )+
    };
}
