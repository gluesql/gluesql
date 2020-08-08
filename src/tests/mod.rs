pub mod aggregate;
pub mod arithmetic;
pub mod basic;
pub mod blend;
pub mod drop_table;
pub mod error;
pub mod join;
pub mod migrate;
pub mod nested_select;
pub mod nullable;
pub mod ordering;
pub mod sql_types;
pub mod synthesize;
mod tester;

pub mod macros;

pub use tester::*;

#[macro_export]
macro_rules! generate_tests {
    ($test: meta, $storage: ident) => {
        macro_rules! glue {
            ($title: ident, $func: path) => {
                #[$test]
                fn $title() {
                    let path = stringify!($title);
                    let storage = $storage::new(path);

                    $func(storage);
                }
            };
        }

        glue!(basic, basic::basic);
        glue!(aggregate, aggregate::aggregate);
        glue!(arithmetic, arithmetic::arithmetic);
        glue!(arithmetic_blend, arithmetic::blend);
        glue!(blend, blend::blend);
        glue!(drop_table, drop_table::drop_table);
        glue!(error, error::error);
        glue!(join, join::join);
        glue!(join_blend, join::blend);
        glue!(migrate, migrate::migrate);
        glue!(nested_select, nested_select::nested_select);
        glue!(nullable, nullable::nullable);
        glue!(ordering, ordering::ordering);
        glue!(sql_types, sql_types::sql_types);
        glue!(synthesize, synthesize::synthesize);
    };
}
