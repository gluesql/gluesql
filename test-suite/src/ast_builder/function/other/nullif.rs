use {
    crate::*,
    gluesql_core::{
        ast_builder::{function as f, *},
        executor::Payload,
        prelude::Value::*,
    },
};

test_case!(ifnull, {
    let glue = get_glue!();
    // TODO(@miinhho) implement nullif test with ast builder
});
