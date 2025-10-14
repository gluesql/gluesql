use gluesql_macros::FromGlueRow;

#[derive(FromGlueRow)]
struct S {
    #[glue]
    v: i32,
}
