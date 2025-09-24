use gluesql_macros::FromGlueRow;

#[derive(FromGlueRow)]
struct T(i32);
