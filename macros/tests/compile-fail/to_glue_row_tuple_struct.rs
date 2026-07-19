use gluesql_macros::ToGlueRow;

#[derive(ToGlueRow)]
struct T(i32);
