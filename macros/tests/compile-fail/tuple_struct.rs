use gluesql::FromGlueRow;

#[derive(FromGlueRow)]
struct T(i32);

