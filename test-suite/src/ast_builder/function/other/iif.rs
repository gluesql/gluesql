use {
    crate::*,
    gluesql_core::{ast::ToSql, ast_builder::{function as f, expr, *}},
};

test_case!(iif_ast_builder, {

    // free function (true branch)
    let sql = values(vec![vec![f::iif(expr("1 < 2"), num(10), num(20))]])
        .build().unwrap().to_sql();
    assert_eq!(sql, "VALUES (IIF(1 < 2, 10, 20))");

    // method chain style (column condition)
    let sql = values(vec![vec![col("flag").iif(text("yes"), text("no"))]])
        .build().unwrap().to_sql();
    assert_eq!(sql, "VALUES (IIF(flag, 'yes', 'no'))");

    // free function style (false branch)
    let sql = values(vec![vec![f::iif(expr("1 > 2"), num(10), num(20))]])
        .build().unwrap().to_sql();
    assert_eq!(sql, "VALUES (IIF(1 > 2, 10, 20))");

    // alias in projection
    let sql = table("t")
        .select()
        .project(col("id"))
        .project(col("flag").iif(text("Y"), text("N")).alias_as("yn"))
        .build().unwrap().to_sql();
    assert_eq!(sql, "SELECT id, IIF(flag, 'Y', 'N') AS yn FROM t");

    // IIF as filter expression
    let sql = table("t")
        .select()
        .filter(f::iif(expr("score >= 60"), expr("TRUE"), expr("FALSE")))
        .project("*")
        .build()
        .unwrap()
        .to_sql();
    assert_eq!(sql, "SELECT * FROM t WHERE IIF(score >= 60, TRUE, FALSE)");

    // NULL argument
    let sql = values(vec![vec![f::iif(col("flag"), expr("NULL"), text("no"))]])
        .build().unwrap().to_sql();
    assert_eq!(sql, "VALUES (IIF(flag, NULL, 'no'))");

    // nested IIF
    let inner = f::iif(expr("b"), num(1), num(2));
    let outer = f::iif(expr("a"), inner, num(3));
    let sql = values(vec![vec![outer]])
        .build().unwrap().to_sql();
    assert_eq!(sql, "VALUES (IIF(a, IIF(b, 1, 2), 3))");
});
