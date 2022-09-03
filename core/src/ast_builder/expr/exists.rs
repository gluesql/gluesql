use {super::ExprNode, crate::ast_builder::QueryNode};

pub fn exists<T: Into<QueryNode>>(query: T) -> ExprNode {
    ExprNode::Exists(Box::new(query.into()))
}

#[cfg(test)]
mod test {
    use crate::ast_builder::{col, exists, table, test, test_expr};

    #[test]
    fn exist() {
        let actual = table("FOO")
            .select()
            .filter(exists(
                table("BAR")
                    .select()
                    .filter("id IS NOT NULL")
                    .group_by("name"),
            ))
            .build();
        let expected =
            "SELECT * FROM FOO WHERE EXISTS (SELECT * FROM BAR WHERE id IS NOT NULL GROUP BY name)";
        test(actual, expected);

        let actual = table("FOO")
            .select()
            .filter(exists(table("BAR").select().filter("id IS NOT NULL")).not())
            .build();
        let expected =
            "SELECT * FROM FOO WHERE NOT EXISTS (SELECT * FROM BAR WHERE id IS NOT NULL)";
        test(actual, expected);

        let actual = exists(table("FOO").select().filter(col("id").gt(2)));
        let expected = "EXISTS (SELECT * FROM FOO WHERE id > 2)";
        test_expr(actual, expected);

        let actual = exists(table("FOO").select().filter(col("id").gt(2))).not();
        let expected = "NOT EXISTS (SELECT * FROM FOO WHERE id > 2)";
        test_expr(actual, expected);

        let actual = exists("SELECT * FROM FOO");
        let expected = "EXISTS (SELECT * FROM FOO)";
        test_expr(actual, expected);

        let actual = exists("SELECT * FROM FOO").not();
        let expected = "NOT EXISTS (SELECT * FROM FOO)";
        test_expr(actual, expected);
    }
}
