use gluesql::FromGlueRow;

#[derive(Debug, PartialEq, FromGlueRow)]
struct User {
    id: i64,
    name: String,
    email: Option<String>,
}

#[test]
fn end_to_end_with_memory_storage() {
    use gluesql::prelude::{Glue, MemoryStorage};
    use gluesql::row::SelectResultExt;

    let fut = async move {
        let storage = MemoryStorage::default();
        let mut glue = Glue::new(storage);

        glue.execute("CREATE TABLE users (id INTEGER, name TEXT, email TEXT);")
            .await
            .unwrap();

        glue.execute(
            "INSERT INTO users (id, name, email) VALUES (1, 'A', NULL), (2, 'B', 'b@x.com');",
        )
        .await
        .unwrap();

        use gluesql::row::SelectExt;
        let users: Vec<User> = glue
            .execute("SELECT id, name, email FROM users;")
            .await
            .rows_as::<User>()
            .unwrap();
        assert_eq!(users.len(), 2);
        assert_eq!(
            users[0],
            User {
                id: 1,
                name: "A".into(),
                email: None
            }
        );
        assert_eq!(users[1].email.as_deref(), Some("b@x.com"));
    };

    futures::executor::block_on(fut);
}
