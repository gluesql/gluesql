#[cfg(feature = "gluesql-redb-storage")]
mod api_usage {
    use {
        gluesql::prelude::{Glue, RedbStorage},
        std::fs,
    };

    pub fn run() {
        let redb_dir = "/tmp/gluesql";
        let redb_path = format!("{redb_dir}/api_usage");
        fs::create_dir_all(redb_dir).unwrap();
        fs::remove_file(&redb_path).unwrap_or(());
        let storage = RedbStorage::new(redb_path).unwrap();
        let mut glue = Glue::new(storage);

        let sqls = [
            "CREATE TABLE Glue (id INTEGER);",
            "INSERT INTO Glue VALUES (100);",
            "INSERT INTO Glue VALUES (200);",
            "DROP TABLE Glue;",
        ];

        for sql in sqls {
            glue.execute(sql).unwrap();
        }
    }
}

fn main() {
    #[cfg(feature = "gluesql-redb-storage")]
    api_usage::run();
}
