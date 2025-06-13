# Git Storage

Git Storage allows GlueSQL to track table schemas and data inside a Git repository. It wraps one of the existing file based storages (File, CSV or JSON) and commits every change automatically.

Whenever a schema or data modification is performed, GitStorage runs `git add .` followed by a commit containing a short message describing the operation. This means each `INSERT`, `DELETE` or schema change creates a commit in your repository.

Remote synchronisation is kept manual to give you full control. Use [`pull()`](https://docs.rs/gluesql-git-storage/latest/gluesql_git_storage/struct.GitStorage.html#method.pull) and [`push()`](https://docs.rs/gluesql-git-storage/latest/gluesql_git_storage/struct.GitStorage.html#method.push) when you want to fetch or upload commits. The default remote is `origin` on the `main` branch but you can change these with `set_remote` and `set_branch`.

## Example

```rust
use gluesql::prelude::Glue;
use gluesql_git_storage::{GitStorage, StorageType};

#[tokio::main]
async fn main() -> gluesql::result::Result<()> {
    // initialise a new repository using JSON files
    let storage = GitStorage::init("data/git_db", StorageType::Json)?;
    let mut glue = Glue::new(storage);

    // create table and insert some rows - each call creates a Git commit
    glue.execute("CREATE TABLE Foo (id INT);").await?;
    glue.execute("INSERT INTO Foo VALUES (1), (2);").await?;

    // push commits to remote when you are ready
    glue.storage.push()?;
    Ok(())
}
```

Manually run `pull()` before executing commands if you want to update the working copy from the remote. Pushing is also manual so you can control when commits are shared.

