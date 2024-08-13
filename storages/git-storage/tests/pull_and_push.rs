#![cfg(feature = "test-git-remote")]

use {
    gluesql_core::prelude::Glue,
    gluesql_git_storage::{GitStorage, StorageType},
    std::{env, fs::remove_dir_all},
    uuid::Uuid,
};

#[tokio::test]
async fn pull_and_push() {
    let remote =
        env::var("GIT_REMOTE").unwrap_or("git@github.com:gluesql/git-storage-test.git".to_owned());
    let path = "./tmp/git-storage-test/";
    let _ = remove_dir_all(path);
    GitStorage::git("./tmp", &["clone", &remote]).unwrap();

    let branch = format!("test-{}", Uuid::now_v7());
    GitStorage::git(path, &["checkout", "-b", &branch]).unwrap();

    let mut storage = GitStorage::open(path, StorageType::Json).unwrap();
    storage.set_remote(remote.clone());
    storage.set_branch(branch.clone());
    storage.pull().unwrap();

    let mut glue = Glue::new(storage);
    glue.execute("CREATE TABLE Foo (id INTEGER);")
        .await
        .unwrap();
    glue.execute("INSERT INTO Foo VALUES (1), (2), (3);")
        .await
        .unwrap();

    glue.storage.push().unwrap();

    GitStorage::git(path, &["push", "remote", "-d", &branch]).unwrap();
}
