#![cfg(feature = "test-git-remote")]

use {
    core::str,
    gluesql_core::prelude::Glue,
    gluesql_git_storage::GitStorage,
    gluesql_json_storage::JsonStorage,
    std::{
        env,
        fs::remove_dir_all,
        process::{Command, Output},
    },
    uuid::Uuid,
};

#[tokio::test]
async fn pull_and_push() {
    let remote =
        env::var("GIT_REMOTE").unwrap_or("git@github.com:gluesql/git-storage-test.git".to_owned());

    let path = "./tmp/git-storage-test/";
    let _ = remove_dir_all(path);

    let validate_err = |output: Output| {
        if !output.status.success() {
            panic!("{:?}", str::from_utf8(&output.stderr).unwrap());
        }
    };

    Command::new("git")
        .current_dir("./tmp")
        .arg("clone")
        .arg(&remote)
        .output()
        .map(validate_err)
        .unwrap();

    let branch = format!("test-{}", Uuid::now_v7());
    Command::new("git")
        .current_dir(path)
        .arg("checkout")
        .arg("-b")
        .arg(&branch)
        .output()
        .map(validate_err)
        .unwrap();

    let mut storage: GitStorage<JsonStorage> = GitStorage::open(path).unwrap();
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

    Command::new("git")
        .current_dir(path)
        .arg("push")
        .arg(remote)
        .arg("-d")
        .arg(branch)
        .output()
        .map(validate_err)
        .unwrap();
}
