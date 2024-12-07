use {
    crate::ResultExt,
    gluesql_core::error::{Error, Result},
    std::process::Command,
};

pub trait CommandExt {
    fn execute(&mut self) -> Result<(), Error>;
}

impl CommandExt for Command {
    fn execute(&mut self) -> Result<(), Error> {
        let output = self.output().map_storage_err()?;

        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);

            let out_and_err = [
                (!stdout.is_empty()).then(|| format!("[stdout] {}", stdout)),
                (!stderr.is_empty()).then(|| format!("[stderr] {}", stderr)),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("");

            return Err(Error::StorageMsg(out_and_err));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {crate::CommandExt, std::process::Command};

    #[test]
    fn test_command_ext() {
        let path = "tmp/command_ext_test";

        Command::new("mkdir").arg("-p").arg(path).execute().unwrap();
        Command::new("git")
            .current_dir(path)
            .arg("init")
            .execute()
            .unwrap();
        Command::new("git")
            .current_dir(path)
            .arg("checkout")
            .arg("-b")
            .arg("main")
            .execute()
            .unwrap();

        let executed = Command::new("git")
            .current_dir(path)
            .arg("commit")
            .arg("-m")
            .arg("test")
            .execute();
        assert!(executed.is_err());
        assert_eq!(
        executed.unwrap_err().to_string(),
        "storage: [stdout] On branch main\n\nInitial commit\n\nnothing to commit (create/copy files and use \"git add\" to track)\n"
    );

        let executed = Command::new("git")
            .current_dir(path)
            .arg("commit")
            .arg("-m")
            .arg("test")
            .arg("--amend")
            .execute();
        assert!(executed.is_err());
        assert_eq!(
            executed.unwrap_err().to_string(),
            "storage: [stderr] fatal: You have nothing to amend.\n"
        );
    }
}
