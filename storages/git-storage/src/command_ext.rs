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
            return Err(Error::StorageMsg(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }

        Ok(())
    }
}
