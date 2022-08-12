use {std::fmt::Debug, thiserror::Error as ThisError};

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Help,
    Quit,
    Execute(String),
    ExecuteFromFile(String),
    SpoolOn(String),
}

#[derive(ThisError, Debug, PartialEq)]
pub enum CommandError {
    #[error("should specify table")]
    LackOfTable,
    #[error("should specify file path")]
    LackOfFile,
    #[error("command not supported")]
    NotSupported,
}

impl Command {
    pub fn parse(line: &str) -> Result<Self, CommandError> {
        let line = line.trim_start().trim_end_matches(|c| c == ' ' || c == ';');
        // We detect if the line is a command or not
        if line.starts_with('.') {
            let params: Vec<&str> = line.split_whitespace().collect();
            match params[0] {
                ".help" => Ok(Self::Help),
                ".quit" => Ok(Self::Quit),
                ".tables" => Ok(Self::Execute("SHOW TABLES".to_owned())),
                ".columns" => match params.get(1) {
                    Some(table_name) => {
                        Ok(Self::Execute(format!("SHOW COLUMNS FROM {}", table_name)))
                    }
                    None => Err(CommandError::LackOfTable),
                },
                ".version" => Ok(Self::Execute("SHOW VERSION".to_owned())),
                ".execute" if params.len() == 2 => Ok(Self::ExecuteFromFile(params[1].to_owned())),
                ".spool" => match params.get(1) {
                    Some(path) => Ok(Self::SpoolOn(path.to_string())),
                    None => Err(CommandError::LackOfFile),
                },
                _ => Err(CommandError::NotSupported),
            }
        } else {
            Ok(Self::Execute(line.to_owned()))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::command::CommandError;

    #[test]
    fn parse_command() {
        use super::Command;

        assert_eq!(Ok(Command::Help), Command::parse(".help"));
        assert_eq!(Ok(Command::Help), Command::parse("   .help;"));
        assert_eq!(Ok(Command::Quit), Command::parse(".quit"));
        assert_eq!(Ok(Command::Quit), Command::parse(".quit;"));
        assert_eq!(Ok(Command::Quit), Command::parse(" .quit; "));
        assert_eq!(
            Ok(Command::Execute("SHOW TABLES".to_owned())),
            Command::parse(".tables")
        );
        assert_eq!(
            Ok(Command::Execute("SHOW COLUMNS FROM Foo".to_owned())),
            Command::parse(".columns Foo")
        );
        assert_eq!(Err(CommandError::LackOfTable), Command::parse(".columns"));
        assert_eq!(
            Ok(Command::Execute("SHOW VERSION".to_owned())),
            Command::parse(".version")
        );
        assert_eq!(Err(CommandError::NotSupported), Command::parse(".foo"));
        assert_eq!(
            Ok(Command::Execute("SELECT * FROM Foo".to_owned())),
            Command::parse("SELECT * FROM Foo;")
        );
    }
}
