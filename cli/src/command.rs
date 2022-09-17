use {std::fmt::Debug, thiserror::Error as ThisError};

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Help,
    Quit,
    Execute(String),
    ExecuteFromFile(String),
    SpoolOn(String),
    SpoolOff,
    Set { key: String, value: String },
    Show(String),
}

#[derive(ThisError, Debug, PartialEq)]
pub enum CommandError {
    #[error("should specify table")]
    LackOfTable,
    #[error("should specify file path")]
    LackOfFile,
    #[error("should specify value for option")]
    LackOfValue,
    #[error("should specify option")]
    LackOfOption,
    #[error("cannot support option: {0}")]
    WrongOption(String),
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
                    Some(&"off") => Ok(Self::SpoolOff),
                    Some(path) => Ok(Self::SpoolOn(path.to_string())),
                    None => Err(CommandError::LackOfFile),
                },
                ".set" => match (params.get(1), params.get(2)) {
                    (Some(key), Some(value)) => Ok(Self::Set {
                        key: key.to_string(),
                        value: value.to_string(),
                    }),
                    (Some(_), None) => Err(CommandError::LackOfValue),
                    (None, Some(_)) => Err(CommandError::LackOfOption),
                    (None, None) => Err(CommandError::LackOfOption),
                },
                ".show" => match params.get(1) {
                    Some(option) => Ok(Self::Show(option.to_string())),
                    None => Err(CommandError::LackOfOption),
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
        assert_eq!(
            Ok(Command::SpoolOn("query.log".into())),
            Command::parse(".spool query.log")
        );
        assert_eq!(Ok(Command::SpoolOff), Command::parse(".spool off"));
        assert_eq!(Err(CommandError::LackOfFile), Command::parse(".spool"));
    }
}
