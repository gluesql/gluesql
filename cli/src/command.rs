use crate::print::bool_from;

use {
    crate::print::{PrintOption, Tabular},
    std::fmt::Debug,
    thiserror::Error as ThisError,
};

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Help,
    Quit,
    Execute(String),
    ExecuteFromFile(String),
    SpoolOn(String),
    SpoolOff,
    Set(SetOption),
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

#[derive(Eq, Debug, PartialEq)]
pub enum SetOption {
    Tabular(bool),
    Colsep(String),
    Colwrap(String),
    Heading(bool),
}

impl SetOption {
    fn parse(key: &str, value: String, option: &PrintOption) -> Result<Self, CommandError> {
        let key = match (key.to_lowercase().as_str(), &option.tabular) {
            ("tabular", _) => Self::Tabular(bool_from(value)?),
            ("colsep", Tabular::Off { .. }) => Self::Colsep(value),
            ("colwrap", Tabular::Off { .. }) => Self::Colwrap(value),
            ("heading", Tabular::Off { .. }) => Self::Heading(bool_from(value)?),
            (_, Tabular::On) => {
                return Err(CommandError::WrongOption("run .set tabular OFF".into()))
            }

            _ => return Err(CommandError::WrongOption(key.into())),
        };

        Ok(key)
    }
}

impl Command {
    pub fn parse(line: &str, option: &PrintOption) -> Result<Self, CommandError> {
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
                    (Some(key), Some(value)) => {
                        Ok(Self::Set(SetOption::parse(key, value.to_string(), option)?))
                    }
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
    use crate::{
        command::CommandError,
        print::{PrintOption, Tabular},
    };

    #[test]
    fn parse_command() {
        use super::Command;
        let option = PrintOption {
            tabular: Tabular::On,
        };
        let parse = |command| Command::parse(command, &option);

        assert_eq!(parse(".help"), Ok(Command::Help));
        assert_eq!(parse("   .help;"), Ok(Command::Help));
        assert_eq!(parse(".quit"), Ok(Command::Quit));
        assert_eq!(parse(".quit;"), Ok(Command::Quit));
        assert_eq!(parse(" .quit; "), Ok(Command::Quit));
        assert_eq!(
            parse(".tables"),
            Ok(Command::Execute("SHOW TABLES".to_owned())),
        );
        assert_eq!(
            parse(".columns Foo"),
            Ok(Command::Execute("SHOW COLUMNS FROM Foo".to_owned())),
        );
        assert_eq!(parse(".columns"), Err(CommandError::LackOfTable));
        assert_eq!(
            parse(".version"),
            Ok(Command::Execute("SHOW VERSION".to_owned()))
        );
        assert_eq!(parse(".foo"), Err(CommandError::NotSupported));
        assert_eq!(
            parse("SELECT * FROM Foo;"),
            Ok(Command::Execute("SELECT * FROM Foo".to_owned())),
        );
        assert_eq!(
            parse(".spool query.log"),
            Ok(Command::SpoolOn("query.log".into()))
        );
        assert_eq!(parse(".spool off"), Ok(Command::SpoolOff));
        assert_eq!(parse(".spool"), Err(CommandError::LackOfFile));
        assert_eq!(
            parse(".set colsep ,"),
            Err(CommandError::WrongOption("run .set tabular OFF".into()))
        );
        assert_eq!(
            parse(".set colwrap '"),
            Err(CommandError::WrongOption("run .set tabular OFF".into()))
        );
        assert_eq!(
            parse(".set heading off"),
            Err(CommandError::WrongOption("run .set tabular OFF".into()))
        );
    }
}
