use {crate::print::PrintOption, std::fmt::Debug, thiserror::Error as ThisError};

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Help,
    Quit,
    Execute(String),
    ExecuteFromFile(String),
    SpoolOn(String),
    SpoolOff,
    Set(SetOption),
    Show(ShowOption),
    Edit(Option<String>),
    Run,
}

#[derive(ThisError, Debug, PartialEq, Eq)]
pub enum CommandError {
    #[error("should specify table")]
    LackOfTable,
    #[error("should specify file path")]
    LackOfFile,
    #[error("should specify value for option")]
    LackOfValue(String),
    #[error("should specify option")]
    LackOfOption,
    #[error("cannot support option: {0}")]
    WrongOption(String),
    #[error("command not supported")]
    NotSupported,
    #[error("Nothing in SQL history to run.")]
    LackOfSQLHistory,
}

#[derive(Eq, Debug, PartialEq)]
pub enum SetOption {
    Tabular(bool),
    Colsep(String),
    Colwrap(String),
    Heading(bool),
}

impl SetOption {
    fn parse(key: &str, value: Option<&&str>, option: &PrintOption) -> Result<Self, CommandError> {
        fn bool_from(value: String) -> Result<bool, CommandError> {
            match value.to_uppercase().as_str() {
                "ON" => Ok(true),
                "OFF" => Ok(false),
                _ => Err(CommandError::WrongOption(value)),
            }
        }

        if let Some(value) = value {
            let value = match *value {
                "\"\"" => "",
                _ => value,
            }
            .to_owned();

            let set_option = match (key.to_lowercase().as_str(), &option.tabular) {
                ("tabular", _) => Self::Tabular(bool_from(value)?),
                ("colsep", false) => Self::Colsep(value),
                ("colwrap", false) => Self::Colwrap(value),
                ("heading", false) => Self::Heading(bool_from(value)?),
                (_, true) => return Err(CommandError::WrongOption("run .set tabular OFF".into())),

                _ => return Err(CommandError::WrongOption(key.into())),
            };

            Ok(set_option)
        } else {
            let payload = match key.to_lowercase().as_str() {
                "tabular" => "Usage: .set tabular {ON|OFF}",
                "colsep" => "Usage: .set colsep {\"\"|TEXT}",
                "colwrap" => "Usage: .set colwrap {\"\"|TEXT}",
                "heading" => "Usage: .set heading {ON|OFF}",

                _ => return Err(CommandError::WrongOption(key.into())),
            };

            Err(CommandError::LackOfValue(payload.into()))
        }
    }
}

#[derive(Eq, Debug, PartialEq)]
pub enum ShowOption {
    Tabular,
    Colsep,
    Colwrap,
    Heading,
    All,
}

impl ShowOption {
    fn parse(key: &str) -> Result<Self, CommandError> {
        let show_option = match key.to_lowercase().as_str() {
            "tabular" => Self::Tabular,
            "colsep" => Self::Colsep,
            "colwrap" => Self::Colwrap,
            "heading" => Self::Heading,
            "all" => Self::All,
            _ => return Err(CommandError::WrongOption(key.into())),
        };

        Ok(show_option)
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
                    (Some(key), value) => Ok(Self::Set(SetOption::parse(key, value, option)?)),
                    (None, Some(_)) => Err(CommandError::LackOfOption),
                    (None, None) => Err(CommandError::LackOfOption),
                },
                ".show" => match params.get(1) {
                    Some(key) => Ok(Self::Show(ShowOption::parse(key)?)),
                    None => Err(CommandError::LackOfOption),
                },
                ".edit" => Ok(Self::Edit(params.get(1).map(|&v| v.to_owned()))),
                ".run" => Ok(Self::Run),

                _ => Err(CommandError::NotSupported),
            }
        } else {
            Ok(Self::Execute(line.to_owned()))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{command::CommandError, print::PrintOption};

    #[test]
    fn parse_command() {
        use super::{Command, SetOption, ShowOption};
        let option = PrintOption::default();
        let parse = |command| Command::parse(command, &option);

        assert_eq!(parse(".help"), Ok(Command::Help));
        assert_eq!(parse("   .help;"), Ok(Command::Help));
        assert_eq!(parse(".quit"), Ok(Command::Quit));
        assert_eq!(parse(".quit;"), Ok(Command::Quit));
        assert_eq!(parse(" .quit; "), Ok(Command::Quit));
        assert_eq!(parse(".run"), Ok(Command::Run));
        assert_eq!(parse(".edit"), Ok(Command::Edit(None)));
        assert_eq!(
            parse(".edit foo.sql"),
            Ok(Command::Edit(Some("foo.sql".into())))
        );
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
        assert_eq!(parse(".abc"), Err(CommandError::NotSupported));
        assert_eq!(
            parse(".set abc"),
            Err(CommandError::WrongOption("abc".to_owned()))
        );
        assert_eq!(
            parse(".set tabular abc"),
            Err(CommandError::WrongOption("abc".to_owned()))
        );
        assert_eq!(
            parse(".set tabular off"),
            Ok(Command::Set(SetOption::Tabular(false)))
        );
        assert_eq!(
            parse(".set tabular on"),
            Ok(Command::Set(SetOption::Tabular(true)))
        );

        let mut option = PrintOption::default();
        option.tabular(false);
        let parse = |command| Command::parse(command, &option);

        assert_eq!(
            parse(".set tabular"),
            Err(CommandError::LackOfValue(
                "Usage: .set tabular {ON|OFF}".into()
            ))
        );
        assert_eq!(
            parse(".set colsep"),
            Err(CommandError::LackOfValue(
                "Usage: .set colsep {\"\"|TEXT}".into()
            ))
        );
        assert_eq!(
            parse(".set colwrap"),
            Err(CommandError::LackOfValue(
                "Usage: .set colwrap {\"\"|TEXT}".into()
            ))
        );
        assert_eq!(
            parse(".set heading"),
            Err(CommandError::LackOfValue(
                "Usage: .set heading {ON|OFF}".into()
            ))
        );
        assert_eq!(parse(".set"), Err(CommandError::LackOfOption));
        assert_eq!(
            parse(".show tabular"),
            Ok(Command::Show(ShowOption::Tabular))
        );
        assert_eq!(parse(".show colsep"), Ok(Command::Show(ShowOption::Colsep)));
        assert_eq!(
            parse(".show colwrap"),
            Ok(Command::Show(ShowOption::Colwrap))
        );
        assert_eq!(
            parse(".show heading"),
            Ok(Command::Show(ShowOption::Heading))
        );
        assert_eq!(parse(".show all"), Ok(Command::Show(ShowOption::All)));
        assert_eq!(
            parse(".show abc"),
            Err(CommandError::WrongOption("abc".to_owned()))
        );
        assert_eq!(parse(".show"), Err(CommandError::LackOfOption));
    }
}
