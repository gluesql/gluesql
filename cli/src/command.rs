#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Help,
    Quit,
    Execute(String),
    ExecuteFromFile(String),
}

impl Command {
    pub fn parse(line: &str) -> Result<Self, ()> {
        let line = line.trim_start().trim_end_matches(|c| c == ' ' || c == ';');
        // We detect if the line is a command or not
        if line.starts_with('.') {
            let params: Vec<&str> = line.split_whitespace().collect();
            match params[0] {
                ".help" => Ok(Self::Help),
                ".quit" => Ok(Self::Quit),
                ".tables" => Ok(Self::Execute("SHOW TABLES".to_owned())),
                ".columns" => match params.len() > 1 {
                    true => Ok(Self::Execute(
                        format!("SHOW COLUMNS FROM {}", params[1]).to_owned(),
                    )),
                    false => Err(()), // should throw another error
                },
                ".version" => Ok(Self::Execute("SHOW VERSION".to_owned())),
                ".execute" if params.len() == 2 => Ok(Self::ExecuteFromFile(params[1].to_owned())),
                _ => Err(()),
            }
        } else {
            Ok(Self::Execute(line.to_owned()))
        }
    }
}

#[cfg(test)]
mod tests {
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
            Command::parse(".column Foo")
        );
        assert_eq!(
            Ok(Command::Execute("SHOW VERSION".to_owned())),
            Command::parse(".version")
        );
        assert_eq!(Err(()), Command::parse(".foo"));
        assert_eq!(
            Ok(Command::Execute("SELECT * FROM Foo".to_owned())),
            Command::parse("SELECT * FROM Foo;")
        );
    }
}
