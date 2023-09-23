use {
    rustyline::{
        validate::{ValidationContext, ValidationResult, Validator},
        Result,
    },
    rustyline_derive::{Completer, Helper, Highlighter, Hinter},
};

#[derive(Completer, Helper, Highlighter, Hinter)]
pub struct CliHelper;

impl Validator for CliHelper {
    fn validate(&self, ctx: &mut ValidationContext<'_>) -> Result<ValidationResult> {
        let input = ctx.input().trim();

        if input.ends_with(';') || input.starts_with('.') {
            Ok(ValidationResult::Valid(None))
        } else {
            Ok(ValidationResult::Incomplete)
        }
    }
}
