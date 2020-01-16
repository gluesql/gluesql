use crate::translator;
use std::convert::From;
use translator::CommandType;

pub struct CommandQueue {
    pub items: Vec<CommandType>,
}

impl From<Vec<CommandType>> for CommandQueue {
    fn from(items: Vec<CommandType>) -> Self {
        CommandQueue { items }
    }
}
