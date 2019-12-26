use crate::translator;
use translator::CommandType;
use translator::CommandType::{GET, SET};

pub struct CommandQueue {
    pub items: Vec<CommandType>,
}

impl CommandQueue {
    pub fn new() -> CommandQueue {
        let items = vec![SET, GET];

        CommandQueue { items }
    }
}
