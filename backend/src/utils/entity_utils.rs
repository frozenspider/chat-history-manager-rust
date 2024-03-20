use std::fmt::{Display, Formatter};

use crate::prelude::*;

pub mod entity_equality;

//
// Helper entities
//

impl Display for Difference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)?;
        if let Some(ref values) = self.values {
            write!(f, "\nWas:    {}\nBecame: {}", values.old, values.new)?;
        }
        Ok(())
    }
}

impl From<ChatWithDetailsPb> for ChatWithDetails {
    fn from(value: ChatWithDetailsPb) -> Self {
        Self {
            chat: value.chat,
            last_msg_option: value.last_msg_option,
            members: value.members,
        }
    }
}

impl From<ChatWithDetails> for ChatWithDetailsPb {
    fn from(value: ChatWithDetails) -> Self {
        Self {
            chat: value.chat,
            last_msg_option: value.last_msg_option,
            members: value.members,
        }
    }
}
