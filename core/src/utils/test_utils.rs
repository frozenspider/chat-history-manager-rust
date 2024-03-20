use itertools::Itertools;
use crate::protobuf::history::*;
use crate::utils::entity_utils::*;

pub const fn src_id(id: i64) -> MessageSourceId { MessageSourceId(id) }

impl Message {
    pub fn source_id(&self) -> MessageSourceId { src_id(self.source_id_option.unwrap()) }
}

impl RichText {
    pub fn unwrap(rtes: &[RichTextElement]) -> Vec<&rich_text_element::Val> {
        rtes.iter().map(|rte| rte.val.as_ref().unwrap()).collect_vec()
    }

    pub fn unwrap_copy(rtes: &[RichTextElement]) -> Vec<rich_text_element::Val> {
        Self::unwrap(rtes).into_iter().cloned().collect_vec()
    }
}
