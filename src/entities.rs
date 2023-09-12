use std::path::Path;

use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;

use crate::protobuf::history::*;
use crate::protobuf::history::message_service::SealedValueOptional;

// TODO: Replace with some sort of tagget types?
pub type DatasetRoot = Path;
pub type MessageSourceId = i64;
pub type MessageInternalId = i64;
pub type Timestamp = i64;

pub const UNNAMED: &str = "[unnamed]";
pub const UNKNOWN: &str = "[unknown]";

pub const NO_INTERNAL_ID: MessageInternalId = -1;

pub struct ChatWithDetails {
    pub chat: Chat,
    pub last_msg_option: Option<Message>,
    /** First element MUST be myself, the rest should be in some fixed order. */
    pub members: Vec<User>,
}

impl ChatWithDetails {
    pub fn ds_uuid(&self) -> &PbUuid {
        self.chat.ds_uuid.as_ref().unwrap()
    }

    /** Used to resolve plaintext members */
    pub fn resolve_member_index(&self, member_name: &str) -> Option<usize> {
        self.members.iter().position(|m| m.pretty_name() == member_name)
    }

    /** Used to resolve plaintext members */
    pub fn resolve_member(&self, member_name: &str) -> Option<&User> {
        self.resolve_member_index(member_name).map(|i| &self.members[i])
    }

    pub fn resolve_members(&self, member_names: Vec<String>) -> Vec<Option<&User>> {
        member_names.iter().map(|mn| self.resolve_member(mn)).collect_vec()
    }
}

impl Chat {
    /// Unfortunately needed heler due to rust-protobuf code generation strategy.
    pub fn ds_uuid(&self) -> &PbUuid {
        self.ds_uuid.as_ref().unwrap()
    }

    fn name_or_unnamed(&self) -> String {
        self.name_option.clone().unwrap_or_else(|| UNNAMED.to_owned())
    }

    pub fn qualified_name(&self) -> String {
        format!("'{}' (#${})", self.name_or_unnamed(), self.id)
    }
}

impl User {
    pub fn pretty_name(&self) -> String {
        unimplemented!()
    }
}

pub struct RichText {}

impl RichText {
    pub fn make_plain(text: String) -> RichTextElement {
        RichTextElement {
            searchable_string: normalize_seachable_string(text.as_str()),
            val: Some(rich_text_element::Val::Plain(RtePlain { text })),
        }
    }

    pub fn make_bold(text: String) -> RichTextElement {
        RichTextElement {
            searchable_string: normalize_seachable_string(text.as_str()),
            val: Some(rich_text_element::Val::Bold(RteBold { text })),
        }
    }

    pub fn make_italic(text: String) -> RichTextElement {
        RichTextElement {
            searchable_string: normalize_seachable_string(text.as_str()),
            val: Some(rich_text_element::Val::Italic(RteItalic { text })),
        }
    }

    pub fn make_underline(text: String) -> RichTextElement {
        RichTextElement {
            searchable_string: normalize_seachable_string(text.as_str()),
            val: Some(rich_text_element::Val::Underline(RteUnderline { text })),
        }
    }

    pub fn make_strikethrough(text: String) -> RichTextElement {
        RichTextElement {
            searchable_string: normalize_seachable_string(text.as_str()),
            val: Some(rich_text_element::Val::Strikethrough(RteStrikethrough { text })),
        }
    }

    pub fn make_spoiler(text: String) -> RichTextElement {
        RichTextElement {
            searchable_string: normalize_seachable_string(text.as_str()),
            val: Some(rich_text_element::Val::Spoiler(RteSpoiler { text })),
        }
    }

    pub fn make_link(text_option: Option<String>, href: String, hidden: bool) -> RichTextElement {
        let text = text_option.as_deref().unwrap_or("");
        let searchable_string =
            if text == href.as_str() {
                href.clone()
            } else {
                format!("{} {}", text, href).trim().to_owned()
            };
        let searchable_string = normalize_seachable_string(searchable_string.as_str());

        RichTextElement {
            val: Some(rich_text_element::Val::Link(RteLink {
                text_option,
                href,
                hidden,
            })),
            searchable_string: searchable_string,
        }
    }

    pub fn make_prefmt_inline(text: String) -> RichTextElement {
        RichTextElement {
            searchable_string: normalize_seachable_string(text.as_str()),
            val: Some(rich_text_element::Val::PrefmtInline(RtePrefmtInline { text })),
        }
    }

    pub fn make_prefmt_block(text: String, language_option: Option<String>) -> RichTextElement {
        RichTextElement {
            searchable_string: normalize_seachable_string(text.as_str()),
            val: Some(rich_text_element::Val::PrefmtBlock(RtePrefmtBlock { text, language_option })),
        }
    }
}

fn normalize_seachable_string(s: &str) -> String {
    lazy_static! {
        // \p is unicode category
        // \p{Z} is any separator (including \u00A0 no-break space)
        // \p{Cf} is any invisible formatting character (including \u200B zero-width space)
        static ref NORMALIZE_REGEX: Regex = Regex::new(r"[\p{Z}\p{Cf}\n]+").unwrap();
    }
    NORMALIZE_REGEX.replace_all(s, " ").trim().to_owned()
}

pub fn make_searchable_string(components: &Vec<RichTextElement>, typed: &message::Typed) -> String {
    let joined_text: String =
        components.iter()
            .map(|rte| &rte.searchable_string)
            .filter(|s| !s.is_empty())
            .join(" ");

    let typed_component_text: Vec<String> = match typed {
        message::Typed::Regular(MessageRegular { content_option, .. }) => {
            match content_option {
                Some(Content { sealed_value_optional: Some(content::SealedValueOptional::Sticker(sticker)) }) =>
                    vec![&sticker.emoji_option].into_iter().flatten().cloned().collect_vec(),
                Some(Content { sealed_value_optional: Some(content::SealedValueOptional::File(file)) }) =>
                    vec![&file.performer_option].into_iter().flatten().cloned().collect_vec(),
                Some(Content { sealed_value_optional: Some(content::SealedValueOptional::Location(loc)) }) => {
                    let mut vec1 = vec![&loc.address_option, &loc.title_option].into_iter().flatten().collect_vec();
                    let mut vec2 = vec![&loc.lat_str, &loc.lon_str];
                    vec1.append(&mut vec2);
                    vec1.into_iter().cloned().collect_vec()
                }
                Some(Content { sealed_value_optional: Some(content::SealedValueOptional::Poll(poll)) }) =>
                    vec![poll.question.clone()],
                Some(Content { sealed_value_optional: Some(content::SealedValueOptional::SharedContact(contact)) }) =>
                    vec![&contact.first_name_option, &contact.last_name_option, &contact.phone_number_option]
                        .into_iter().flatten().cloned().collect_vec(),
                _ => {
                    // Text is enough.
                    vec![]
                }
            }
        }
        message::Typed::Service(MessageService { sealed_value_optional: Some(m) }) =>
            match m {
                SealedValueOptional::GroupCreate(m) => vec![vec![m.title.clone()], m.members.clone()].into_iter().flatten().collect_vec(),
                SealedValueOptional::GroupInviteMembers(m) => m.members.clone(),
                SealedValueOptional::GroupRemoveMembers(m) => m.members.clone(),
                SealedValueOptional::GroupMigrateFrom(m) => vec![m.title.clone()],
                SealedValueOptional::GroupCall(m) => m.members.clone(),
                _ => vec![],
            }
        _ => unreachable!()
    };

    vec![joined_text, typed_component_text.join(" ")].iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .join(" ")
        .trim()
        .to_owned()
}
