use std::fmt::{Display, Formatter};
use std::path::PathBuf;

use derive_deref::Deref;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;

use crate::protobuf::history::*;
use crate::protobuf::history::message_service::SealedValueOptional;

pub const UNNAMED: &str = "[unnamed]";
pub const UNKNOWN: &str = "[unknown]";

pub const NO_INTERNAL_ID: MessageInternalId = MessageInternalId(-1);

//
// Helper entities
//

#[derive(Deref)]
pub struct DatasetRoot(pub PathBuf);

#[derive(Deref, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct UserId(pub i64);

impl UserId {
    pub const MIN: UserId = UserId(i64::MIN);

    pub const INVALID: UserId = UserId(0);

    pub fn is_valid(&self) -> bool { self.0 > 0 }
}

#[derive(Deref)]
pub struct MessageSourceId(pub i64);

#[derive(Deref)]
pub struct MessageInternalId(pub i64);

#[derive(Deref)]
pub struct Timestamp(pub i64);

impl Timestamp {
    pub const MAX: Timestamp = Timestamp(i64::MAX);
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShortUser {
    pub id: UserId,
    pub full_name_option: Option<String>,
}

impl ShortUser {
    pub fn new(id: UserId, full_name_option: Option<String>) -> Self {
        Self { id, full_name_option }
    }

    #[allow(dead_code)]
    pub fn new_name_str(id: UserId, full_name: &str) -> Self {
        Self::new(id, Some(full_name.to_owned()))
    }

    pub fn default() -> Self {
        Self::new(UserId::INVALID, None)
    }

    pub fn to_user(&self, ds_uuid: &PbUuid) -> User {
        User {
            ds_uuid: Some(ds_uuid.clone()),
            id: *self.id,
            first_name_option: self.full_name_option.clone(),
            last_name_option: None,
            username_option: None,
            phone_number_option: None,
        }
    }
}

impl Display for ShortUser {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShortUser(id: {}, full_name: {:?})", *self.id, self.full_name_option)
    }
}

impl User {
    pub fn id(&self) -> UserId { UserId(self.id) }

    pub fn pretty_name(&self) -> String { unimplemented!() }
}

impl Dataset {
    pub fn uuid(&self) -> &PbUuid { self.uuid.as_ref().unwrap() }
}

pub struct ChatWithDetails {
    pub chat: Chat,
    pub last_msg_option: Option<Message>,
    /** First element MUST be myself, the rest should be in some fixed order. */
    pub members: Vec<User>,
}

impl ChatWithDetails {
    // pub fn ds_uuid(&self) -> &PbUuid {
    //     self.chat.ds_uuid.as_ref().unwrap()
    // }
    //
    // /** Used to resolve plaintext members */
    // pub fn resolve_member_index(&self, member_name: &str) -> Option<usize> {
    //     self.members.iter().position(|m| m.pretty_name() == member_name)
    // }
    //
    // /** Used to resolve plaintext members */
    // pub fn resolve_member(&self, member_name: &str) -> Option<&User> {
    //     self.resolve_member_index(member_name).map(|i| &self.members[i])
    // }
    //
    // pub fn resolve_members(&self, member_names: Vec<String>) -> Vec<Option<&User>> {
    //     member_names.iter().map(|mn| self.resolve_member(mn)).collect_vec()
    // }
}

impl Chat {
    /// Unfortunately needed heler due to rust-protobuf code generation strategy.
    pub fn ds_uuid(&self) -> &PbUuid {
        self.ds_uuid.as_ref().unwrap()
    }

    pub fn qualified_name(&self) -> String {
        format!("'{}' (#${})", name_or_unnamed(&self.name_option), self.id)
    }

    pub fn member_ids(&self) -> impl Iterator<Item=UserId> + '_ {
        self.member_ids.iter().map(|id| UserId(*id))
    }
}

impl Message {
    pub fn internal_id(&self) -> MessageInternalId { MessageInternalId(self.internal_id) }

    pub fn timestamp(&self) -> Timestamp { Timestamp(self.timestamp) }
}

pub struct RichText {}

impl RichText {
    #[cfg(test)]
    pub fn unwrap(rtes: &[RichTextElement]) -> Vec<&rich_text_element::Val> {
        rtes.iter().map(|rte| rte.val.as_ref().unwrap()).collect_vec()
    }

    #[cfg(test)]
    pub fn unwrap_copy(rtes: &[RichTextElement]) -> Vec<rich_text_element::Val> {
        Self::unwrap(rtes).into_iter().cloned().collect_vec()
    }

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
            searchable_string,
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

//
// Master/slave specific entities
//

#[derive(Deref, Copy, Clone, Debug, PartialEq, Eq)]
pub struct MasterInternalId(i64);

#[derive(Deref, Copy, Clone, Debug, PartialEq, Eq)]
pub struct SlaveInternalId(i64);

#[derive(Deref, Clone, Debug)]
pub struct MasterMessage(pub Message);

impl MasterMessage {
    fn typed_id(&self) -> MasterInternalId { MasterInternalId(self.0.internal_id) }
}

impl PartialEq for MasterMessage {
    fn eq(&self, other: &Self) -> bool {
        self.0.internal_id == other.0.internal_id &&
            self.0.source_id_option == other.0.source_id_option
    }
}

#[derive(Deref, Clone, Debug)]
pub struct SlaveMessage(pub Message);

impl SlaveMessage {
    fn typed_id(&self) -> SlaveInternalId { SlaveInternalId(self.0.internal_id) }
}


impl PartialEq for SlaveMessage {
    fn eq(&self, other: &Self) -> bool {
        self.0.internal_id == other.0.internal_id &&
            self.0.source_id_option == other.0.source_id_option
    }
}

//
// Helper functions
//

fn normalize_seachable_string(s: &str) -> String {
    lazy_static! {
        // \p is unicode category
        // \p{Z} is any separator (including \u00A0 no-break space)
        // \p{Cf} is any invisible formatting character (including \u200B zero-width space)
        static ref NORMALIZE_REGEX: Regex = Regex::new(r"[\p{Z}\p{Cf}\n]+").unwrap();
    }
    NORMALIZE_REGEX.replace_all(s, " ").trim().to_owned()
}

pub fn make_searchable_string(components: &[RichTextElement], typed: &message::Typed) -> String {
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

pub fn name_or_unnamed(name_option: &Option<String>) -> String {
    name_option.as_ref().cloned().unwrap_or(UNNAMED.to_owned())
}
