use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};

use derive_deref::Deref;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;

use crate::protobuf::history::*;
use crate::protobuf::history::message::Typed;
use crate::protobuf::history::message_service::SealedValueOptional;

use super::*;

pub mod entity_equality;

pub const UNNAMED: &str = "[unnamed]";
pub const UNKNOWN: &str = "[unknown]";
pub const SOMEONE: &str = "[someone]";

pub const NO_INTERNAL_ID: MessageInternalId = MessageInternalId(-1);

//
// Helper entities
//

#[derive(Deref, Debug)]
pub struct DatasetRoot(pub PathBuf);

impl DatasetRoot {
    pub fn to_absolute(&self, path: &str) -> PathBuf {
        let path = Path::new(path);
        assert!(!path.is_absolute());
        self.0.join(path)
    }

    pub fn to_relative(&self, path: &Path) -> Result<String> {
        let ds_root = &self.0;
        assert!(ds_root.is_absolute());
        let path = path.canonicalize()?;
        let path = path.to_str().ok_or(anyhow!("Path is not a valid string!"))?;
        let ds_root = ds_root.to_str().ok_or(anyhow!("Dataset root is not a valid string!"))?;
        if !path.starts_with(ds_root) {
            bail!("Path {} is not under dataset root {}", path, ds_root);
        }
        Ok(path[(ds_root.len() + 1)..].to_owned())
    }
}

#[derive(Deref, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct UserId(pub i64);

impl UserId {
    pub const MIN: UserId = UserId(i64::MIN);

    pub const INVALID: UserId = UserId(0);

    pub fn is_valid(&self) -> bool { self.0 > 0 }
}

#[derive(Deref, Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageSourceId(pub i64);

#[derive(Deref, Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageInternalId(pub i64);

/// Number of epoch seconds
#[derive(Deref, Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Timestamp(pub i64);

impl Timestamp {
    pub const MIN: Timestamp = Timestamp(0);
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

impl Default for ShortUser {
    fn default() -> Self {
        Self::new(UserId::INVALID, None)
    }
}

impl Display for ShortUser {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShortUser(id: {}, full_name: {:?})", *self.id, self.full_name_option)
    }
}

impl User {
    pub fn id(&self) -> UserId { UserId(self.id) }

    pub fn pretty_name_option(&self) -> Option<String> {
        match (self.first_name_option.as_ref(), self.last_name_option.as_ref(), self.phone_number_option.as_ref()) {
            (Some(first_name), Some(last_name), _) => Some(format!("{first_name} {last_name}")),
            (Some(first_name), None, _) => Some(first_name.clone()),
            (None, Some(last_name), _) => Some(last_name.clone()),
            (None, None, Some(phone_number)) => Some(phone_number.clone()),
            (None, None, None) => None
        }
    }

    pub fn pretty_name(&self) -> String {
        self.pretty_name_option().unwrap_or(UNNAMED.to_owned())
    }
}

impl Dataset {
    pub fn uuid(&self) -> &PbUuid { self.uuid.as_ref().unwrap() }
}

#[derive(Debug)]
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

    pub fn resolve_members(&self, member_names: &[String]) -> Vec<Option<&User>> {
        member_names.iter().map(|mn| self.resolve_member(mn)).collect_vec()
    }
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

impl ChatType {
    pub fn resolve(tpe: i32) -> Result<ChatType> {
        ChatType::try_from(tpe).with_context(|| format!("Chat type {tpe} has no associated enum"))
    }
}

impl Message {
    pub fn new(internal_id: i64,
               source_id_option: Option<i64>,
               timestamp: i64,
               from_id: i64,
               text: Vec<RichTextElement>,
               typed: message::Typed) -> Self {
        let searchable_string = make_searchable_string(&text, &typed);
        Message {
            internal_id,
            source_id_option,
            timestamp,
            from_id,
            text,
            searchable_string,
            typed: Some(typed),
        }
    }

    pub fn internal_id(&self) -> MessageInternalId { MessageInternalId(self.internal_id) }

    // pub fn source_id_option(&self) -> Option<MessageSourceId> { self.source_id_option.map(MessageSourceId) }

    pub fn timestamp(&self) -> Timestamp { Timestamp(self.timestamp) }

    pub fn typed(&self) -> &message::Typed {
        self.typed.as_ref().expect("Invalid typed message")
    }

    pub fn typed_mut(&mut self) -> &mut message::Typed {
        self.typed.as_mut().expect("Invalid typed message")
    }

    pub fn files_relative(&self) -> Vec<&str> {
        let possibilities: Vec<Option<&str>> = match self.typed() {
            message::Typed::Regular(ref mr) => {
                use content::SealedValueOptional::*;
                match mr.content_option.as_ref().and_then(|c| c.sealed_value_optional.as_ref()) {
                    Some(Sticker(v)) => vec![v.path_option.as_deref(), v.thumbnail_path_option.as_deref()],
                    Some(Photo(v)) => vec![v.path_option.as_deref()],
                    Some(VoiceMsg(v)) => vec![v.path_option.as_deref()],
                    Some(Audio(v)) => vec![v.path_option.as_deref()],
                    Some(VideoMsg(v)) => vec![v.path_option.as_deref(), v.thumbnail_path_option.as_deref()],
                    Some(Video(v)) => vec![v.path_option.as_deref(), v.thumbnail_path_option.as_deref()],
                    Some(File(v)) => vec![v.path_option.as_deref(), v.thumbnail_path_option.as_deref()],
                    Some(Location(_)) => vec![],
                    Some(Poll(_)) => vec![],
                    Some(SharedContact(v)) => vec![v.vcard_path_option.as_deref()],
                    None => vec![],
                }
            }
            message::Typed::Service(MessageService { sealed_value_optional: ref ms }) => {
                use message_service::SealedValueOptional::*;
                match ms {
                    Some(PhoneCall(_)) => vec![],
                    Some(SuggestProfilePhoto(v)) => vec![v.photo.as_ref().and_then(|p| p.path_option.as_deref())],
                    Some(PinMessage(_)) => vec![],
                    Some(ClearHistory(_)) => vec![],
                    Some(BlockUser(_)) => vec![],
                    Some(GroupCreate(_)) => vec![],
                    Some(GroupEditTitle(_)) => vec![],
                    Some(GroupEditPhoto(v)) => vec![v.photo.as_ref().and_then(|p| p.path_option.as_deref())],
                    Some(GroupDeletePhoto(_)) => vec![],
                    Some(GroupInviteMembers(_)) => vec![],
                    Some(GroupRemoveMembers(_)) => vec![],
                    Some(GroupMigrateFrom(_)) => vec![],
                    Some(GroupMigrateTo(_)) => vec![],
                    Some(GroupCall(_)) => vec![],
                    None => unreachable!("Unexpected MessageService type: {:?}", ms)
                }
            }
        };
        possibilities.into_iter().flatten().collect()
    }

    pub fn files(&self, ds_root: &DatasetRoot) -> Vec<PathBuf> {
        self.files_relative().iter().map(|p| ds_root.to_absolute(p)).collect()
    }
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

impl Content {
    pub fn path_file_option(&self, ds_root: &DatasetRoot) -> Option<PathBuf> {
        use content::SealedValueOptional::*;
        match self.sealed_value_optional.as_ref() { // @formatter:off
            Some(Sticker(c))   => c.path_option.as_ref().map(|c| ds_root.to_absolute(c)),
            Some(Photo(c))     => c.path_option.as_ref().map(|c| ds_root.to_absolute(c)),
            Some(VoiceMsg(c))  => c.path_option.as_ref().map(|c| ds_root.to_absolute(c)),
            Some(Audio(c))     => c.path_option.as_ref().map(|c| ds_root.to_absolute(c)),
            Some(VideoMsg(c))  => c.path_option.as_ref().map(|c| ds_root.to_absolute(c)),
            Some(Video(c))     => c.path_option.as_ref().map(|c| ds_root.to_absolute(c)),
            Some(File(c))      => c.path_option.as_ref().map(|c| ds_root.to_absolute(c)),
            _ => None
        } // @formatter:on
    }
}

impl ContentLocation {
    pub fn lat(&self) -> Result<f64> { self.lat_str.parse::<f64>().map_err(|e| e.into()) }

    pub fn lon(&self) -> Result<f64> { self.lon_str.parse::<f64>().map_err(|e| e.into()) }
}

//
// Master/slave specific entities
//

#[derive(Deref, Copy, Clone, Debug, PartialEq, Eq)]
pub struct MasterInternalId(i64);

#[derive(Deref, Copy, Clone, Debug, PartialEq, Eq)]
pub struct SlaveInternalId(i64);

pub trait WithTypedId {
    type Item: Clone;
    fn typed_id(&self) -> Self::Item;
}

#[derive(Deref, Clone, Debug)]
pub struct MasterMessage(pub Message);

impl PartialEq for MasterMessage {
    fn eq(&self, other: &Self) -> bool {
        self.0.internal_id == other.0.internal_id &&
            self.0.source_id_option == other.0.source_id_option
    }
}

impl WithTypedId for MasterMessage {
    type Item = MasterInternalId;
    fn typed_id(&self) -> MasterInternalId { MasterInternalId(self.0.internal_id) }
}

#[derive(Deref, Clone, Debug)]
pub struct SlaveMessage(pub Message);

impl PartialEq for SlaveMessage {
    fn eq(&self, other: &Self) -> bool {
        self.0.internal_id == other.0.internal_id &&
            self.0.source_id_option == other.0.source_id_option
    }
}

impl WithTypedId for SlaveMessage {
    type Item = SlaveInternalId;
    fn typed_id(&self) -> SlaveInternalId { SlaveInternalId(self.0.internal_id) }
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
                Some(Content { sealed_value_optional: Some(content::SealedValueOptional::Audio(file)) }) =>
                    vec![&file.title_option, &file.performer_option].into_iter().flatten().cloned().collect_vec(),
                Some(Content { sealed_value_optional: Some(content::SealedValueOptional::Video(file)) }) =>
                    vec![&file.title_option, &file.performer_option].into_iter().flatten().cloned().collect_vec(),
                Some(Content { sealed_value_optional: Some(content::SealedValueOptional::File(file)) }) =>
                    vec![&file.file_name_option].into_iter().flatten().cloned().collect_vec(),
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
        message::Typed::Service(MessageService { sealed_value_optional: Some(m) }) => {
            use message_service::SealedValueOptional::*;
            match m {
                GroupCreate(m) => vec![vec![m.title.clone()], m.members.clone()].into_iter().flatten().collect_vec(),
                GroupInviteMembers(m) => m.members.clone(),
                GroupRemoveMembers(m) => m.members.clone(),
                GroupMigrateFrom(m) => vec![m.title.clone()],
                GroupCall(m) => m.members.clone(),
                _ => vec![],
            }
        }
        _ => unreachable!()
    };

    [joined_text, typed_component_text.join(" ")].iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .join(" ")
        .trim()
        .to_owned()
}

pub fn name_or_unnamed(name_option: &Option<String>) -> String {
    name_or_unnamed_str(name_option.as_deref())
}

pub fn name_or_unnamed_str(name_option: Option<&str>) -> String {
    name_option.unwrap_or(UNNAMED).to_owned()
}
