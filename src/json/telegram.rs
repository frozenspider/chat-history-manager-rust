use std::borrow::Cow;
use std::collections::HashSet;
use std::num::ParseIntError;
use std::ops::Deref;
use std::path::{Path, PathBuf};

use chrono::{Local, NaiveDate};
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use simd_json::{BorrowedValue, StaticNode, Value as JValue};
use simd_json::borrowed::{Object, Value};

use crate::{EmptyRes, error_to_string, InMemoryDb, Res};
use crate::protobuf::*;
use crate::protobuf::history::{Chat, ChatType, ChatWithMessages, Dataset, Message, MessageRegular, MessageService, RichTextElement, User, PbUuid};

use super::*;

mod parser_full;
mod parser_single;
#[cfg(test)]
#[path = "telegram_tests.rs"]
mod tests;

/// Starting with Telegram 2020-10, user IDs are shifted by this value
static USER_ID_SHIFT: Id = 0x100000000_i64;

/// Starting with Telegram 2021-05, personal chat IDs are un-shifted by this value
static PERSONAL_CHAT_ID_SHIFT: Id = 0x100000000_i64;

/// Starting with Telegram 2021-05, personal chat IDs are un-shifted by this value
static GROUP_CHAT_ID_SHIFT: Id = PERSONAL_CHAT_ID_SHIFT * 2;

#[derive(Default, Debug)]
pub struct Users {
    id_to_user: HashMap<Id, User, Hasher>,
    pretty_name_to_idless_users: Vec<(String, User)>,
}

impl Users {
    fn pretty_name(u: &User) -> String {
        String::from(format!(
            "{} {}",
            u.first_name_option.as_ref().map(|s| s.as_str()).unwrap_or(""),
            u.last_name_option.as_ref().map(|s| s.as_str()).unwrap_or(""),
        ).trim())
    }

    /// Consumes both users, creating a mega-user!
    fn merge(original: User, new: User) -> User {
        let (first_name_option, last_name_option) =
            match (original.last_name_option.is_some(), new.last_name_option.is_some()) {
                (true, _) => (original.first_name_option, original.last_name_option),
                (_, true) => (new.first_name_option, new.last_name_option),
                _ => (original.first_name_option.or(new.first_name_option),
                      original.last_name_option.or(new.last_name_option))
            };
        User {
            ds_uuid: original.ds_uuid.or(new.ds_uuid),
            id: if original.id == 0 { new.id } else { original.id },
            first_name_option,
            last_name_option,
            phone_number_option: original.phone_number_option.or(new.phone_number_option),
            username_option: original.username_option.or(new.username_option),
        }
    }

    fn insert(&mut self, user: User) {
        log::debug!("Inserting user {:?}", user);

        let pretty_name = Self::pretty_name(&user);

        let existing_pos = self.pretty_name_to_idless_users.iter()
            .position(|(u_pretty_name, u)| {
                let has_matching_name = match pretty_name.as_str() {
                    "" => None,
                    s => Some(s == u_pretty_name)
                };
                let has_matching_phone = match user.phone_number_option {
                    None => None,
                    ref some => Some(*some == u.phone_number_option)
                };
                match (has_matching_name, has_matching_phone) {
                    // One is matching and the other isn't.
                    (Some(true), Some(false)) | (Some(false), Some(true)) => false,

                    // Matching
                    (Some(true), _) | (_, Some(true)) => true,

                    _ => false,
                }
            });
        let existing_user =
            existing_pos.map(|p| self.pretty_name_to_idless_users.remove(p).1);
        log::debug!("> Found user: {:?}", existing_user);
        let user = match existing_user {
            None => user,
            Some(eu) => {
                let user = Self::merge(eu, user);
                log::debug!("> Merged into {:?}", user);
                user
            }
        };
        let id = user.id;
        if id > 0 {
            log::debug!("> User has valid ID");
            self.id_to_user.insert(id, user);
        } else {
            log::debug!("> User has no ID!");
            self.pretty_name_to_idless_users.push((pretty_name, user));
        }
    }
}

enum ShouldProceed {
    Proceed,
    Skip,
}

#[derive(Clone)]
struct ExpectedMessageField<'lt> {
    required_fields: HashSet<&'lt str, Hasher>,
    optional_fields: HashSet<&'lt str, Hasher>,
}

pub fn parse_file(path: &Path, ds_uuid: &Uuid, myself_chooser: &dyn ChooseMyselfTrait) -> Res<InMemoryDb> {
    let path: PathBuf =
        if !path.ends_with("result.json") {
            path.join("result.json")
        } else {
            path.to_path_buf()
        };

    if !path.exists() {
        return Err(format!("{} not found!", path.to_str().unwrap()));
    }

    log::info!("Parsing '{}'", path.to_str().unwrap());

    let start_time = Instant::now();
    let ds_uuid = PbUuid { value: ds_uuid.to_string().to_lowercase() };

    let mut file_content = fs::read(&path)
        .map_err(error_to_string)?;
    let parsed = simd_json::to_borrowed_value(&mut file_content)
        .map_err(error_to_string)?;

    log::info!("Parsed in {} ms", start_time.elapsed().as_millis());

    let start_time = Instant::now();
    let root_obj = as_object!(parsed, "root");

    let mut myself: User = Default::default();
    myself.ds_uuid = Some(ds_uuid.clone());

    let single_chat_keys = HashSet::from(["name", "type", "id", "messages"]);
    let keys = root_obj.keys().map(|s| s.deref()).collect::<HashSet<_>>();
    let (users, chats_with_messages) =
        if single_chat_keys.is_superset(&keys) {
            parser_single::parse(root_obj, &ds_uuid, &mut myself, myself_chooser)?
        } else {
            parser_full::parse(root_obj, &ds_uuid, &mut myself)?
        };

    log::info!("Processed in {} ms", start_time.elapsed().as_millis());

    let ds = Dataset {
        uuid: Some(ds_uuid.clone()),
        alias: String::new(), // Will be set by caller.
        source_type: String::new(), // Will be set by caller.
    };

    if !users.pretty_name_to_idless_users.is_empty() {
        log::warn!("Discarding users with no IDs:");
        for (_pretty_name, u) in users.pretty_name_to_idless_users {
            log::warn!("> {:?}", u);
        }
    }

    // Sanity check: every chat member is supposed to have an associated user.
    for cwm in &chats_with_messages {
        let chat = cwm.chat.as_ref().ok_or("Chat absent!")?;
        for member_id in &chat.member_ids {
            if !users.id_to_user.contains_key(member_id) {
                return Err(format!("No member with id={} found for chat with id={} '{}'",
                                   member_id, chat.id,
                                   chat.name_option.as_ref().unwrap_or(&UNNAMED.to_owned())));
            }
        }
    }

    let mut users = users.id_to_user.into_values().collect_vec();

    // Set myself to be a first member (not required by convention but to match existing behaviour).
    users.sort_by_key(|u| if u.id == myself.id { Id::MIN } else { u.id });

    Ok(InMemoryDb {
        dataset: ds,
        ds_root: path.parent().unwrap().to_path_buf(),
        myself: myself,
        users: users,
        cwm: chats_with_messages,
    })
}

/** Returns a partially filled user. */
fn parse_contact(json_path: &str, bw: &BorrowedValue) -> Res<User> {
    let mut user: User = Default::default();

    parse_bw_as_object(bw, json_path, action_map([
        ("date", consume()),
        ("date_unixtime", consume()),
        ("user_id", Box::new(|v: &BorrowedValue| {
            // In older (pre-2021-06) dumps, id field was present but was always 0.
            let id = as_i64!(v, json_path, "user_id");
            if id == 0 {
                Ok(())
            } else {
                Err("ID was an actual value and not zero!".to_owned())
            }
        })),
        ("first_name", Box::new(|v: &BorrowedValue| {
            user.first_name_option = as_string_option!(v, json_path, "first_name");
            Ok(())
        })),
        ("last_name", Box::new(|v: &BorrowedValue| {
            user.last_name_option = as_string_option!(v, json_path, "last_name");
            Ok(())
        })),
        ("phone_number", Box::new(|v: &BorrowedValue| {
            user.phone_number_option = as_string_option!(v, json_path, "phone_number");
            Ok(())
        })),
    ]))?;

    // Normalize user ID.
    if user.id >= USER_ID_SHIFT {
        user.id -= USER_ID_SHIFT;
    }

    Ok(user)
}

/// Returns None if the chat is skipped (e.g. is saved_messages).
fn parse_chat(json_path: &str,
              chat_json: &Object,
              ds_uuid: &PbUuid,
              myself_id_option: Option<&Id>,
              users: &mut Users) -> Res<Option<ChatWithMessages>> {
    let mut chat: Chat = Default::default();
    let mut messages: Vec<Message> = vec![];

    let skip_processing = Cell::from(false);

    let mut member_ids: HashSet<Id, Hasher> =
        HashSet::with_capacity_and_hasher(100, hasher());

    let json_path = format!("{json_path}.chat");
    // Name will not be present for saved messages
    let json_path = match get_field!(chat_json, json_path, "name") {
        Ok(name) => format!("{json_path}[{}]", name),
        Err(_) => format!("{json_path}[#{}]", get_field!(chat_json, json_path, "id")?)
    };


    parse_object(chat_json, &json_path, action_map([
        ("", consume()), // No idea how to get rid of it
        ("name", Box::new(|v: &BorrowedValue| {
            if v.value_type() != ValueType::Null {
                chat.name_option = as_string_option!(v, json_path, "name");
            }
            Ok(())
        })),
        ("type", Box::new(|v: &BorrowedValue| {
            let tpe = match as_str!(v, json_path, "type") {
                "personal_chat" => Ok(ChatType::Personal),
                "private_group" => Ok(ChatType::PrivateGroup),
                "private_supergroup" => Ok(ChatType::PrivateGroup),
                "saved_messages" | "private_channel" => {
                    skip_processing.set(true);
                    Ok(ChatType::Personal) // Doesn't matter
                }
                other => Err(format!("Unknown chat type: {}", other)),
            }?;
            chat.tpe = tpe as i32;
            Ok(())
        })),
        ("id", Box::new(|v: &BorrowedValue| {
            chat.id = as_i64!(v, json_path, "id");
            Ok(())
        })),
        ("messages", Box::new(|v: &BorrowedValue| {
            if skip_processing.get() { return Ok(()); }
            let path = format!("{json_path}.messages");
            let messages_json = as_array!(v, path);
            for v in messages_json {
                if let Some(message) = parse_message(&path, v, ds_uuid, users,
                                                     &mut member_ids)? {
                    messages.push(message);
                }
            }
            Ok(())
        })),
    ]))?;

    if skip_processing.get() {
        return Ok(None);
    }

    messages.sort_by_key(|m| (m.timestamp, m.internal_id));

    chat.msg_count = messages.len() as i32;

    // Undo the shifts introduced by Telegram 2021-05.
    match ChatType::from_i32(chat.tpe) {
        Some(ChatType::Personal) if chat.id < PERSONAL_CHAT_ID_SHIFT =>
            chat.id += PERSONAL_CHAT_ID_SHIFT,
        Some(ChatType::PrivateGroup) if chat.id < GROUP_CHAT_ID_SHIFT =>
            chat.id += GROUP_CHAT_ID_SHIFT,
        Some(_etc) =>
            { /* Don't change anything. */ }
        None =>
            return Err(format!("Chat type has no associated enum: {}", chat.tpe))
    }

    if let Some(myself_id) = myself_id_option {
        // Add myself as a first member (not required by convention but to match existing behaviour).
        member_ids.remove(myself_id);
    }
    let mut member_ids = member_ids.into_iter().collect_vec();
    member_ids.sort();
    if let Some(myself_id) = myself_id_option {
        member_ids.insert(0, myself_id.clone());
    }
    chat.member_ids = member_ids;

    Ok(Some(ChatWithMessages { chat: Some(chat), messages }))
}

//
// Parsing message
//

struct MessageJson<'lt> {
    json_path: String,
    val: &'lt Object<'lt>,
    expected_fields: Option<ExpectedMessageField<'lt>>,
}

impl<'lt> MessageJson<'lt> {
    fn unopt<T>(v: Res<Option<T>>, name: &str, val: &Object) -> Res<T> {
        match v? {
            None => Err(format!("message.{name} not found for message {:?}", val)),
            Some(v) => Ok(v),
        }
    }

    fn add_required(&mut self, name: &'lt str) {
        self.expected_fields.as_mut().map(|ef| ef.required_fields.insert(name));
    }

    fn add_optional(&mut self, name: &'lt str) {
        self.expected_fields.as_mut().map(|ef| ef.optional_fields.insert(name));
    }

    fn field_opt(&mut self, name: &'lt str) -> Res<Option<&BorrowedValue>> {
        self.add_optional(name);
        Ok(self.val.get(name))
    }

    fn field(&mut self, name: &'lt str) -> Res<&BorrowedValue> {
        self.add_required(name);
        Self::unopt(Ok(self.val.get(name)), name, self.val)
    }

    fn field_opt_i32(&mut self, name: &'lt str) -> Res<Option<i32>> {
        match self.field_opt(name)? {
            None => Ok(None),
            Some(v) => Ok(Some(as_i32!(v, self.json_path, name)))
        }
    }

    fn field_i32(&mut self, name: &'lt str) -> Res<i32> {
        Self::unopt(self.field_opt_i32(name), name, self.val)
    }

    fn field_opt_i64(&mut self, name: &'lt str) -> Res<Option<i64>> {
        match self.field_opt(name)? {
            None => Ok(None),
            Some(v) => Ok(Some(as_i64!(v, self.json_path, name)))
        }
    }

    fn field_i64(&mut self, name: &'lt str) -> Res<i64> {
        Self::unopt(self.field_opt_i64(name), name, self.val)
    }

    fn field_opt_str(&mut self, name: &'lt str) -> Res<Option<String>> {
        let json_path = format!("{}.{}", self.json_path, name);
        match self.field_opt(name)? {
            None => Ok(None),
            Some(v) if v.is_null() => Ok(None),
            Some(v) => Ok(Some(as_string!(v, json_path)))
        }
    }

    fn field_str(&mut self, name: &'lt str) -> Res<String> {
        Self::unopt(self.field_opt_str(name), name, self.val)
    }

    /// Retrieve a RELATIVE path!
    fn field_opt_path(&mut self, name: &'lt str) -> Res<Option<String>> {
        Ok(self.field_opt_str(name)?.and_then(|s| (match s.as_str() {
            "" => None,
            "(File not included. Change data exporting settings to download.)" => None,
            "(File exceeds maximum size. Change data exporting settings to download.)" => None,
            _ => Some(s)
        })))
    }
}

fn parse_message(json_path: &str,
                 bw: &BorrowedValue,
                 ds_uuid: &PbUuid,
                 users: &mut Users,
                 member_ids: &mut HashSet<Id, Hasher>) -> Res<Option<Message>> {
    use history::message::Typed;

    fn as_hash_set<'lt>(arr: &[&'lt str]) -> HashSet<&'lt str, Hasher> {
        let mut result = HashSet::with_capacity_and_hasher(100, hasher());
        result.extend(arr);
        result
    }
    lazy_static! {
        static ref REGULAR_MSG_FIELDS: ExpectedMessageField<'static> = ExpectedMessageField {
            required_fields: as_hash_set(&["id", "type", "date", "text", "from", "from_id"]),
            optional_fields: as_hash_set(&["date_unixtime", "text_entities", "forwarded_from", "via_bot"]),
        };

        static ref SERVICE_MSG_FIELDS: ExpectedMessageField<'static> = ExpectedMessageField {
            required_fields: as_hash_set(&["id", "type", "date", "text", "actor", "actor_id", "action"]),
            optional_fields: as_hash_set(&["date_unixtime", "text_entities", "edited"]),
        };
    }

    let mut message_json = MessageJson {
        json_path: format!("{json_path}.message[{}]", get_field!(bw, "message", "id")?),
        val: as_object!(bw, "message"),
        expected_fields: None,
    };

    let mut message: Message = Default::default();
    message.internal_id = -1;

    // Determine message type an parse short user from it.
    let mut short_user: ShortUser = ShortUser::default();
    let tpe = message_json.field_str("type")?;
    match tpe.as_str() {
        "message" => {
            message_json.expected_fields = Some(REGULAR_MSG_FIELDS.clone());

            let mut regular: MessageRegular = Default::default();
            parse_regular_message(&mut message_json, &mut regular)?;
            message.typed = Some(Typed::Regular(regular));

            short_user.id = parse_user_id(message_json.field("from_id")?)?;
            short_user.full_name_option = message_json.field_opt_str("from")?;
        }
        "service" => {
            message_json.expected_fields = Some(SERVICE_MSG_FIELDS.clone());

            let mut service: MessageService = Default::default();
            let proceed = parse_service_message(&mut message_json, &mut service)?;
            if matches!(proceed, ShouldProceed::Skip) {
                return Ok(None);
            }
            message.typed = Some(Typed::Service(service));

            short_user.id = parse_user_id(message_json.field("actor_id")?)?;
            short_user.full_name_option = message_json.field_opt_str("actor")?;
        }
        etc => return Err(format!("Unknown message type: {}", etc)),
    }

    // Normalize user ID.
    if short_user.id >= USER_ID_SHIFT {
        short_user.id -= USER_ID_SHIFT;
    }

    message.from_id = short_user.id;

    member_ids.insert(short_user.id);

    // Associate it with a real user, or create one if none found.
    append_user(short_user, users, ds_uuid)?;

    let has_unixtime = message_json.val.get("date_unixtime").is_some();
    let has_text_entities = message_json.val.get("text_entities").is_some();

    for (k, v) in message_json.val.iter() {
        let kr = k.as_ref();
        if let Some(ref mut ef) = message_json.expected_fields {
            if !ef.required_fields.remove(kr) &&
                !ef.optional_fields.remove(kr) {
                return Err(format!("Unexpected message field '{kr}' for {:?}", message));
            }
        }

        match kr {
            "id" =>
                message.source_id_option = Some(as_i64!(v, message_json.json_path, "id")),
            "date_unixtime" => {
                message.timestamp = parse_timestamp(as_str!(v, message_json.json_path, "date_unixtime"))?;
            }
            "date" if !has_unixtime => {
                message.timestamp = parse_datetime(as_str!(v, message_json.json_path, "date"))?;
            }
            "text_entities" => {
                message.text = parse_rich_text(&format!("{}.text_entities", message_json.json_path), v)?;
            }
            "text" if !has_text_entities => {
                message.text = parse_rich_text(&format!("{}.text", message_json.json_path), v)?;
            }
            _ => { /* Ignore, already consumed */ }
        }
    }

    if let Some(ref ef) = message_json.expected_fields {
        if !ef.required_fields.is_empty() {
            return Err(format!("Message fields not found: {:?}", ef.required_fields));
        }
    }

    Ok(Some(message))
}

fn parse_regular_message(message_json: &mut MessageJson,
                         regular_msg: &mut MessageRegular) -> EmptyRes {
    use history::*;
    use history::content::SealedValueOptional;

    let json_path = message_json.json_path.clone();

    if let Some(ref edited) = message_json.field_opt_str("edited_unixtime")? {
        message_json.add_required("edited");
        regular_msg.edit_timestamp_option = Some(parse_timestamp(&edited)?);
    } else if let Some(ref edited) = message_json.field_opt_str("edited")? {
        regular_msg.edit_timestamp_option = Some(parse_datetime(&edited)?);
    }
    regular_msg.forward_from_name_option = match message_json.field_opt("forwarded_from")? {
        None => None,
        Some(forwarded_from) if forwarded_from.is_null() => Some(UNKNOWN.to_owned()),
        Some(forwarded_from) => Some(as_string!(forwarded_from, json_path, "forwarded_from")),
    };
    regular_msg.reply_to_message_id_option = message_json.field_opt_i64("reply_to_message_id")?;

    let media_type_option = message_json.field_opt_str("media_type")?;
    let photo_option = message_json.field_opt_str("photo")?;
    let file_present = message_json.field_opt_str("file")?.is_some();
    let loc_present = message_json.field_opt("location_information")?.is_some();
    let poll_question_present = match message_json.field_opt("poll")? {
        None => false,
        Some(poll) => as_object!(poll, json_path, "poll").get("question").is_some(),
    };
    let contact_info_present = message_json.field_opt("contact_information")?.is_some();
    let content_val: Option<SealedValueOptional> = match (media_type_option.as_deref(),
                                                          photo_option.as_deref(),
                                                          file_present,
                                                          loc_present,
                                                          poll_question_present,
                                                          contact_info_present) {
        (None, None, false, false, false, false) => None,
        (Some("sticker"), None, true, false, false, false) => {
            // Ignoring animated sticker duration
            message_json.add_optional("duration_seconds");
            Some(SealedValueOptional::Sticker(ContentSticker {
                path_option: message_json.field_opt_path("file")?,
                width: message_json.field_i32("width")?,
                height: message_json.field_i32("height")?,
                thumbnail_path_option: message_json.field_opt_path("thumbnail")?,
                emoji_option: message_json.field_opt_str("sticker_emoji")?,
            }))
        }
        (Some("animation"), None, true, false, false, false) =>
            Some(SealedValueOptional::Animation(ContentAnimation {
                path_option: message_json.field_opt_path("file")?,
                width: message_json.field_i32("width")?,
                height: message_json.field_i32("height")?,
                mime_type: message_json.field_str("mime_type")?,
                duration_sec_option: message_json.field_opt_i32("duration_seconds")?,
                thumbnail_path_option: message_json.field_opt_path("thumbnail")?,
            })),
        (Some("video_message"), None, true, false, false, false) =>
            Some(SealedValueOptional::VideoMsg(ContentVideoMsg {
                path_option: message_json.field_opt_path("file")?,
                width: message_json.field_i32("width")?,
                height: message_json.field_i32("height")?,
                mime_type: message_json.field_str("mime_type")?,
                duration_sec_option: message_json.field_opt_i32("duration_seconds")?,
                thumbnail_path_option: message_json.field_opt_path("thumbnail")?,
            })),
        (Some("voice_message"), None, true, false, false, false) =>
            Some(SealedValueOptional::VoiceMsg(ContentVoiceMsg {
                path_option: message_json.field_opt_path("file")?,
                mime_type: message_json.field_str("mime_type")?,
                duration_sec_option: message_json.field_opt_i32("duration_seconds")?,
            })),
        (Some("video_file"), None, true, false, false, false) |
        (Some("audio_file"), None, true, false, false, false) |
        (None, None, true, false, false, false) => {
            let title = message_json.field_opt_str("title")?.unwrap_or_else(|| {
                (match media_type_option.as_deref() {
                    None => "<File>",
                    Some("video_file") => "<Video>",
                    Some("audio_file") => "<Audio>",
                    Some(_) => unimplemented!("Unreachable code")
                }).to_owned()
            });
            Some(SealedValueOptional::File(ContentFile {
                path_option: message_json.field_opt_path("file")?,
                title,
                width_option: message_json.field_opt_i32("width")?,
                height_option: message_json.field_opt_i32("height")?,
                mime_type_option: message_json.field_opt_str("mime_type")?,
                duration_sec_option: message_json.field_opt_i32("duration_seconds")?,
                thumbnail_path_option: message_json.field_opt_path("thumbnail")?,
                performer_option: message_json.field_opt_str("performer")?,
            }))
        }
        (None, Some(_), false, false, false, false) =>
            Some(SealedValueOptional::Photo(ContentPhoto {
                path_option: message_json.field_opt_path("photo")?,
                width: message_json.field_i32("width")?,
                height: message_json.field_i32("height")?,
            })),
        (None, None, false, true, false, false) => {
            let (lat_str, lon_str) = {
                let loc_info =
                    as_object!(message_json.field("location_information")?, json_path, "location_information");
                (loc_info.get("latitude").ok_or("latitude not found!")?.to_string(),
                 loc_info.get("longitude").ok_or("longitude not found!")?.to_string())
            };
            Some(SealedValueOptional::Location(ContentLocation {
                title_option: message_json.field_opt_str("place_name")?,
                address_option: message_json.field_opt_str("address")?,
                lat_str,
                lon_str,
                duration_sec_option: message_json.field_opt_i32("live_location_period_seconds")?,
            }))
        }
        (None, None, false, false, true, false) => {
            let question = {
                let poll_info = as_object!(message_json.field("poll")?, json_path, "poll");
                get_field_string!(poll_info, json_path, "question")
            };
            Some(SealedValueOptional::Poll(ContentPoll { question }))
        }
        (None, None, false, false, false, true) => {
            let (
                first_name_option,
                last_name_option,
                phone_number_option,
                vcard_path_option
            ) = {
                let contact_info =
                    as_object!(message_json.field("contact_information")?, json_path, "contact_information");

                (get_field_string_option!(contact_info, json_path, "first_name"),
                 get_field_string_option!(contact_info, json_path, "last_name"),
                 get_field_string_option!(contact_info, json_path, "phone_number"),
                 message_json.field_opt_path("contact_vcard")?)
            };
            if first_name_option.is_none() && last_name_option.is_none() &&
                phone_number_option.is_none() && vcard_path_option.is_none()
            {
                return Err("Shared contact had no information whatsoever!".to_owned());
            }
            Some(SealedValueOptional::SharedContact(ContentSharedContact {
                first_name_option,
                last_name_option,
                phone_number_option,
                vcard_path_option,
            }))
        }
        _ => return Err(format!("Couldn't determine content type for '{:?}'", message_json.val))
    };

    regular_msg.content_option = content_val.map(|v| Content { sealed_value_optional: Some(v) });
    Ok(())
}

fn parse_service_message(message_json: &mut MessageJson,
                         service_msg: &mut MessageService) -> Res<ShouldProceed> {
    use history::*;
    use history::message_service::SealedValueOptional;

    // Null members are added as unknown
    fn parse_members(message_json: &mut MessageJson) -> Res<Vec<String>> {
        let json_path = format!("{}.members", message_json.json_path);
        message_json.field("members")?
            .try_as_array().map_err(error_to_string)?
            .into_iter()
            .map(|v|
                if v.value_type() != ValueType::Null {
                    as_string_res!(v, json_path)
                } else {
                    Ok(UNKNOWN.to_owned())
                }
            )
            .collect::<Res<Vec<String>>>()
    }

    let val: SealedValueOptional = match message_json.field_str("action")?.as_str() {
        "phone_call" =>
            SealedValueOptional::PhoneCall(MessageServicePhoneCall {
                duration_sec_option: message_json.field_opt_i32("duration_seconds")?,
                discard_reason_option: message_json.field_opt_str("discard_reason")?,
            }),
        "group_call" => // Treated the same as phone_call
            SealedValueOptional::PhoneCall(MessageServicePhoneCall {
                duration_sec_option: message_json.field_opt_i32("duration")?,
                discard_reason_option: None,
            }),
        "pin_message" =>
            SealedValueOptional::PinMessage(MessageServicePinMessage {
                message_id: message_json.field_i64("message_id")?
            }),
        "suggest_profile_photo" =>
            SealedValueOptional::SuggestProfilePhoto(MessageServiceSuggestProfilePhoto {
                photo: Some(ContentPhoto {
                    path_option: message_json.field_opt_path("photo")?,
                    height: message_json.field_i32("height")?,
                    width: message_json.field_i32("width")?,
                })
            }),
        "clear_history" =>
            SealedValueOptional::ClearHistory(MessageServiceClearHistory {}),
        "create_group" =>
            SealedValueOptional::GroupCreate(MessageServiceGroupCreate {
                title: message_json.field_str("title")?,
                members: parse_members(message_json)?,
            }),
        "edit_group_photo" =>
            SealedValueOptional::GroupEditPhoto(MessageServiceGroupEditPhoto {
                photo: Some(ContentPhoto {
                    path_option: message_json.field_opt_path("photo")?,
                    height: message_json.field_i32("height")?,
                    width: message_json.field_i32("width")?,
                })
            }),
        "delete_group_photo" =>
            SealedValueOptional::GroupDeletePhoto(MessageServiceGroupDeletePhoto {}),
        "edit_group_title" =>
            SealedValueOptional::GroupEditTitle(MessageServiceGroupEditTitle {
                title: message_json.field_str("title")?
            }),
        "invite_members" =>
            SealedValueOptional::GroupInviteMembers(MessageServiceGroupInviteMembers {
                members: parse_members(message_json)?
            }),
        "remove_members" =>
            SealedValueOptional::GroupRemoveMembers(MessageServiceGroupRemoveMembers {
                members: parse_members(message_json)?
            }),
        "join_group_by_link" => {
            message_json.add_required("inviter");
            SealedValueOptional::GroupInviteMembers(MessageServiceGroupInviteMembers {
                members: vec![message_json.field_str("actor")?]
            })
        }
        "migrate_from_group" =>
            SealedValueOptional::GroupMigrateFrom(MessageServiceGroupMigrateFrom {
                title: message_json.field_str("title")?
            }),
        "migrate_to_supergroup" =>
            SealedValueOptional::GroupMigrateTo(MessageServiceGroupMigrateTo {}),
        "invite_to_group_call" =>
            SealedValueOptional::GroupCall(MessageServiceGroupCall {
                members: parse_members(message_json)?
            }),
        "edit_chat_theme" => {
            // Not really interesting to track.
            return Ok(ShouldProceed::Skip);
        }
        etc =>
            return Err(format!("Don't know how to parse service message for action '{etc}'")),
    };
    service_msg.sealed_value_optional = Some(val);
    Ok(ShouldProceed::Proceed)
}

//
// Rich Text
//

fn parse_rich_text(json_path: &str, rt_json: &Value) -> Res<Vec<RichTextElement>> {
    use history::*;
    use history::rich_text_element::Val;

    fn parse_plain_option(s: &Cow<str>) -> Option<Val> {
        if s.is_empty() {
            None
        } else {
            Some(Val::Plain(RtePlain { text: s.deref().to_owned() }))
        }
    }

    fn wrap_rte(val: Val) -> RichTextElement {
        RichTextElement { val: Some(val), searchable_string: None }
    }

    // Empty plain strings are discarded
    let mut rtes = match rt_json {
        Value::Static(StaticNode::Null) =>
            Ok(vec![]),
        Value::String(s) => {
            let plain = parse_plain_option(s);
            if let Some(plain) = plain {
                Ok(vec![wrap_rte(plain)])
            } else {
                Ok(vec![])
            }
        }
        Value::Array(arr) => {
            let mut result: Vec<RichTextElement> = vec![];
            for json_el in arr {
                let val: Option<Val> = match json_el {
                    Value::String(s) =>
                        parse_plain_option(s),
                    Value::Object(obj) =>
                        parse_rich_text_object(json_path, obj)?,
                    etc =>
                        return Err(format!("Don't know how to parse RichText element '{:?}'", etc))
                };
                if let Some(val) = val {
                    result.push(wrap_rte(val))
                }
            }
            Ok(result)
        }
        etc =>
            Err(format!("Don't know how to parse RichText container '{:?}'", etc))
    }?;

    // Concatenate consecutive plaintext elements
    let mut i = 0;
    while (i + 1) < rtes.len() {
        let el1 = &rtes[i];
        let el2 = &rtes[i + 1];
        if let (Some(Val::Plain(plain1)), Some(Val::Plain(plain2))) = (&el1.val, &el2.val) {
            let mut new_text = String::new();
            new_text.push_str(&plain1.text);
            new_text.push_str(&plain2.text);
            let new_plain = Val::Plain(RtePlain { text: new_text });
            rtes.splice(i..=(i + 1), vec![wrap_rte(new_plain)]);
        } else {
            i += 1;
        }
    }

    Ok(rtes)
}

fn parse_rich_text_object(json_path: &str,
                          rte_json: &Box<Object>) -> Res<Option<history::rich_text_element::Val>> {
    use history::*;
    use history::rich_text_element::Val;

    let keys =
        rte_json.keys().map(|s| s.deref()).collect::<HashSet<&str, Hasher>>();
    macro_rules! check_keys {
        ($keys:expr) => {
            if keys != HashSet::<&str, Hasher>::from_iter($keys) {
                return Err(format!("Unexpected keys: {:?}", keys))
            }
        };
    }

    macro_rules! str_to_option {
        ($s:expr) => {
            match $s {
                "" => None,
                etc => Some(etc.to_owned())
            }
        };
    }

    let res: Option<Val> = match get_field_str!(rte_json, json_path, "type") {
        "plain" => {
            check_keys!(["type", "text"]);
            // Empty plain string is discarded
            get_field_string_option!(rte_json, json_path, "text")
                .map(|s| Val::Plain(RtePlain { text: s }))
        }
        "bold" => {
            check_keys!(["type", "text"]);
            Some(Val::Bold(RteBold { text: get_field_string!(rte_json, json_path, "text") }))
        }
        "italic" => {
            check_keys!(["type", "text"]);
            Some(Val::Italic(RteItalic { text: get_field_string!(rte_json, json_path, "text") }))
        }
        "underline" => {
            check_keys!(["type", "text"]);
            Some(Val::Underline(RteUnderline { text: get_field_string!(rte_json, json_path, "text") }))
        }
        "strikethrough" => {
            check_keys!(["type", "text"]);
            Some(Val::Strikethrough(RteStrikethrough { text: get_field_string!(rte_json, json_path, "text") }))
        }
        "spoiler" => {
            check_keys!(["type", "text"]);
            Some(Val::Spoiler(RteSpoiler { text: get_field_string!(rte_json, json_path, "text") }))
        }
        "unknown" => {
            // Unknown is rendered as plaintext in telegram
            check_keys!(["type", "text"]);
            Some(Val::Plain(RtePlain { text: get_field_string!(rte_json, json_path, "text") }))
        }
        "code" => {
            check_keys!(["type", "text"]);
            Some(Val::PrefmtInline(RtePrefmtInline { text: get_field_string!(rte_json, json_path, "text") }))
        }
        "pre" => {
            check_keys!(["type", "text", "language"]);
            Some(Val::PrefmtBlock(RtePrefmtBlock {
                text: get_field_string!(rte_json, json_path, "text"),
                language_option: get_field_string_option!(rte_json, json_path, "language"),
            }))
        }
        "text_link" => {
            check_keys!(["type", "text", "href"]);
            let text = get_field_string!(rte_json, json_path, "text");
            Some(Val::Link(RteLink {
                text_option: str_to_option!(text.as_str()),
                href: get_field_string!(rte_json, json_path, "href"),
                hidden: is_whitespace_or_invisible(&text),
            }))
        }
        "link" => {
            // Link format is hyperlink alone
            check_keys!(["type", "text"]);
            Some(Val::Link(RteLink {
                text_option: get_field_string_option!(rte_json, json_path, "text"),
                href: get_field_string!(rte_json, json_path, "text"),
                hidden: false,
            }))
        }
        "mention_name" => {
            // No special treatment for mention_name, but prepent @
            check_keys!(["type", "text", "user_id"]);
            Some(Val::Plain(RtePlain { text: format!("@{}", get_field_str!(rte_json, json_path, "text")) }))
        }
        "email" | "mention" | "phone" | "hashtag" | "bot_command" | "bank_card" | "cashtag" => {
            // No special treatment for any of these
            check_keys!(["type", "text"]);
            Some(Val::Plain(RtePlain { text: get_field_string!(rte_json, json_path, "text") }))
        }
        "custom_emoji" => {
            // Just taken as a regular emoji
            check_keys!(["type", "text", "document_id"]);
            Some(Val::Plain(RtePlain { text: get_field_string!(rte_json, json_path, "text") }))
        }
        etc =>
            return Err(format!("Don't know how to parse RichText element of type '{etc}' for {:?}", rte_json))
    };
    Ok(res)
}

//
// Other
//

fn append_user(short_user: ShortUser,
               users: &mut Users,
               ds_uuid: &PbUuid) -> Res<Id> {
    if short_user.id == 0 || short_user.id == -1 {
        Err(format!("Incorrect ID for a user!"))
    } else if let Some(user) = users.id_to_user.get(&short_user.id) {
        Ok(user.id)
    } else {
        let user = short_user.to_user(ds_uuid);
        let id = user.id;
        users.insert(user);
        Ok(id)
    }
}

fn parse_user_id(bw: &BorrowedValue) -> Res<Id> {
    let err_msg = format!("Don't know how to get user ID from '{}'", bw.to_string());
    let parse_str = |s: &str| -> Res<Id> {
        match s {
            s if s.starts_with("user") => s[4..].parse::<Id>().map_err(|_| err_msg.clone()),
            s if s.starts_with("channel") => s[7..].parse::<Id>().map_err(|_| err_msg.clone()),
            _ => Err(err_msg.clone())
        }
    };
    match bw {
        Value::Static(StaticNode::I64(i)) => Ok(*i),
        Value::Static(StaticNode::U64(u)) => Ok(*u as Id),
        Value::String(Cow::Borrowed(s)) => parse_str(s),
        Value::String(Cow::Owned(s)) => parse_str(&s),
        _ => Err(err_msg)
    }
}

fn parse_timestamp(s: &str) -> Res<i64> {
    s.parse::<i64>().map_err(|e| format!("Failed to parse unit timestamp {s}: {e}"))
}

fn parse_datetime(s: &str) -> Res<i64> {
    lazy_static! {
        static ref TZ: Local = Local::now().timezone();
    }
    // NaiveDateTime::parse_from_str is very slow! So we're parsing by hand.
    // Otherwise, we would use const DATE_TIME_FMT: &str = "%Y-%m-%dT%H:%M:%S";
    let split =
        s.split(|c| c == '-' || c == ':' || c == 'T')
            .map(|s| s.parse::<u32>())
            .collect::<Result<Vec<u32>, ParseIntError>>()
            .map_err(|e| format!("Failed to parse date {s}: {e}"))?;
    let date =
        NaiveDate::from_ymd_opt(split[0] as i32, split[1], split[2]).unwrap()
            .and_hms_opt(split[3], split[4], split[5]).unwrap()
            .and_local_timezone(TZ.clone())
            .single()
            .ok_or(format!("failed to parse date {}: ambiguous?", s))?;
    Ok(date.timestamp())
}

// Accounts for invisible formatting indicator, e.g. zero-width space \u200B
fn is_whitespace_or_invisible(s: &str) -> bool {
    lazy_static! {
        static ref IS_WHITESPACE_OR_INVISIBLE: Regex = Regex::new(r"^[\s\p{Cf}]*$").unwrap();
    }
    IS_WHITESPACE_OR_INVISIBLE.is_match(s)
}
