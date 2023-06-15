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

use crate::{EmptyRes, InMemoryDb, Res};
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
    id_to_user: HashMap<Id, User>,
    pretty_name_to_id: HashMap<String, Id>,
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
        println!("Inserting user {:?}", user);

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
        println!("> Found user: {:?}", existing_user);
        let user = match existing_user {
            None => user,
            Some(eu) => Self::merge(eu, user),
        };
        println!("> Merged into {:?}", user);
        let id = user.id;
        if id > 0 {
            println!("> User has valid ID");
            self.id_to_user.insert(id, user);
            self.pretty_name_to_id.insert(pretty_name, id);
        } else {
            println!("> User has no ID!");
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
    required_fields: HashSet<&'lt str>,
    optional_fields: HashSet<&'lt str>,
}

pub fn parse_file(path: &Path, ds_uuid: &Uuid, myself_chooser: MyselfChooser) -> Res<InMemoryDb> {
    let path: PathBuf =
        if !path.ends_with("result.json") {
            path.join("result.json")
        } else {
            path.to_path_buf()
        };

    if !path.exists() {
        return Err(format!("{} not found!", path.to_str().unwrap()))
    }

    println!("Parsing '{}'", path.to_str().unwrap());

    let start_time = Instant::now();
    let ds_uuid = PbUuid { value: ds_uuid.to_string().to_lowercase() };

    let mut file_content = fs::read(&path)
        .map_err(|e| e.to_string())?;
    let parsed = simd_json::to_borrowed_value(&mut file_content)
        .map_err(|e| e.to_string())?;

    println!("Parsed in {} ms", start_time.elapsed().as_millis());

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

    println!("Processed in {} ms", start_time.elapsed().as_millis());

    let ds = Dataset {
        uuid: Some(ds_uuid.clone()),
        alias: String::new(), // Will be set by caller.
        source_type: String::new(), // Will be set by caller.
    };

    if !users.pretty_name_to_idless_users.is_empty() {
        println!("Discarding users with no IDs:");
        for (_pretty_name, u) in users.pretty_name_to_idless_users {
            println!("> {:?}", u);
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
fn parse_contact(bw: &BorrowedValue, name: &str) -> Res<User> {
    let mut user: User = Default::default();

    parse_bw_as_object(bw, name, ActionMap::from([
        ("date", consume()),
        ("user_id", Box::new(|v: &BorrowedValue| {
            // In older (pre-2021-06) dumps, id field was present but was always 0.
            let id = as_i64!(v, "id");
            if id == 0 {
                Ok(())
            } else {
                Err("ID was an actual value and not zero!".to_owned())
            }
        })),
        ("first_name", Box::new(|v: &BorrowedValue| {
            user.first_name_option = as_string_option!(v, "first_name");
            Ok(())
        })),
        ("last_name", Box::new(|v: &BorrowedValue| {
            user.last_name_option = as_string_option!(v, "last_name");
            Ok(())
        })),
        ("phone_number", Box::new(|v: &BorrowedValue| {
            user.phone_number_option = as_string_option!(v, "phone_number");
            Ok(())
        })),
    ]))?;

    // Normalize user ID.
    if user.id >= USER_ID_SHIFT {
        user.id -= USER_ID_SHIFT;
    }

    Ok(user)
}

fn parse_chat(chat_json: &Object,
              ds_uuid: &PbUuid,
              myself_id: &Id,
              users: &mut Users) -> Res<ChatWithMessages> {
    let mut chat: Chat = Default::default();
    let mut messages: Vec<Message> = vec![];

    let is_saved_messages = Cell::from(false);

    let mut member_ids: HashSet<Id> = HashSet::with_capacity(100);

    parse_object(chat_json, "chat", ActionMap::from([
        ("", consume()), // No idea how to get rid of it
        ("name", Box::new(|v: &BorrowedValue| {
            if v.value_type() != ValueType::Null {
                chat.name_option = as_string_option!(v, "chat.name");
            }
            Ok(())
        })),
        ("type", Box::new(|v: &BorrowedValue| {
            let tpe = match as_str!(v, "chat.type") {
                "personal_chat" => Ok(ChatType::Personal),
                "private_group" => Ok(ChatType::PrivateGroup),
                "private_supergroup" => Ok(ChatType::PrivateGroup),
                "saved_messages" => {
                    is_saved_messages.set(true);
                    Ok(ChatType::Personal) // Doesn't matter
                }
                other => Err(format!("Unknown chat type: {}", other)),
            }?;
            chat.tpe = tpe as i32;
            Ok(())
        })),
        ("id", Box::new(|v: &BorrowedValue| {
            chat.id = as_i64!(v, "chat.id");
            Ok(())
        })),
        ("messages", Box::new(|v: &BorrowedValue| {
            if is_saved_messages.get() { return Ok(()); }
            let messages_json = as_array!(v, "messages");
            for v in messages_json {
                if let Some(message) = parse_message(v, ds_uuid, myself_id, users,
                                                     &mut member_ids)? {
                    messages.push(message);
                }
            }
            Ok(())
        })),
    ]))?;

    chat.msg_count = messages.len() as i32;

    // Undo the shifts introduced by Telegram 2021-05.
    match ChatType::from_i32(chat.tpe) {
        Some(ChatType::Personal) if chat.id < PERSONAL_CHAT_ID_SHIFT =>
            chat.id += PERSONAL_CHAT_ID_SHIFT,
        Some(ChatType::PrivateGroup) if chat.id < GROUP_CHAT_ID_SHIFT =>
            chat.id += GROUP_CHAT_ID_SHIFT,
        Some(_etc) =>
            { /* Don't change anything. */ },
        None =>
            return Err(format!("Chat type has no associated enum: {}", chat.tpe))
    }

    // Add myself as a first member (not required by convention but to match existing behaviour).
    member_ids.remove(myself_id);
    let mut member_ids = member_ids.into_iter().collect_vec();
    member_ids.sort();
    member_ids.insert(0, myself_id.clone());
    chat.member_ids = member_ids;

    Ok(ChatWithMessages { chat: Some(chat), messages })
}

//
// Parsing message
//

struct MessageJson<'lt> {
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
            Some(v) => Ok(Some(v.try_as_i32()
                .map_err(|e| format!("{} conversion: {:?}", name, e))?))
        }
    }

    fn field_i32(&mut self, name: &'lt str) -> Res<i32> {
        Self::unopt(self.field_opt_i32(name), name, self.val)
    }

    fn field_opt_i64(&mut self, name: &'lt str) -> Res<Option<i64>> {
        match self.field_opt(name)? {
            None => Ok(None),
            Some(v) => Ok(Some(v.try_as_i64()
                .map_err(|e| format!("{} conversion: {:?}", name, e))?))
        }
    }

    fn field_i64(&mut self, name: &'lt str) -> Res<i64> {
        Self::unopt(self.field_opt_i64(name), name, self.val)
    }

    fn field_opt_str(&mut self, name: &'lt str) -> Res<Option<String>> {
        match self.field_opt(name)? {
            None => Ok(None),
            Some(v) => Ok(Some(as_string!(v, name)))
        }
    }

    fn field_str(&mut self, name: &'lt str) -> Res<String> {
        Self::unopt(self.field_opt_str(name), name, self.val)
    }

    fn field_strs(&mut self, name: &'lt str) -> Res<Vec<String>> {
        self.field(name)?
            .try_as_array().map_err(|e| e.to_string())?
            .into_iter()
            .map(|v| as_string_res!(v, name))
            .collect::<Res<Vec<String>>>()
    }

    /// Retrieve a RELATIVE path!
    fn field_opt_path(&mut self, name: &'lt str) -> Res<Option<String>> {
        Ok(self.field_opt_str(name)?.and_then(|s| (match s.as_str() {
            "" => None,
            "(File not included. Change data exporting settings to download.)" => None,
            _ => Some(s)
        })))
    }
}

fn parse_message(bw: &BorrowedValue,
                 ds_uuid: &PbUuid,
                 myself_id: &Id,
                 users: &mut Users,
                 member_ids: &mut HashSet<Id>) -> Res<Option<Message>> {
    use history::message::Typed;

    fn as_hash_set<'lt>(arr: &[&'lt str]) -> HashSet<&'lt str> {
        let mut result = HashSet::with_capacity(100);
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
            optional_fields: as_hash_set(&["edited"]),
        };
    }

    let mut message_json = MessageJson {
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
            short_user.full_name_option = match message_json.field_opt("from")? {
                None => None,
                Some(from) if from.is_null() => None,
                Some(from) => Some(as_string!(from, "from")),
            };
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
            short_user.full_name_option = Some(message_json.field_str("actor")?);
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
    append_user(short_user, users, ds_uuid, myself_id)?;

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
                message.source_id_option = Some(as_i64!(v, "id")),
            "date_unixtime" => {
                message.timestamp = parse_timestamp(as_str!(v, "date_unixtime"))?;
            }
            "date" if !has_unixtime => {
                message.timestamp = parse_datetime(as_str!(v, "date"))?;
            }
            "text_entities" => {
                message.text = parse_rich_text(v)?;
            }
            "text" if !has_text_entities => {
                message.text = parse_rich_text(v)?;
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
    use history::content::Val;

    if let Some(ref edited) = message_json.field_opt_str("edited_unixtime")? {
        message_json.add_required("edited");
        regular_msg.edit_timestamp_option = Some(parse_timestamp(edited.as_str())?);
    } else if let Some(ref edited) = message_json.field_opt_str("edited")? {
        regular_msg.edit_timestamp_option = Some(parse_datetime(edited.as_str())?);
    }
    regular_msg.forward_from_name_option = match message_json.field_opt("forwarded_from")? {
        None => None,
        Some(forwarded_from) if forwarded_from.is_null() => Some("<unknown>".to_owned()),
        Some(forwarded_from) => Some(as_string!(forwarded_from, "forwarded_from")),
    };
    regular_msg.reply_to_message_id_option = message_json.field_opt_i64("reply_to_message_id")?;

    let media_type_option = message_json.field_opt_str("media_type")?;
    let photo_option = message_json.field_opt_str("photo")?;
    let file_present = message_json.field_opt_str("file")?.is_some();
    let loc_present = message_json.field_opt("location_information")?.is_some();
    let poll_question_present = match message_json.field_opt("poll")? {
        None => false,
        Some(poll) => as_object!(poll, "poll").get("question").is_some(),
    };
    let contact_info_present = message_json.field_opt("contact_information")?.is_some();
    let content_val: Option<Val> = match (media_type_option.as_deref(),
                                          photo_option.as_deref(),
                                          file_present,
                                          loc_present,
                                          poll_question_present,
                                          contact_info_present) {
        (None, None, false, false, false, false) => None,
        (Some("sticker"), None, true, false, false, false) =>
            Some(Val::Sticker(ContentSticker {
                path_option: message_json.field_opt_path("file")?,
                width: message_json.field_i32("width")?,
                height: message_json.field_i32("height")?,
                thumbnail_path_option: message_json.field_opt_path("thumbnail")?,
                emoji_option: message_json.field_opt_str("sticker_emoji")?,
            })),
        (Some("animation"), None, true, false, false, false) =>
            Some(Val::Animation(ContentAnimation {
                path_option: message_json.field_opt_path("file")?,
                width: message_json.field_i32("width")?,
                height: message_json.field_i32("height")?,
                mime_type: message_json.field_str("mime_type")?,
                duration_sec_option: message_json.field_opt_i32("duration_seconds")?,
                thumbnail_path_option: message_json.field_opt_path("thumbnail")?,
            })),
        (Some("video_message"), None, true, false, false, false) =>
            Some(Val::VideoMsg(ContentVideoMsg {
                path_option: message_json.field_opt_path("file")?,
                width: message_json.field_i32("width")?,
                height: message_json.field_i32("height")?,
                mime_type: message_json.field_str("mime_type")?,
                duration_sec_option: message_json.field_opt_i32("duration_seconds")?,
                thumbnail_path_option: message_json.field_opt_path("thumbnail")?,
            })),
        (Some("voice_message"), None, true, false, false, false) =>
            Some(Val::VoiceMsg(ContentVoiceMsg {
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
            Some(Val::File(ContentFile {
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
            Some(Val::Photo(ContentPhoto {
                path_option: message_json.field_opt_path("photo")?,
                width: message_json.field_i32("width")?,
                height: message_json.field_i32("height")?,
            })),
        (None, None, false, true, false, false) => {
            let (lat_str, lon_str) = {
                let loc_info =
                    as_object!(message_json.field("location_information") ?, "location_information");
                (loc_info.get("latitude").ok_or("latitude not found!")?.to_string(),
                 loc_info.get("longitude").ok_or("longitude not found!")?.to_string())
            };
            Some(Val::Location(ContentLocation {
                title_option: message_json.field_opt_str("place_name")?,
                address_option: message_json.field_opt_str("address")?,
                lat_str,
                lon_str,
                duration_sec_option: message_json.field_opt_i32("live_location_period_seconds")?,
            }))
        }
        (None, None, false, false, true, false) => {
            let question = {
                let poll_info = as_object!(message_json.field("poll") ?, "poll");
                get_field_str!(poll_info, "question").to_owned()
            };
            Some(Val::Poll(ContentPoll { question }))
        }
        (None, None, false, false, false, true) => {
            let (first_name, last_name, phone_number) = {
                let contact_info =
                    as_object!(message_json.field("contact_information") ?, "contact_information");

                (get_field_str!(contact_info, "first_name").to_owned(),
                 get_field_str!(contact_info, "last_name").to_owned(),
                 get_field_str!(contact_info, "phone_number").to_owned())
            };
            Some(Val::SharedContact(ContentSharedContact {
                first_name,
                last_name_option: Some(last_name),
                phone_number_option: Some(phone_number),
                vcard_path_option: message_json.field_opt_path("contact_vcard")?,
            }))
        }
        _ => return Err(format!("Couldn't determine content type for '{:?}'", message_json.val))
    };

    regular_msg.content_option = content_val.map(|v| Content { val: Some(v) });
    Ok(())
}

fn parse_service_message(message_json: &mut MessageJson,
                         service_msg: &mut MessageService) -> Res<ShouldProceed> {
    use history::*;
    use history::message_service::Val;

    let val: Val = match message_json.field_str("action")?.as_str() {
        "phone_call" =>
            Val::PhoneCall(MessageServicePhoneCall {
                duration_sec_option: message_json.field_opt_i32("duration_seconds")?,
                discard_reason_option: message_json.field_opt_str("discard_reason")?,
            }),
        "group_call" => // Treated the same as phone_call
            Val::PhoneCall(MessageServicePhoneCall {
                duration_sec_option: None,
                discard_reason_option: None,
            }),
        "pin_message" =>
            Val::PinMessage(MessageServicePinMessage {
                message_id: message_json.field_i64("message_id")?
            }),
        "clear_history" =>
            Val::ClearHistory(MessageServiceClearHistory {}),
        "create_group" =>
            Val::GroupCreate(MessageServiceGroupCreate {
                title: message_json.field_str("title")?,
                members: message_json.field_strs("members")?,
            }),
        "edit_group_photo" =>
            Val::GroupEditPhoto(MessageServiceGroupEditPhoto {
                photo: Some(ContentPhoto {
                    path_option: message_json.field_opt_path("photo")?,
                    height: message_json.field_i32("height")?,
                    width: message_json.field_i32("width")?,
                })
            }),
        "edit_group_title" =>
            Val::GroupEditTitle(MessageServiceGroupEditTitle {
                title: message_json.field_str("title")?
            }),
        "invite_members" =>
            Val::GroupInviteMembers(MessageServiceGroupInviteMembers {
                members: message_json.field_strs("members")?
            }),
        "remove_members" =>
            Val::GroupRemoveMembers(MessageServiceGroupRemoveMembers {
                members: message_json.field_strs("members")?
            }),
        "join_group_by_link" => {
            message_json.add_required("inviter");
            Val::GroupInviteMembers(MessageServiceGroupInviteMembers {
                members: vec![message_json.field_str("actor")?]
            })
        }
        "migrate_from_group" =>
            Val::GroupMigrateFrom(MessageServiceGroupMigrateFrom {
                title: message_json.field_str("title")?
            }),
        "migrate_to_supergroup" =>
            Val::GroupMigrateTo(MessageServiceGroupMigrateTo {}),
        "invite_to_group_call" =>
            Val::GroupCall(MessageServiceGroupCall {
                members: message_json.field_strs("members")?
            }),
        "edit_chat_theme" => {
            // Not really interesting to track.
            return Ok(ShouldProceed::Skip);
        }
        etc =>
            return Err(format!("Don't know how to parse service message for action '{etc}'")),
    };
    service_msg.val = Some(val);
    Ok(ShouldProceed::Proceed)
}

//
// Rich Text
//

fn parse_rich_text(rt_json: &Value) -> Res<Vec<RichTextElement>> {
    use history::*;
    use history::rich_text_element::Val;

    match rt_json {
        Value::Static(StaticNode::Null) =>
            Ok(vec!()),
        Value::String(s) =>
            if s.is_empty() {
                Ok(vec!())
            } else {
                Ok(vec![RichTextElement {
                    val: Some(Val::Plain(RtePlain { text: s.deref().to_owned() })),
                    searchable_string: None,
                }])
            },
        Value::Array(arr) => {
            let mut result: Vec<RichTextElement> = vec!();
            for json_el in arr {
                let val: Val = match json_el {
                    Value::String(s) =>
                        Val::Plain(RtePlain { text: s.deref().to_owned() }),
                    Value::Object(obj) =>
                        parse_rich_text_object(obj)?,
                    etc =>
                        return Err(format!("Don't know how to parse RichText element '{:?}'", etc))
                };
                result.push(RichTextElement { val: Some(val), searchable_string: None })
            }
            Ok(result)
        }
        etc =>
            Err(format!("Don't know how to parse RichText container '{:?}'", etc))
    }
}

fn parse_rich_text_object(rte_json: &Box<Object>) -> Res<history::rich_text_element::Val> {
    use history::*;
    use history::rich_text_element::Val;

    let keys =
        rte_json.keys().map(|s| s.deref()).collect::<HashSet<&str>>();
    macro_rules! check_keys {
        ($keys:expr) => {
            if keys != HashSet::from($keys) {
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

    let res: Val = match get_field_str!(rte_json, "type") {
        "plain" => {
            check_keys!(["type", "text"]);
            Val::Plain(RtePlain { text: get_field_string!(rte_json, "text") })
        }
        "bold" => {
            check_keys!(["type", "text"]);
            Val::Bold(RteBold { text: get_field_string!(rte_json, "text") })
        }
        "italic" => {
            check_keys!(["type", "text"]);
            Val::Italic(RteItalic { text: get_field_string!(rte_json, "text") })
        }
        "underline" => {
            check_keys!(["type", "text"]);
            Val::Underline(RteUnderline { text: get_field_string!(rte_json, "text") })
        }
        "strikethrough" => {
            check_keys!(["type", "text"]);
            Val::Strikethrough(RteStrikethrough { text: get_field_string!(rte_json, "text") })
        }
        "unknown" => {
            // Unknown is rendered as plaintext in telegram
            check_keys!(["type", "text"]);
            Val::Plain(RtePlain { text: get_field_string!(rte_json, "text") })
        }
        "code" => {
            check_keys!(["type", "text"]);
            Val::PrefmtInline(RtePrefmtInline { text: get_field_string!(rte_json, "text") })
        }
        "pre" => {
            check_keys!(["type", "text", "language"]);
            Val::PrefmtBlock(RtePrefmtBlock {
                text: get_field_string!(rte_json, "text"),
                language_option: str_to_option!(get_field_str!(rte_json, "language")),
            })
        }
        "text_link" => {
            check_keys!(["type", "text", "href"]);
            let text = get_field_str!(rte_json, "text").to_owned();
            Val::Link(RteLink {
                text_option: str_to_option!(text.as_str()),
                href: get_field_string!(rte_json, "href"),
                hidden: is_whitespace_or_invisible(text.as_str()),
            })
        }
        "link" => {
            // Link format is hyperlink alone
            check_keys!(["type", "text"]);
            Val::Link(RteLink {
                text_option: str_to_option!(get_field_str!(rte_json, "text")),
                href: get_field_string!(rte_json, "text"),
                hidden: false,
            })
        }
        "mention_name" => {
            // No special treatment for mention_name, but prepent @
            check_keys!(["type", "text", "user_id"]);
            Val::Plain(RtePlain { text: format!("@{}", get_field_str!(rte_json, "text")) })
        }
        "email" | "mention" | "phone" | "hashtag" | "bot_command" | "bank_card" | "cashtag" => {
            // No special treatment for any of these
            check_keys!(["type", "text"]);
            Val::Plain(RtePlain { text: get_field_string!(rte_json, "text") })
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
               ds_uuid: &PbUuid,
               myself_id: &Id) -> Res<Id> {
    if short_user.id == 0 || short_user.id == -1 {
        Err(format!("Incorrect ID for a user!"))
    } else if *myself_id == short_user.id {
        Ok(myself_id.clone())
    } else if let Some(user) = users.id_to_user.get(&short_user.id) {
        Ok(user.id)
    } else {
        let su_full_name_option = short_user.full_name_option.as_ref();
        let found_id =
            if su_full_name_option.is_none() || su_full_name_option.unwrap().is_empty() {
                None
            } else {
                let su_full_name = su_full_name_option.unwrap().as_str();
                users.pretty_name_to_id.iter().find(|&(pn, _)| {
                    su_full_name.contains(pn)
                }).map(|(_, id)| *id)
            };
        match found_id {
            Some(id) => Ok(id),
            None => {
                let user = short_user.to_user(ds_uuid);
                let id = user.id;
                users.insert(user);
                Ok(id)
            }
        }
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
        Value::String(Cow::Owned(s)) => parse_str(s.as_str()),
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
