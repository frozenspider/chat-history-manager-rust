use std::borrow::Cow;
use std::collections::HashSet;
use std::num::ParseIntError;
use std::ops::Deref;

use chrono::{Local, NaiveDate};
use lazy_static::lazy_static;
use regex::Regex;
use simd_json::{BorrowedValue, StaticNode, Value as JValue};
use simd_json::borrowed::{Object, Value};
use uuid::Uuid;
use crate::{InMemoryDb, EmptyRes, Res};

use crate::proto::history;
use crate::proto::history::{Chat, ChatType, ChatWithMessages, Dataset, Message, MessageRegular, MessageService, RichTextElement, User, Uuid as PbUuid};

use super::*;

type Id = i64;

#[derive(Default, Debug)]
struct Users {
    id_to_user: HashMap<Id, User>,
    pretty_name_to_id: Vec<(String, Id)>,
}

impl Users {
    fn insert(&mut self, user: User, pretty_name: String) {
        let id = user.id;
        self.id_to_user.insert(id, user);
        self.pretty_name_to_id.push((pretty_name, id));
    }
}

enum ShouldProceed {
    Proceed,
    Skip,
}

#[derive(Debug)]
struct ShortUser {
    id: Id,
    full_name: Option<String>,
}

#[derive(Clone)]
struct ExpectedMessageField<'lt> {
    required_fields: HashSet<&'lt str>,
    optional_fields: HashSet<&'lt str>,
}

lazy_static! {
    static ref REGULAR_MSG_FIELDS: ExpectedMessageField<'static> = ExpectedMessageField {
        required_fields: HashSet::from(["id", "type", "date", "text", "from", "from_id"]),
        optional_fields: HashSet::from(["forwarded_from", "via_bot"]),
    };

    static ref SERVICE_MSG_FIELDS: ExpectedMessageField<'static> = ExpectedMessageField {
        required_fields: HashSet::from(["id", "type", "date", "text", "actor", "actor_id", "action"]),
        optional_fields: HashSet::from([]),
    };
}

fn add_user(users: &mut Users, user: User) {
    let pretty_name = String::from(
        format!(
            "{} {}",
            user.first_name.as_ref().map(|s| s.as_str()).unwrap_or(""),
            user.last_name.as_ref().map(|s| s.as_str()).unwrap_or(""),
        ).trim()
    );
    users.insert(user, pretty_name);
}

pub fn parse_file(path: &str, ds_uuid: &Uuid) -> Res<InMemoryDb> {
    let start_time = Instant::now();
    let ds_uuid = PbUuid { value: ds_uuid.to_string() };

    let mut file_content = fs::read(path)
        .map_err(|e| e.to_string())?;
    let parsed = simd_json::to_borrowed_value(&mut file_content)
        .map_err(|e| e.to_string())?;

    println!("Parsed in {} ms", start_time.elapsed().as_millis());

    let start_time = Instant::now();
    let root_obj = as_object!(parsed, "root");

    let mut myself: User = Default::default();
    myself.ds_uuid = Some(ds_uuid.clone());

    let mut users: Users = Default::default();

    let mut chats_with_messages: Vec<ChatWithMessages> = vec!();

    parse_object(root_obj, "root", ActionMap::from([
        ("about", consume()),
        ("profile_pictures", consume()),
        ("frequent_contacts", consume()),
        ("other_data", consume()),
        ("contacts", Box::new(|v: &BorrowedValue| {
            parse_bw_as_object(v, "personal_information", ActionMap::from([
                ("about", consume()),
                ("list", Box::new(|v: &BorrowedValue| {
                    for v in v.as_array().ok_or("contact list is not an array!")? {
                        let mut contact = parse_contact(v, "contact")?;
                        contact.ds_uuid = Some(ds_uuid.clone());
                        add_user(&mut users, contact);
                    }
                    Ok(())
                })),
            ]))?;
            Ok(())
        })),
        ("personal_information", Box::new(|v: &BorrowedValue| {
            parse_bw_as_object(v, "personal_information", ActionMap::from([
                ("about", consume()),
                ("user_id", Box::new(|v: &BorrowedValue| {
                    myself.id = as_i64!(v, "ID");
                    Ok(())
                })),
                ("first_name", Box::new(|v: &BorrowedValue| {
                    myself.first_name = Some(as_string!(v, "first_name"));
                    Ok(())
                })),
                ("last_name", Box::new(|v: &BorrowedValue| {
                    myself.last_name = Some(as_string!(v, "last_name"));
                    Ok(())
                })),
                ("username", Box::new(|v: &BorrowedValue| {
                    myself.username = Some(as_string!(v, "username"));
                    Ok(())
                })),
                ("phone_number", Box::new(|v: &BorrowedValue| {
                    myself.phone_number = Some(as_string!(v, "phone_number"));
                    Ok(())
                })),
                ("bio", consume()),
            ]))
        })),
        ("chats", consume() /* Cannot borrow users the second time here! */),
        ("left_chats", consume() /* Cannot borrow users the second time here! */),
    ]))?;

    add_user(&mut users, myself.clone());

    fn parse_chats_inner(section: &str,
                         chat_json: &Object,
                         ds_uuid: &PbUuid,
                         myself_id: &Id,
                         users: &mut Users,
                         chats_with_messages: &mut Vec<ChatWithMessages>) -> EmptyRes {
        let chats_arr = chat_json
            .get("list").ok_or("No chats list in dataset!")?
            .as_array().ok_or(format!("{section} list is not an array!"))?;

        for v in chats_arr {
            let mut cwm = parse_chat(v, &ds_uuid, myself_id, users)?;
            if let Some(ref mut c) = cwm.chat {
                c.ds_uuid = Some(ds_uuid.clone());
            }
            chats_with_messages.push(cwm);
        }

        Ok(())
    }

    match root_obj.get("chats") {
        Some(chats_json) => parse_chats_inner(
            "chats", as_object!(chats_json, "chats"),
            &ds_uuid, &myself.id, &mut users, &mut chats_with_messages,
        )?,
        None => return Err(String::from("No chats in dataset!")),
    }

    match root_obj.get("left_chats") {
        Some(chats_json) => parse_chats_inner(
            "left_chats", as_object!(chats_json, "left_chats"),
            &ds_uuid, &myself.id, &mut users, &mut chats_with_messages,
        )?,
        None => { /* NOOP, left_chats are optional */ }
    }

    println!("Processed in {} ms", start_time.elapsed().as_millis());

    let ds = Dataset {
        uuid: Some(ds_uuid.clone()),
        alias: String::new(), // Will be set by caller.
        source_type: String::new(), // Will be set by caller.
    };

    let mut users = users.id_to_user.into_values().collect::<Vec<User>>();
    users.sort_by_key(|u| u.id);

    Ok(InMemoryDb {
        dataset: ds,
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
        ("first_name", Box::new(|v: &BorrowedValue| {
            user.first_name = Some(as_string!(v, "first_name"));
            Ok(())
        })),
        ("last_name", Box::new(|v: &BorrowedValue| {
            user.last_name = Some(as_string!(v, "last_name"));
            Ok(())
        })),
        ("phone_number", Box::new(|v: &BorrowedValue| {
            user.phone_number = Some(as_string!(v, "phone_number"));
            Ok(())
        })),
    ]))?;

    Ok(user)
}

fn parse_chat(bw: &BorrowedValue,
              ds_uuid: &PbUuid,
              myself_id: &Id,
              users: &mut Users) -> Res<ChatWithMessages> {
    let mut chat: Chat = Default::default();
    let mut messages: Vec<Message> = vec!();

    let is_saved_messages = Cell::from(false);

    parse_bw_as_object(bw, "chat", ActionMap::from([
        ("", consume()), // No idea how to get rid of it
        ("name", Box::new(|v: &BorrowedValue| {
            if v.value_type() != ValueType::Null {
                chat.name = as_string!(v, "chat.name");
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
                if let Some(message) = parse_message(v, ds_uuid, myself_id, users)? {
                    messages.push(message);
                }
            }
            Ok(())
        })),
    ]))?;

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

    fn field_opt(&mut self, name: &'lt str) -> Res<Option<&BorrowedValue>> {
        self.expected_fields.as_mut().map(|ef| ef.optional_fields.insert(name));
        Ok(self.val.get(name))
    }

    fn field(&mut self, name: &'lt str) -> Res<&BorrowedValue> {
        self.expected_fields.as_mut().map(|ef| ef.required_fields.insert(name));
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

    fn field_opt_path(&mut self, name: &'lt str) -> Res<Option<String>> {
        Ok(self.field_opt_str(name)?.and_then(|s| (match s.as_str() {
            "" => None,
            "(File not included. Change data exporting settings to download.)" => None,
            _ => Some(s) // FIXME: Relative path!
        })))
    }
}

fn parse_message(bw: &BorrowedValue,
                 ds_uuid: &PbUuid,
                 myself_id: &Id,
                 users: &mut Users) -> Res<Option<Message>> {
    let mut message_json = MessageJson {
        val: as_object!(bw, "message"),
        expected_fields: None,
    };

    let mut message: Message = Default::default();
    message.internal_id = -1;

    // Determine message type an parse short user from it.
    let mut short_user: ShortUser = ShortUser { id: -1, full_name: None };
    let tpe = message_json.field_str("type")?;
    match tpe.as_str() {
        "message" => {
            message_json.expected_fields = Some(REGULAR_MSG_FIELDS.clone());

            let mut regular: MessageRegular = Default::default();
            parse_regular_message(&mut message_json, &mut regular)?;
            message.regular = Some(regular);

            short_user.id = parse_user_id(message_json.field("from_id")?)?;
            short_user.full_name = match message_json.field_opt("from")? {
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
            message.service = Some(service);

            short_user.id = parse_user_id(message_json.field("actor_id")?)?;
            short_user.full_name = Some(message_json.field_str("actor")?);
        }
        etc => return Err(format!("Unknown message type: {}", etc)),
    }

    append_user(short_user, users, ds_uuid, myself_id)?;

    // Associate it with a real user, or create one if none found.

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
                message.source_id = as_i64!(v, "id"),
            "date" => {
                message.timestamp = parse_datetime(as_str!(v, "date"))?;
            }
            "text" => {
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

    if let Some(ref edited) = message_json.field_opt_str("edited")? {
        regular_msg.edit_timestamp = Some(parse_datetime(edited.as_str())?);
    }
    regular_msg.forward_from_name = match message_json.field_opt("forwarded_from")? {
        None => None,
        Some(forwarded_from) if forwarded_from.is_null() => Some("<unknown>".to_owned()),
        Some(forwarded_from) => Some(as_string!(forwarded_from, "forwarded_from")),
    };
    regular_msg.reply_to_message_id = message_json.field_opt_i64("reply_to_message_id")?;

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
                path: message_json.field_opt_path("file")?,
                width: message_json.field_i32("width")?,
                height: message_json.field_i32("height")?,
                thumbnail_path: message_json.field_opt_path("thumbnail")?,
                emoji: message_json.field_opt_str("sticker_emoji")?,
            })),
        (Some("animation"), None, true, false, false, false) =>
            Some(Val::Animation(ContentAnimation {
                path: message_json.field_opt_path("file")?,
                width: message_json.field_i32("width")?,
                height: message_json.field_i32("height")?,
                mime_type: message_json.field_opt_str("mime_type")?,
                duration_sec: message_json.field_opt_i32("duration_seconds")?,
                thumbnail_path: message_json.field_opt_path("thumbnail")?,
            })),
        (Some("video_message"), None, true, false, false, false) =>
            Some(Val::VideoMsg(ContentVideoMsg {
                path: message_json.field_opt_path("file")?,
                width: message_json.field_i32("width")?,
                height: message_json.field_i32("height")?,
                mime_type: message_json.field_str("mime_type")?,
                duration_sec: message_json.field_opt_i32("duration_seconds")?,
                thumbnail_path: message_json.field_opt_path("thumbnail")?,
            })),
        (Some("voice_message"), None, true, false, false, false) =>
            Some(Val::VoiceMsg(ContentVoiceMsg {
                path: message_json.field_opt_path("file")?,
                mime_type: message_json.field_str("mime_type")?,
                duration_sec: message_json.field_opt_i32("duration_seconds")?,
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
                path: message_json.field_opt_path("file")?,
                title,
                width: message_json.field_opt_i32("width")?,
                height: message_json.field_opt_i32("height")?,
                mime_type: message_json.field_opt_str("mime_type")?,
                duration_sec: message_json.field_opt_i32("duration_seconds")?,
                thumbnail_path: message_json.field_opt_path("thumbnail")?,
                performer: message_json.field_opt_str("performer")?,
            }))
        }
        (None, Some(_), false, false, false, false) =>
            Some(Val::Photo(ContentPhoto {
                path: message_json.field_opt_path("photo")?,
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
                title: message_json.field_opt_str("place_name")?,
                address: message_json.field_opt_str("address")?,
                lat_str,
                lon_str,
                duration_sec: message_json.field_opt_i32("live_location_period_seconds")?,
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
                last_name,
                phone_number,
                vcard_path: message_json.field_opt_path("contact_vcard")?,
            }))
        }
        _ => return Err(format!("Couldn't determine content type for '{:?}'", message_json.val))
    };

    regular_msg.content = content_val.map(|v| Content { val: Some(v) });
    Ok(())
}

fn parse_service_message(message_json: &mut MessageJson,
                         service_msg: &mut MessageService) -> Res<ShouldProceed> {
    use history::*;
    use history::message_service::Val;

    let val: Val = match message_json.field_str("action")?.as_str() {
        "phone_call" =>
            Val::PhoneCall(MessageServicePhoneCall {
                duration_sec: message_json.field_opt_i32("duration_seconds")?,
                discard_reason: message_json.field_opt_str("discard_reason")?,
            }),
        "group_call" => // Treated the same as phone_call
            Val::PhoneCall(MessageServicePhoneCall {
                duration_sec: None,
                discard_reason: None,
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
                    path: message_json.field_opt_path("photo")?,
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
            if let Some(ref mut ef) = message_json.expected_fields {
                ef.required_fields.insert("inviter");
            }
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
                    val: Some(Val::Plain(RtePlain { text: s.deref().to_owned() }))
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
                result.push(RichTextElement { val: Some(val) })
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
                language: str_to_option!(get_field_str!(rte_json, "language")),
            })
        }
        "text_link" => {
            check_keys!(["type", "text", "href"]);
            let text = get_field_str!(rte_json, "text").to_owned();
            Val::Link(RteLink {
                text: str_to_option!(text.as_str()),
                href: get_field_string!(rte_json, "href"),
                hidden: is_whitespace_or_invisible(text.as_str()),
            })
        }
        "link" => {
            // Link format is hyperlink alone
            check_keys!(["type", "text"]);
            Val::Link(RteLink {
                text: str_to_option!(get_field_str!(rte_json, "text")),
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
        let su_full_name = short_user.full_name.as_ref();
        let found_id =
            if su_full_name.is_none() ||
                su_full_name.unwrap().is_empty() {
                None
            } else {
                let su_full_name = su_full_name.unwrap().as_str();
                users.pretty_name_to_id.iter().find(|&(pn, _)| {
                    su_full_name.contains(pn)
                }).map(|(_, id)| *id)
            };
        match found_id {
            Some(id) => Ok(id),
            None => {
                let user = User {
                    ds_uuid: Some(ds_uuid.clone()),
                    id: short_user.id,
                    first_name: short_user.full_name,
                    last_name: None,
                    username: None,
                    phone_number: None,
                };
                let id = user.id;
                add_user(users, user);
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
