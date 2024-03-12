#![allow(clippy::reversed_empty_ranges)]

use std::{cmp, fmt, fs, mem, slice};
use std::borrow::Cow;
use std::fmt::Debug;
use std::path::PathBuf;

use encoding_rs::Encoding;
use itertools::Itertools;
use lazy_static::lazy_static;
use num_traits::FromPrimitive;
use regex::{Captures, Regex};
use utf16string::{LE, WStr};

use content::SealedValueOptional as ContentSvo;
use message_service::SealedValueOptional as ServiceSvo;

use crate::dao::in_memory_dao::{DatasetEntry, InMemoryDao};
use crate::loader::DataLoader;
use crate::prelude::*;

mod mra_dbs;
mod db;

pub struct MailRuAgentDataLoader;

type DatasetMap = HashMap<String, MraDatasetEntry>;
type TextAndTyped = (Vec<RichTextElement>, message::Typed);

const CONFERENCE_USER_JOINED: u32 = 0x03;
const CONFERENCE_USER_LEFT: u32 = 0x05;

const MRA_DBS: &str = "mra.dbs";

/// Using a first legal ID (i.e. "1") for myself
const MYSELF_ID: UserId = UserId(UserId::INVALID.0 + 1);

lazy_static! {
    static ref DB_FILE_DIRS: Vec<&'static str> = vec!["Agent", "ICQ"];

    // Expected entries are @mail.ru, @bk.ru, @inbox.ru and @uin.icq.
    // Could also be @chat.agent, which indicates a group chat.
    static ref EMAIL_REGEX: Regex = Regex::new(r"^[a-zA-Z0-9._-]+@([a-z-]+\.)+[a-z]+$").unwrap();

    static ref SMILE_TAG_REGEX: Regex = Regex::new(r"<SMILE>id=(?<id>[^ ]+)( alt='(?<alt>[^']+)')?</SMILE>").unwrap();
    static ref SMILE_INLINE_REGEX: Regex = Regex::new(r":(([А-ЯË][^:\n]+)|([0-9]{3,})):").unwrap();
    static ref SMILE_IMG_REGEX: Regex = Regex::new(r"<###(?<prefix>\d+)###img(?<id>\d+)>").unwrap();
}

impl DataLoader for MailRuAgentDataLoader {
    fn name(&self) -> &'static str { "Mail.Ru Agent" }

    fn src_alias(&self) -> &'static str { "MRA" }

    fn src_type(&self) -> SourceType { SourceType::Mra }

    fn looks_about_right_inner(&self, path: &Path) -> EmptyRes {
        if path_file_name(path)? != MRA_DBS {
            bail!("Given file is not {MRA_DBS}")
        }
        Ok(())
    }

    fn load_inner(&self, path: &Path, ds: Dataset, _myself_chooser: &dyn MyselfChooser) -> Result<Box<InMemoryDao>> {
        // We're not using the supplied dataset, just the name of it
        load_mra_dbs(path, ds.alias)
    }
}

fn load_mra_dbs(path: &Path, dao_name: String) -> Result<Box<InMemoryDao>> {
    let parent_path = path.parent().expect("Database file has no parent!");
    let storage_path = if path_file_name(parent_path)? == "Base" {
        parent_path.parent().expect(r#""Base" directory has no parent!"#)
    } else {
        parent_path
    }.to_path_buf();

    let mut dataset_map: DatasetMap = Default::default();

    log::info!("Loading {} (legacy) format", MRA_DBS);

    // Read the whole file into the memory.
    let dbs_bytes = fs::read(path)?;

    // We'll be loading chats in three phases.
    // Phase 1: Read conversations in an MRA inner format, mapped to file bytes.
    let convs_with_msgs = mra_dbs::load_convs_with_msgs(&dbs_bytes)?;

    // Phase 2: Populate datasets and users with latest values, usernames being emails.
    let dataset_map_2 = mra_dbs::collect_datasets(&convs_with_msgs, &storage_path)?;
    dataset_map.extend(dataset_map_2);

    // Phase 3: Convert conversations to our format.
    mra_dbs::convert_messages(&convs_with_msgs, &mut dataset_map)?;

    log::info!("Loading .db (newer) format");
    let mut conv_maps = HashMap::new();
    for subdir in DB_FILE_DIRS.iter() {
        let path = parent_path.join(subdir);
        if path.exists() {
            let conv_map = db::load_accounts_dir(&path, &storage_path, &mut dataset_map)?;
            conv_maps.extend(conv_map);
        }
    }

    if let Some(timestamp_diff) = find_timestamp_delta(&dataset_map, &conv_maps)? {
        change_timestamps(timestamp_diff, &mut dataset_map);
    }

    db::merge_conversations(conv_maps, &mut dataset_map)?;

    let data = dataset_map_to_dao_data(dataset_map);
    Ok(Box::new(InMemoryDao::new(
        dao_name,
        storage_path,
        data,
    )))
}

/// Old MRA format has invalid timezone while newer format treats timezone properly.
/// There's also a (possible) overlap between old and new messages, so we use it to try to determine
/// timezone difference.
fn find_timestamp_delta(
    old_dataset_map: &HashMap<String, MraDatasetEntry>,
    new_conv_maps: &HashMap<String, db::ConversationsMap>,
) -> Result<Option<i64>> {
    log::debug!("Trying to determine timestamp delta");
    for (myself_username, old_entry, new_conv) in new_conv_maps.iter()
        .filter_map(|(k, nv)| old_dataset_map.get(k).map(|ov| (k, ov, nv)))
    {
        for (other_username, old_msgs, new_msgs) in new_conv.iter()
            .filter_map(|(k, nv)| old_entry.cwms.get(k).map(|ov| (k, &ov.messages, &nv.0)))
            .filter(|(_, ov, nv)| !ov.is_empty() && !nv.is_empty())
        {
            log::debug!("Analyzing conversation between {myself_username} and {other_username}");

            // First new message that doesn't have earlier duplicates
            let first_viable_new_msg = new_msgs.iter()
                .enumerate()
                .find(|(idx, msg)| !new_msgs.iter().skip(*idx + 1).any(|msg2| msg_eq(msg, msg2)))
                .map(|(_, msg)| msg);

            if let Some(first_viable_new_msg) = first_viable_new_msg {
                // Find an equivalent message within old messages.
                // Note that for simplicity we don't check old message's relative uniqueness, leaving us vulnerable
                // to this unlikely corner case.
                if let Some(old_msg) = old_msgs.iter().rev().find(|old_msg| msg_eq(old_msg, first_viable_new_msg)) {
                    let delta = first_viable_new_msg.timestamp - old_msg.timestamp;
                    // Timestamps often differs by a few seconds beyond the normal timezone difference.
                    // Let's round it within 15-minutes precision
                    const DIV: f64 = 60.0 * 15.0;
                    let delta = ((delta as f64 / DIV).round() * DIV) as i64;
                    if delta.abs() < 24 * 60 * 60 {
                        log::debug!("Timestamp delta determined to be {delta} ({} hrs)", (delta as f64 / 60.0 / 60.0));
                        return Ok(Some(delta));
                    }
                }
            }
        }
    }
    log::warn!("Timestamp delta could not be determined!");
    Ok(None)
}

/// Whether or not two messages are equal, judging by the text and typed parts only.
fn msg_eq(msg1: &Message, msg2: &Message) -> bool {
    // PracticallyEq requires too much context, but direct Typed equality should do fine here
    msg1.text == msg2.text && msg1.typed == msg2.typed
}

fn change_timestamps(timestamp_diff: i64, dataset_map: &mut HashMap<String, MraDatasetEntry>) {
    for entry in dataset_map.values_mut() {
        for cwm in entry.cwms.values_mut() {
            for msg in cwm.messages.iter_mut() {
                msg.timestamp += timestamp_diff;
            }
        }
    }
}

fn dataset_map_to_dao_data(dataset_map: DatasetMap) -> Vec<DatasetEntry> {
    dataset_map.into_values().sorted_by_key(|e| e.ds.alias.clone()).map(|mut entry| {
        // Now that we know all user names, rename chats accordingly
        for cwm in entry.cwms.values_mut() {
            let chat_email = cwm.chat.name_option.as_ref().unwrap();
            if let Some(pretty_name) = entry.users.get(chat_email).map(|u| u.pretty_name()) {
                cwm.chat.name_option = Some(pretty_name);
            }
        }
        DatasetEntry {
            ds: entry.ds,
            ds_root: entry.ds_root,
            myself_id: MYSELF_ID,
            users: entry.users.into_values()
                .sorted_by_key(|u| if u.id() == MYSELF_ID { i64::MIN } else { u.id })
                .collect_vec(),
            cwms: entry.cwms.into_values().collect_vec(),
        }
    }).collect_vec()
}

fn convert_microblog_record(
    raw_text: &str,
    target_name: Option<&str>,
) -> (Vec<RichTextElement>, message::Typed) {
    let text = normalize_plaintext(raw_text);
    let text = format!("{}{}", target_name.map(|n| format!("(To {n})\n")).unwrap_or_default(), text);
    (vec![RichText::make_plain(text)],
     message_service!(ServiceSvo::StatusTextChanged(MessageServiceStatusTextChanged {})))
}

/// Turns out this format is shared exactly between old and new formats
fn collect_users_from_conference_user_changed_record(
    users: &mut HashMap<String, User>,
    ds_uuid: &PbUuid,
    conv_username: &str,
    mra_msg: &impl MraMessage,
    payload: &[u8],
) -> EmptyRes {
    // All payload is a single chunk
    let (change_tpe, payload) = next_u32(payload);

    match change_tpe {
        CONFERENCE_USER_JOINED => {
            let (_inviting_user_name_or_email, payload) = next_sized_chunk(payload)?;
            let (num_invited_user_names, mut payload) = next_u32_size(payload);
            let mut names = Vec::with_capacity(num_invited_user_names);
            let mut usernames = Vec::with_capacity(num_invited_user_names);

            for _ in 0..num_invited_user_names {
                let (name_bytes, payload2) = next_sized_chunk(payload)?;
                payload = payload2;
                names.push(utf16le_to_string(name_bytes)?);
            }
            let (num_invited_user_emails, mut payload) = next_u32_size(payload);
            require_format(num_invited_user_names == num_invited_user_emails, mra_msg, conv_username)?;

            for _ in 0..num_invited_user_names {
                let (username_bytes, payload2) = next_sized_chunk(payload)?;
                payload = payload2;
                usernames.push(utf16le_to_string(username_bytes)?);
            }
            require_format(payload.is_empty(), mra_msg, conv_username)?;

            for (username, name_or_email) in usernames.into_iter().zip(names.into_iter()) {
                upsert_user(users, ds_uuid, &username, Some(name_or_email));
            }
        }
        CONFERENCE_USER_LEFT => {
            let (name_bytes, payload) = next_sized_chunk(payload)?;
            let (email_bytes, payload) = next_sized_chunk(payload)?;
            require_format(payload.is_empty(), mra_msg, conv_username)?;

            upsert_user(users,
                        ds_uuid,
                        &utf16le_to_string(email_bytes)?,
                        Some(utf16le_to_string(name_bytes)?));
        }
        etc => {
            require_format_clue(false, mra_msg, conv_username,
                                &format!("Unexpected conference user change type {etc}"))?;
        }
    };
    Ok(())
}

fn convert_cartoon(src: &str) -> Result<TextAndTyped> {
    let (_id, emoji_option) = match SMILE_TAG_REGEX.captures(src) {
        Some(captures) => (captures.name("id").unwrap().as_str(),
                           captures.name("alt").and_then(|smiley| smiley_to_emoji(smiley.as_str()))),
        None => bail!("Unexpected cartoon source: {src}")
    };

    Ok((vec![], message_regular! {
        content_option: Some(Content {
            sealed_value_optional: Some(ContentSvo::Sticker(ContentSticker {
                path_option: None,
                width: 0,
                height: 0,
                thumbnail_path_option: None,
                emoji_option,
            }))
        }),
        ..Default::default()
    }))
}

fn convert_file_transfer(text: &str) -> Result<TextAndTyped> {
    let text = normalize_plaintext(text);
    // We can get file names from the outgoing messages.
    // Mail.Ru allowed us to send several files in one message, so we unite them here.
    // There are several formats Mail.Ru uses for these messages:
    // Legacy 1:
    //      <Message>
    //      file_1_path (file_1_size file_1_size_unit)
    //      file_2_path (file_2_size file_2_size_unit)
    //      ...
    //      <Total size message>: total_size total_size_unit
    // Legacy 2:
    //      Передача файлов
    // Legacy 3:
    //      Получены файлы
    // New:
    //      file_1_path;file_1_size;file_2_path;file_2_size;...
    let file_name_option = if text.ends_with(';') {
        let text_parts = text.split(';').collect_vec();
        Some(text_parts.as_slice().chunks_exact(2).map(|c| c[0]).join(", "))
    } else if text == "Передача файлов" || text == "Получены файлы" {
        None
    } else {
        let text_parts = text.split('\n').collect_vec();
        require!(text_parts.len() >= 3, "Unknown file transfer message format: {}", text);
        let file_paths: Vec<&str> = text_parts.smart_slice(1..-1).iter().map(|&s|
            s.trim()
                .rsplitn(3, ' ')
                .nth(2)
                .context("Unexpected file path format!"))
            .try_collect()?;
        Some(file_paths.iter().join(", "))
    };
    Ok((vec![], message_regular! {
        content_option: Some(Content {
            sealed_value_optional: Some(ContentSvo::File(ContentFile {
                path_option: None,
                file_name_option,
                mime_type_option: None,
                thumbnail_path_option: None,
            }))
        }),
        ..Default::default()
    }))
}

/// Turns out this format is shared exactly between old and new formats
fn convert_conference_user_changed_record(
    conv_username: &str,
    mra_msg: &impl MraMessage,
    payload: &[u8],
    users: &HashMap<String, User>,
) -> Result<TextAndTyped> {
    let (change_tpe, payload) = next_u32(payload);
    // We don't care about user names here because they're already set by collect_datasets
    let service = match change_tpe {
        CONFERENCE_USER_JOINED => {
            let (_inviting_user_name_or_email, payload) = next_sized_chunk(payload)?;
            let (num_invited_user_names, mut payload) = next_u32_size(payload);

            for _ in 0..num_invited_user_names {
                let (_name_bytes, payload2) = next_sized_chunk(payload)?;
                payload = payload2;
            }

            let (num_invited_user_emails, mut payload) = next_u32_size(payload);
            let mut emails = Vec::with_capacity(num_invited_user_emails);
            for _ in 0..num_invited_user_names {
                let (email_bytes, payload2) = next_sized_chunk(payload)?;
                payload = payload2;
                emails.push(utf16le_to_string(email_bytes)?);
            }
            require_format(payload.is_empty(), mra_msg, conv_username)?;

            let members = emails.iter().map(|e| users[e].pretty_name()).collect_vec();
            ServiceSvo::GroupInviteMembers(MessageServiceGroupInviteMembers { members })
        }
        CONFERENCE_USER_LEFT => {
            let (_name_bytes, payload) = next_sized_chunk(payload)?;
            let (email_bytes, payload) = next_sized_chunk(payload)?;
            require_format(payload.is_empty(), mra_msg, conv_username)?;

            let email = utf16le_to_string(email_bytes)?;
            let members = vec![users[&email].pretty_name()];
            ServiceSvo::GroupRemoveMembers(MessageServiceGroupRemoveMembers { members })
        }
        etc => bail!("Unexpected {:?} change type {etc}\nMessage: {mra_msg:?}", mra_msg.get_tpe()?)
    };

    Ok((vec![], message_service!(service)))
}

/// Returns `None` if this message should be discarded
fn process_call(
    text: &str,
    internal_id: i64,
    mra_msg: &impl MraMessage,
    conv_username: &str,
    timestamp: i64,
    ongoing_call_msg_id: &mut Option<i64>,
    prev_msgs: &mut [Message],
) -> Result<Option<TextAndTyped>> {
    const BEGIN_CONNECTING: &str = "Устанавливается соединение...";
    const BEGIN_CONNECTING_2: &str = "Устанавливается соединение";
    const BEGIN_I_CALL: &str = "Звонок от вашего собеседника";
    const BEGIN_I_VCALL: &str = "Видеозвонок от вашего собеседника";
    const BEGIN_O_CALL: &str = "Вы звоните собеседнику. Ожидание ответа...";
    const BEGIN_STARTED: &str = "Начался разговор";

    const END_HANG: &str = "Звонок завершен";
    const END_VHANG: &str = "Видеозвонок завершен";
    const END_CONN_FAILED: &str = "Не удалось установить соединение. Попробуйте позже.";
    const END_I_CANCELLED: &str = "Вы отменили звонок";
    const END_I_CANCELLED_2: &str = "Вы отклонили звонок";
    const END_I_VCANCELLED: &str = "Вы отменили видеозвонок";
    const END_I_VCANCELLED_2: &str = "Вы отклонили видеозвонок"; // This one might not be real
    const END_O_CANCELLED: &str = "Собеседник отменил звонок";
    const END_O_CANCELLED_2: &str = "Собеседник отклонил ваш звонок";
    const END_O_VCANCELLED: &str = "Собеседник отменил видеозвонок";

    // MRA is not very rigid in propagating all the statuses.
    match text {
        BEGIN_CONNECTING | BEGIN_CONNECTING_2 | BEGIN_I_CALL | BEGIN_I_VCALL | BEGIN_O_CALL | BEGIN_STARTED => {
            if ongoing_call_msg_id.is_some_and(|id| internal_id - id <= 5) {
                // If call is already (recently) marked, do nothing
                return Ok(None);
            } else {
                // Save call ID to later amend with duration and status.
                *ongoing_call_msg_id = Some(internal_id);
            }
        }
        END_HANG | END_VHANG |
        END_CONN_FAILED |
        END_I_CANCELLED | END_I_CANCELLED_2 | END_I_VCANCELLED | END_I_VCANCELLED_2 |
        END_O_CANCELLED | END_O_CANCELLED_2 | END_O_VCANCELLED => {
            if ongoing_call_msg_id.is_some_and(|id| internal_id - id <= 50) {
                let msg_id = ongoing_call_msg_id.unwrap();
                let msg = prev_msgs.iter_mut().rfind(|m| m.internal_id == msg_id).unwrap();
                let start_time = msg.timestamp;
                let discard_reason_option = match text {
                    END_HANG | END_VHANG => None,
                    END_CONN_FAILED => Some("Failed to connect"),
                    END_I_CANCELLED | END_I_CANCELLED_2 | END_I_VCANCELLED | END_I_VCANCELLED_2 => Some("Declined by you"),
                    END_O_CANCELLED | END_O_CANCELLED_2 | END_O_VCANCELLED => Some("Declined by user"),
                    _ => unreachable!()
                };
                match msg.typed_mut() {
                    message_service_pat!(ServiceSvo::PhoneCall(call)) => {
                        call.duration_sec_option = Some((timestamp - start_time) as i32);
                        call.discard_reason_option = discard_reason_option.map(|s| s.to_owned());
                    }
                    etc => {
                        require_format_clue(false, mra_msg, conv_username,
                                            &format!("unexpected ongoing call type: {etc:?}"))?;
                        unreachable!()
                    }
                };
                *ongoing_call_msg_id = None;
            }
            // Either way, this message itself isn't supposed to have a separate entry.
            return Ok(None);
        }
        etc => {
            require_format_clue(false, mra_msg, conv_username,
                                &format!("unexpected call message: {etc}"))?;
            unreachable!()
        }
    }

    Ok(Some((vec![], message_service!(ServiceSvo::PhoneCall(MessageServicePhoneCall {
            duration_sec_option: None,
            discard_reason_option: None,
        }))
    )))
}

//
// Structs and enums
//

struct MraDatasetEntry {
    ds: Dataset,
    ds_root: PathBuf,
    /// Key is username (in most cases, email)
    users: HashMap<String, User>,
    /// Key is conversation name (in most cases, email or email-like name)
    cwms: HashMap<String, ChatWithMessages>,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromPrimitive)]
enum MraMessageType {
    Empty = 0x00,
    RegularPlaintext = 0x02,
    AuthorizationRequest = 0x04,
    AntispamTriggered = 0x06,
    RegularRtf = 0x07,
    FileTransfer = 0x0A,
    /// Call status change - initiated, cancelled, connecting, done, etc.
    /// Note: some call statuses might be missing!
    Call = 0x0C,
    BirthdayReminder = 0x0D,
    /// Not sure about that one
    Sms = 0x11,
    Cartoon = 0x1A,
    /// Call status change - initiated, cancelled, connecting, done, etc.
    /// Note: some call statuses might be missing!
    VideoCall = 0x1E,
    /// User was invited or left the conference
    ConferenceUsersChange = 0x22,
    MicroblogRecordBroadcast = 0x23,
    ConferenceMessagePlaintext = 0x24,
    ConferenceMessageRtf = 0x25,
    /// Not sure what's the difference with a regular cartoon
    CartoonType2 = 0x27,
    /// Payload has a name of the user this is directed to
    MicroblogRecordDirected = 0x29,
    LocationChange = 0x2E,

    //
    // Encountered in newer MRA only
    //

    Sticker = 0x40,
}

trait MraMessage: Debug {
    fn get_tpe(&self) -> Result<MraMessageType>;

    fn is_from_me(&self) -> Result<bool>;
}

//
// Assertions
//

fn context(mra_msg: &impl MraMessage, conv_username: &str) -> String {
    format!("Unexpected {:?} message format\nConversation: {conv_username}, message: {mra_msg:?}", mra_msg.get_tpe().unwrap())
}

fn require_format(cond: bool, mra_msg: &impl MraMessage, conv_username: &str) -> EmptyRes {
    require!(cond, "Unexpected {:?} message format\nConversation: {conv_username}, message: {mra_msg:?}", mra_msg.get_tpe()?);
    Ok(())
}

fn require_format_clue(cond: bool, mra_msg: &impl MraMessage, conv_username: &str, clue: &str) -> EmptyRes {
    require!(cond, "Unexpected {:?} message format: {clue}\nConversation: {conv_username}, message: {mra_msg:?}", mra_msg.get_tpe()?);
    Ok(())
}

fn require_format_with_clue(cond: bool, mra_msg: &impl MraMessage, conv_username: &str, clue: impl Fn() -> String) -> EmptyRes {
    require!(cond, "Unexpected {:?} message format: {}\nConversation: {conv_username}, message: {mra_msg:?}", mra_msg.get_tpe()?, clue());
    Ok(())
}

//
// Helper functions
//

/// Clone a field from a packed structure, useful for debugging
#[macro_export]
macro_rules! clone_packed { ($e:expr) => { { let v = $e; v.clone() }};}

/// Create or update user by username, possibly setting a first name if it's not an email too.
fn upsert_user(users: &mut HashMap<String, User>,
               ds_uuid: &PbUuid,
               username: &str,
               first_name_or_email: Option<String>) {
    let user = users.entry(username.to_owned()).or_insert_with(|| User {
        ds_uuid: ds_uuid.clone(),
        id: loader::hash_to_id(username),
        first_name_option: None,
        last_name_option: None,
        username_option: Some(username.to_owned()),
        phone_number_option: None,
    });

    if user.first_name_option.is_none() && first_name_or_email.as_ref().is_some_and(|v| v != username) {
        user.first_name_option = first_name_or_email;
    }
}

// All read functions read in Little Endian

fn read_n_bytes<const N: usize>(bytes: &[u8], shift: usize) -> [u8; N] {
    bytes[shift..(shift + N)].try_into().unwrap()
}

fn read_u32(bytes: &[u8], shift: usize) -> u32 {
    u32::from_le_bytes(read_n_bytes(bytes, shift))
}

fn next_u32(bytes: &[u8]) -> (u32, &[u8]) {
    (read_u32(bytes, 0), &bytes[4..])
}

fn next_u32_size(bytes: &[u8]) -> (usize, &[u8]) {
    (read_u32(bytes, 0) as usize, &bytes[4..])
}

/// Assumes the next 4 payload bytes to specify the size of the chunk. Read and return it, and the rest of the payload.
fn next_sized_chunk(payload: &[u8]) -> Result<(&[u8], &[u8])> {
    let (len, rest) = next_u32_size(payload);
    Ok(rest.split_at(len))
}

/// In the next <N_u32><...N bytes...> validate that N bytes correspond to the expected bytes provided
fn validate_skip_chunk<'a>(payload: &'a [u8], expected_bytes: &[u8]) -> Result<&'a [u8]> {
    let (len, payload) = next_u32_size(payload);
    require!(len == expected_bytes.len(),
             "Unexpected message payload format!");
    let (actual, rest) = payload.split_at(len);
    require!(actual == expected_bytes,
             "Unexpected message payload format!");
    Ok(rest)
}

fn u32_ptr_to_option(int: u32) -> Option<u32> {
    match int {
        0 => None,
        x => Some(x)
    }
}

fn filetime_to_timestamp(ft: u64) -> i64 {
    // TODO: Timezone are maybe off, even though both are UTC?
    // WinApi FILETIME epoch starts 1601-01-01T00:00:00Z, which is 11644473600 seconds before the
    // UNIX/Linux epoch (1970-01-01T00:00:00Z). FILETIME ticks are also in in 100 nanoseconds.
    const TICKS_PER_SECOND: u64 = 10_000_000;
    const SECONSDS_TO_UNIX_EPOCH: i64 = 11_644_473_600;
    let time = ft / TICKS_PER_SECOND;
    time as i64 - SECONSDS_TO_UNIX_EPOCH
}

fn find_first_position<T: PartialEq>(source: &[T], to_find: &[T], step: usize) -> Option<usize> {
    inner_find_positions_of(source, to_find, step, true).first().cloned()
}

/// Efficiently find all indexes of the given sequence occurrence within a longer source sequence.
/// Does not return indexes that overlap matches found earlier.
/// Works in O(n) of the source length, assuming to_find length to be negligible and not accounting for degenerate
/// input cases.
fn inner_find_positions_of<T: PartialEq>(source: &[T], to_find: &[T], step: usize, find_one: bool) -> Vec<usize> {
    assert!(to_find.len() % step == 0, "to_find sequence length is not a multiplier of {step}!");
    if to_find.is_empty() { panic!("to_find slice was empty!"); }
    let max_i = source.len() as i64 - to_find.len() as i64 + 1;
    if max_i <= 0 { return vec![]; }
    let max_i = max_i as usize;
    let mut res = vec![];
    let mut i = 0_usize;
    'outer: while i < max_i {
        for j in 0..to_find.len() {
            if source[i + j] != to_find[j] {
                i += step;
                continue 'outer;
            }
        }
        // Match found
        res.push(i);
        if find_one {
            return res;
        }
        i += to_find.len();
    }
    res
}

fn get_null_terminated_utf16le_slice(bs: &[u8]) -> Result<&[u8]> {
    static NULL_UTF16: &[u8] = &[0x00, 0x00];

    let null_term_idx = 2 * bs.chunks(2)
        .position(|bs| bs == NULL_UTF16)
        .context("Null terminator not found!")?;

    Ok(&bs[..null_term_idx])
}

fn bytes_to_pretty_string(bytes: &[u8], columns: usize) -> String {
    let mut result = String::with_capacity(bytes.len() * 3);
    for row in bytes.chunks(columns) {
        for group in row.chunks(4) {
            for b in group {
                if *b == 0x00 {
                    result.push_str("..");
                } else {
                    result.push_str(&format!("{b:02x}"));
                }
            }
            result.push(' ');
        }
        result.push('\n');
    }
    result.trim_end().to_owned()
}

fn utf16le_to_string(unicode_bytes: &[u8]) -> Result<String> {
    let len = unicode_bytes.len();
    require!(len % 2 == 0, "Odd number of UTF-16 bytes");
    let mut unicode_bytes = Cow::Borrowed(unicode_bytes);

    // Handling special case: singular unpaired surrogate code units.
    // This is not a valid Unicode but it might be present - in particular, in RTF.
    // If encountered, replace it with "?" (\U003F)
    macro_rules! is_surrogate_2nd_byte {
        ($i:expr) => { unicode_bytes[$i] >= 0xD8 && unicode_bytes[$i] <= 0xDF };
    }
    let mut i = 1;
    while i < len {
        if is_surrogate_2nd_byte!(i) {
            if i + 2 >= len || !is_surrogate_2nd_byte!(i + 2) {
                let mut bytes2 = unicode_bytes.into_owned();
                bytes2[i] = 0x00;
                bytes2[i - 1] = 0x3F;
                unicode_bytes = Cow::Owned(bytes2);
            } else {
                i += 2; // Skip 2nd code unit
            }
        }
        i += 2;
    }

    let result = WStr::from_utf16le(&unicode_bytes)
        .with_context(|| format!("Illegal UTF-16 bytes: {unicode_bytes:02X?}"))?
        .to_utf8();
    Ok(result)
}

/// Parses an RTF document into internal rich text format, to some degree.
/// Handles bold, italic and underline styles, interprets everything else as a plaintext.
/// This is by no means a full-fledged RTF parser, but it does a decent enough job.
fn parse_rtf(rtf: &str) -> Result<Vec<RichTextElement>> {
    use rtf_grimoire::tokenizer::Token;

    let tokens = rtf_grimoire::tokenizer::parse_finished(rtf.as_bytes())
        .map_err(|_e| anyhow!("Unable to parse RTF {rtf}"))?;
    if tokens.is_empty() { return Ok(vec![]); }

    // \fcharset0 is cp1252
    require!(tokens.iter().any(|t|
                matches!(t, Token::ControlWord { name, arg: Some(arg) }
                            if name == "ansicpg" || (name == "fcharset" && *arg == 0) )
             ), "RTF is not ANSI-encoded!\nRTF: {rtf}");

    const DEFAULT_ENC_ID: i32 = 1;

    // This is by no means exhaustive, and only some encodings has been verified to actually match.
    fn get_rtf_charset(id: i32) -> Option<&'static Encoding> {
        use encoding_rs::*;
        match id {
            0 => Some(WINDOWS_1252),
            1 => Some(WINDOWS_1251) /* Originally, default Windows API code page for system locale*/,
            128 => Some(SHIFT_JIS) /* Windows-932, Japanese Shift JIS */,
            129 => Some(EUC_KR)/* Windows-949, Korean, Unified Hangul */,
            134 => Some(GBK) /* Windows-936, Chinese, GBK (extended GB 2312) */,
            136 => Some(BIG5) /* Windows-950, Chinese, Big5 */,
            161 => Some(WINDOWS_1253) /* Greek */,
            162 => Some(WINDOWS_1254) /* Turkish */,
            163 => Some(WINDOWS_1258) /* Vietnamese */,
            177 => Some(WINDOWS_1255) /* Hebrew */,
            178 => Some(WINDOWS_1256) /* Arabic */,
            186 => Some(WINDOWS_1257) /* Baltic */,
            204 => Some(WINDOWS_1251) /* Russian */,
            222 => Some(WINDOWS_874)/* Thai */,
            238 => Some(WINDOWS_1250) /* Latin, Eastern Europe */,
            254 => Some(IBM866)/* PC 437 */,
            255 => Some(WINDOWS_1251) /* Supposed to be OEM but oh well */,
            _ => None /* Unknown, not available, or don't bother decoding */,
        }
    }

    let fonttbl_charsets = {
        let mut fonttbl_charsets = vec![];
        let mut depth = 0;
        let start = Token::ControlWord { name: "fonttbl".to_owned(), arg: None };
        for token in tokens.iter().skip_while(|&t| t != &start).skip(1) {
            match token {
                Token::ControlWord { ref name, arg: Some(arg) } if name == "f" => {
                    require!(*arg == fonttbl_charsets.len() as i32, "Malformed RTF fonts table!\nRTF: {rtf}");
                }
                Token::ControlWord { ref name, arg: Some(charset_num) } if name == "fcharset" => {
                    fonttbl_charsets.push(get_rtf_charset(*charset_num));
                }
                Token::StartGroup => {
                    depth += 1;
                }
                Token::EndGroup => {
                    depth -= 1;
                    if depth < 0 { break; }
                }
                _ => { /* NOOP */ }
            }
        }
        fonttbl_charsets
    };
    let mut enc = get_rtf_charset(DEFAULT_ENC_ID);

    // Text of current styled section
    let mut curr_text: String = "".to_owned();

    // Bytes of currently constructed UTF-16 LE string
    let mut unicode_bytes: Vec<u8> = vec![];

    // Bytes of currently constructed charset-specific string
    let mut charset_bytes: Vec<u8> = vec![];

    // Returned text is mutable and should be appended.
    // Calling this will flush Unicode string under construction (if any).
    macro_rules! flush_text_buffers {
        () => {{
            assert!(unicode_bytes.is_empty() || charset_bytes.is_empty());
            // Flush the existing constructed string, if any
            if !unicode_bytes.is_empty() {
                curr_text.push_str(&utf16le_to_string(&unicode_bytes)?.replace("\r\n", "\n"));
                unicode_bytes.clear();
            }
            if !charset_bytes.is_empty() {
                if let Some(enc) = enc {
                    curr_text.push_str(&to_utf8(&charset_bytes, enc)?.replace("\r\n", "\n"));
                } else {
                    curr_text.push_str("?");
                }
                charset_bytes.clear();
            }
        }};
    }

    // If multiple styles are set, last one set will override the others
    enum Style { Plain, Bold, Italic, Underline }
    let mut style = Style::Plain;

    fn make_rich_text(src: String, style: &Style) -> RichTextElement {
        match style {
            Style::Plain => RichText::make_plain(src),
            Style::Bold => RichText::make_bold(src),
            Style::Italic => RichText::make_italic(src),
            Style::Underline => RichText::make_underline(src),
        }
    }

    let mut result: Vec<RichTextElement> = vec![];

    // Commits current styled section to a result, clearing current text.
    macro_rules! commit_section {
        () => {
            flush_text_buffers!();
            let text = curr_text.trim();
            if !text.is_empty() {
                let text = normalize_plaintext(text);
                result.push(make_rich_text(text, &style));
            }
            curr_text.clear();
        };
    }

    // Unicode control words are followed by a "backup" plaintext char in case client doesn't speak Unicode.
    // We do, so we skip that char.
    let mut skip_next_char = false;

    // We don't care about styling header, so we're skipping it.
    let colortbl = Token::ControlWord { name: "colortbl".to_owned(), arg: None };
    for token in tokens.into_iter().skip_while(|t| *t != colortbl).skip_while(|t| *t != Token::EndGroup) {
        let get_new_state = |arg: Option<i32>, desired: Style| -> Result<Style> {
            match arg {
                None => Ok(desired),
                Some(0) => Ok(Style::Plain),
                Some(_) => err!("Unknown RTF token {token:?}\nRTF: {rtf}")
            }
        };
        match token {
            Token::ControlWord { ref name, arg } if name == "i" => {
                commit_section!();
                style = get_new_state(arg, Style::Italic)?;
            }
            Token::ControlWord { ref name, arg } if name == "b" => {
                commit_section!();
                style = get_new_state(arg, Style::Bold)?;
            }
            Token::ControlWord { ref name, arg } if name == "ul" => {
                commit_section!();
                style = get_new_state(arg, Style::Underline)?;
            }
            Token::ControlWord { ref name, arg } if name == "ulnone" => {
                commit_section!();
                style = get_new_state(arg, Style::Plain)?;
            }
            Token::ControlWord { name, arg: Some(arg) } if name == "'" && arg >= 0 => {
                // If Unicode was being contructed, commit it first
                if !unicode_bytes.is_empty() {
                    flush_text_buffers!();
                }
                charset_bytes.push(arg as u8);
            }
            Token::ControlWord { name, arg: Some(arg) } if name == "u" => {
                // If charset-encoded string was being contructed, commit it first
                if !charset_bytes.is_empty() {
                    flush_text_buffers!();
                }
                // As per spec, "Unicode values greater than 32767 must be expressed as negative numbers",
                // but Mail.Ru doesn't seem to care.
                require!(arg >= 0, "Unexpected Unicode value!\nRTF: {rtf}");
                let arg = arg as u16;
                unicode_bytes.extend_from_slice(&arg.to_le_bytes());
                skip_next_char = true;
            }
            Token::ControlWord { name, arg: Some(font_num) } if name == "f" => {
                flush_text_buffers!();
                enc = fonttbl_charsets[font_num as usize];
            }
            Token::ControlWord { name, .. } if name == "plain" => {
                flush_text_buffers!();
                enc = get_rtf_charset(DEFAULT_ENC_ID);
            }
            Token::Text(t) => {
                let string = String::from_utf8(t)?;
                let mut str = string.as_str();
                if skip_next_char {
                    str = &str[1..];
                    skip_next_char = false;
                }
                // Only flush text if string is actually appended, otherwise it might interrupt
                // multi-code-points charactes, like those with surrogates.
                if !str.is_empty() {
                    flush_text_buffers!();
                    curr_text.push_str(str);
                }
            }
            Token::Newline(_) => {
                // \r\n is parsed as a single newline token
                flush_text_buffers!();
                curr_text.push('\n');
            }
            Token::ControlSymbol(c) => {
                flush_text_buffers!();
                curr_text.push(c);
            }
            Token::ControlBin(_) =>
                bail!("Unexpected RTF token {token:?} in {rtf}"),
            _ => {}
        }
    }
    commit_section!();
    Ok(result)
}

fn to_utf8<'a>(bytes: &'a [u8], enc: &'static Encoding) -> Result<Cow<'a, str>> {
    let (res, had_errors) = enc.decode_without_bom_handling(bytes);
    if !had_errors {
        Ok(res)
    } else {
        err!("Couldn't decode {bytes:02x?}")
    }
}

/// Replaces \r\n and \r with \n, and <SMILE> tags and inline smiles with emojis
fn normalize_plaintext(s: &str) -> String {
    let s = s.replace("\r\n", "\n").replace('\r', "\n");

    let s = SMILE_TAG_REGEX.replace_all(&s, |capt: &Captures| {
        if let Some(smiley) = capt.name("alt") {
            let smiley = smiley.as_str();
            let emoji_option = smiley_to_emoji(smiley);
            emoji_option.unwrap_or_else(|| smiley.to_owned())
        } else {
            // Leave as-is
            capt.get(0).unwrap().as_str().to_owned()
        }
    });

    let s = SMILE_INLINE_REGEX.replace_all(&s, |capt: &Captures| {
        let smiley = capt.get(0).unwrap().as_str();
        let emoji_option = smiley_to_emoji(smiley);
        emoji_option.unwrap_or_else(|| smiley.to_owned())
    });

    // SMILE_IMG_REGEX is a third format, but I don't know a replacement for any of them
    //
    // let s = SMILE_IMG_REGEX.replace_all(&s, |capt: &Captures| {
    //     let smiley_id = capt.name("id").unwrap().as_str();
    //     println!("{}", smiley_id);
    //     todo!()
    // });

    s.into()
}

/// Replaces a :Smiley: code with an emoji character if known
fn smiley_to_emoji(smiley: &str) -> Option<String> {
    // This isn't a full list, just the ones I got.
    // There's also a bunch of numeric smileys like :6687: whose meaning isn't known.
    match smiley {
        ":Ок!:" | ":Да!:" => Some("👍"),
        ":Не-а:" | ":Нет!:" => Some("👎"),
        ":Отлично!:" => Some("💯"),
        ":Жжёшь!:" => Some("🔥"),
        ":Радуюсь:" | ":Радость:" | ":Улыбка до ушей:" | ":Улыбка_до_ушей:" | ":Смеюсь:" | "[:-D" => Some("😁"),
        ":Улыбаюсь:" => Some("🙂"),
        ":Лопну от смеха:" => Some("😂"),
        ":Хихикаю:" => Some("🤭"),
        ":Подмигиваю:" => Some("😉"),
        ":Расстраиваюсь:" | ":Подавлен:" => Some("😟"),
        ":Смущаюсь:" => Some("🤭"),
        ":Стыдно:" => Some("🫣"),
        ":Удивляюсь:" | ":Ты что!:" | ":Фига:" | ":Ой, ё:" => Some("😯"),
        ":Сейчас расплачусь:" | ":Извини:" | ":Скучаю:" => Some("🥺"),
        ":Хны...!:" => Some("😢"),
        ":Плохо:" | ":В печали:" => Some("😔"),
        ":Рыдаю:" => Some("😭"),
        ":Дразнюсь:" | ":Дурачусь:" | ":Показываю язык:" => Some("😝"),
        ":Виноват:" => Some("😅"),
        ":Сумасшествие:" | ":А я сошла с ума...:" => Some("🤪"),
        ":Целую:" => Some("😘"),
        ":Влюбленный:" | ":Влюблён:" | ":С любовью:" => Some("😍️"),
        ":Поцелуй:" => Some("💋"),
        ":Поцеловали:" => Some("🥰"),
        ":Купидон:" | ":На крыльях любви:" => Some("💘️"),
        ":Сердце:" | ":Люблю:" | ":Любовь:" => Some("❤️"),
        ":Сердце разбито:" => Some("💔️"),
        ":Красотка:" => Some("😊"),
        ":Тошнит:" | ":Гадость:" => Some("🤮"),
        ":Пугаюсь:" => Some("😨"),
        ":Ура!:" | ":Уррра!:" => Some("🎉"),
        ":Кричу:" => Some("📢"),
        ":Подозреваю:" | ":Подозрительно:" => Some("🤨"),
        ":Думаю:" | ":Надо подумать:" => Some("🤔"),
        ":Взрыв мозга:" => Some("🤯"),
        ":Аплодисменты:" => Some("👏"),
        ":Требую:" => Some("🫴"),
        ":Не знаю:" => Some("🤷‍️"),
        ":Ангелок:" | ":Ангелочек:" => Some("😇"),
        ":Чертенок:" | ":Злорадствую:" => Some("😈"),
        ":Пристрелю!:" | ":Застрелю:" | ":Злюсь:" => Some("😡"),
        ":Свирепствую:" => Some("🤬"),
        ":Чертовски злюсь:" => Some("👿"),
        ":Отвали!:" => Some("🖕"),
        ":Побью:" | ":Побили:" | ":В атаку!:" | ":Атакую:" => Some("👊"),
        ":Задолбал!:" => Some("😒"),
        ":Сплю:" => Some("😴"),
        ":Мечтаю:" => Some("😌"),
        ":Прорвемся!:" => Some("💪"),
        ":Пока!:" | ":Пока-пока:" => Some("👋"),
        ":Устал:" | ":В изнеможении:" => Some("😮‍💨"),
        ":Танцую:" => Some("🕺"),
        ":Ктулху:" => Some("🐙"),
        ":Я круче:" => Some("😎"),
        ":Вояка:" => Some("🥷"),
        ":Пиво:" | ":Пивка?;):" => Some("🍺"),
        ":Алкоголик:" => Some("🥴"),
        ":Бойан:" => Some("🪗"),
        ":Лапками-лапками:" => Some("🐾"),
        ":Кондитер:" => Some("👨‍🍳"),
        ":Головой об стену:" => Some("🤕"),
        ":Слушаю музыку:" => Some("🎵"),
        ":Кушаю:" | ":Жую:" => Some("😋"),
        ":Дарю цветочек:" | ":Заяц с цветком:" | ":Не опаздывай:" => Some("🌷"),
        ":Пошалим?:" | ":Хочу тебя:" => Some("😏"),
        ":Ревность:" => Some("😤"),
        ":Внимание!:" => Some("⚠️"),
        ":Помоги:" => Some("🆘"),
        ":Мир!:" => Some("🤝"),
        r#":Левая "коза":"# | r#":Правая "коза":"# => Some("🤘"),
        ":Лучезарно:" => Some("☀️"),
        ":Пацанчик:" => Some("🤠️"),
        ":Карусель:" => Some("🎡"),
        ":Бабочка:" => Some("🦋"),
        ":Голубки:" => Some("🕊"),
        ":Бабло!:" => Some("💸"),
        ":Кот:" | ":Кошки-мышки:" => Some("🐈"),
        ":Пёс:" => Some("🐕"),
        ":Выпей яду:" => Some("☠️"),
        ":Серьёзен как никогда, ага:" => Some("😐️"),
        "[:-|" => Some("🗿"),
        other => {
            // Might also mean this is not a real smiley
            log::warn!("No emoji known for a smiley {other}");
            None
        }
    }.map(|s| s.to_owned())
}
