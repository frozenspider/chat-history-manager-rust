use std::{cmp, fmt, fs, mem, slice};
use std::collections::HashMap;
use std::fmt::Debug;

use lazy_static::lazy_static;
use num_traits::FromPrimitive;
use regex::Regex;
use utf16string::{LE, WStr};

use crate::*;
use crate::dao::in_memory_dao::{DatasetEntry, InMemoryDao};
use crate::loader::DataLoader;
use crate::protobuf::history::*;

use super::*;

const MRA_DBS: &str = "mra.dbs";
const DATASETS_DIR_NAME: &str = "_datasets";
const MSG_HEADER_MAGIC_NUMBER: u32 = 0x38;

/// Using a first legal ID (i.e. "1") for myself
const MYSELF_ID: UserId = UserId(UserId::INVALID.0 + 1);

/// Old versions of Mail.Ru Agent stored data in unknown DBMS format storage, with strings formatted as UTF-16 LE.
///
/// Following references were helpful in reverse engineering the format (in Russian):
/// * https://xakep.ru/2012/11/30/mailru-agent-hack/
/// * https://c0dedgarik.blogspot.com/2010/08/mradbs.html
pub struct MailRuAgentDataLoader;

impl DataLoader for MailRuAgentDataLoader {
    fn name(&self) -> &'static str { "Mail.Ru Agent" }

    fn src_alias(&self) -> &'static str { "MRA (DBS)" }

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
    let mut storage_path = path.parent().expect("Database file has no parent!");
    if path_file_name(storage_path)? == "Base" {
        storage_path = storage_path.parent().expect(r#""Base" directory has no parent!"#);
    }
    let storage_path = storage_path.to_path_buf();

    // Read the whole file into the memory.
    let dbs_bytes = fs::read(path)?;

    // We'll be loading chats in two phases.
    // Phase 1: Read conversations in an inner format, mapped to file bytes.
    const OFFSETS_TABLE_OFFSE: usize = 0x10;

    let offsets_table_addr = read_u32_le(&dbs_bytes, OFFSETS_TABLE_OFFSE) as usize;
    let offsets_table: &[u32] = {
        let u8_slice = &dbs_bytes[offsets_table_addr..];
        require!(offsets_table_addr % 4 == 0 && u8_slice.len() % 4 == 0,
                 "Misaligned offset table at address {:#010x}!", offsets_table_addr);
        // Re-interpreting offsets table as a u32 slice.
        // This is safe because we already checked slice alignment and length.
        unsafe { slice::from_raw_parts(u8_slice.as_ptr() as *const u32, u8_slice.len() / 4) }
    };
    log::debug!("Offsets table is at {:#010x}", offsets_table_addr);
    require!(offsets_table[0] == 4_u32,
             "Sanity check failed: unexpected offset table magic number");

    let convs = load_conversations(&dbs_bytes, offsets_table)?;
    let convs_with_msgs = load_messages(&dbs_bytes, offsets_table, convs)?;

    // // Instead of bothering to traverse hash table and linked list relations, find all messages by a simple linear scan.
    // // "mrahistory_" UTF-16 (LE) bytes
    // let mrahistory_footprint = "mrahistory_".as_bytes().iter().flat_map(|&b| vec![b, 0x00u8]).collect_vec();
    // println!("{:?}", mrahistory_footprint);
    //
    // let mrahistory_record_addrs = find_positions_of(&dbs_bytes, &mrahistory_footprint);
    // println!("{:#010x?}", mrahistory_record_addrs.iter().map(|usz| *usz as u32).collect_vec());
    // println!("{}", mrahistory_record_addrs.len());

    // Phase 2: Convert conversations to our format.

    let data = convert(&convs_with_msgs, &dbs_bytes, &storage_path)?;

    Ok(Box::new(InMemoryDao::new(
        dao_name,
        storage_path,
        data,
    )))
}

fn load_conversations<'a>(dbs_bytes: &'a [u8], offsets_table: &[u32]) -> Result<Vec<MraConversation<'a>>> {
    // Offsets are relative to where offset table points at
    const CONVERSATIONS_COUNT_OFFSET: usize = 0x20;
    const LAST_CONVERSATION_OFFSET: usize = 0x2C;

    const CONVERSATION_IDS_OFFSET: usize = 0x04;
    const MESSAGE_IDS_OFFSET: usize = 0x28;
    const MRAHISTORY_FOOTPRINT_OFFSET: usize = 0x194;

    let mrahistory_footprint: &[u8] =
        &"mrahistory_".as_bytes().iter().flat_map(|&b| vec![b, 0x00u8]).collect_vec();

    let expected_convs_count = read_u32_le(&dbs_bytes, offsets_table[1] as usize + CONVERSATIONS_COUNT_OFFSET);
    let mut conv_id = read_u32_le(&dbs_bytes, offsets_table[1] as usize + LAST_CONVERSATION_OFFSET);

    let mut result = vec![];

    let mut last_processed_conv_id = 0;
    let mut actual_convs_count = 0;
    while conv_id != 0 {
        let current_offset = offsets_table[conv_id as usize] as usize;
        assert!(current_offset < dbs_bytes.len());

        let id = read_u32_le(&dbs_bytes, current_offset);
        let prev_conv_id = read_u32_le(&dbs_bytes, current_offset + CONVERSATION_IDS_OFFSET);
        let next_conv_id = read_u32_le(&dbs_bytes, current_offset + CONVERSATION_IDS_OFFSET + 4);

        require!(prev_conv_id == last_processed_conv_id,
                 "Conversations linked list is broken!");

        let mrahistory_loc = current_offset + MRAHISTORY_FOOTPRINT_OFFSET;
        if &dbs_bytes[mrahistory_loc..][0..mrahistory_footprint.len()] == mrahistory_footprint {
            // Setting the pointer right after the "mrahistory_"
            let name_utf16 = &dbs_bytes[(mrahistory_loc + mrahistory_footprint.len())..];
            let name_utf16 = get_null_terminated_utf16le_slice(name_utf16)?;

            let conv = MraConversation {
                offset: current_offset,
                name: WStr::from_utf16le(name_utf16)?,
                msg_id1: u32_ptr_to_option(read_u32_le(&dbs_bytes, current_offset + MESSAGE_IDS_OFFSET)),
                msg_id2: u32_ptr_to_option(read_u32_le(&dbs_bytes, current_offset + MESSAGE_IDS_OFFSET + 4)),
            };

            let bytes = &dbs_bytes[current_offset..(current_offset + MRAHISTORY_FOOTPRINT_OFFSET)];
            let bytes_str = bytes_to_pretty_string(bytes, 99999);
            log::debug!("{} - {:#010x}, {}", bytes_str, current_offset, conv.name.to_utf8());
            // log::debug!("mail_data at offset {:#010x}: Conversation {id} named {}", current_offset, conv.name.to_utf8());
            result.push(conv);
        } else {
            log::debug!("mail_data at offset {:#010x}: Skipping as it doesn't seem to be message related", current_offset);
        }

        actual_convs_count += 1;
        last_processed_conv_id = conv_id;
        conv_id = next_conv_id;
    }

    require!(actual_convs_count == expected_convs_count,
             "Expected to find {expected_convs_count} conversations, but {actual_convs_count} were found!");

    Ok(result)
}

fn load_messages<'a>(
    dbs_bytes: &'a [u8],
    offsets_table: &[u32],
    convs: Vec<MraConversation<'a>>,
) -> Result<Vec<MraConversationWithMessages<'a>>> {
    let mut result = vec![];
    for conv in convs {
        let mut msg_id_option = conv.msg_id1;
        let mut msgs = vec![];
        while let Some(msg_id) = msg_id_option {
            let header_offset = offsets_table[msg_id as usize] as usize;
            let header = {
                let header_slice = &dbs_bytes[header_offset..];
                let header_ptr = header_slice.as_ptr() as *const MraMessageHeader;
                // This is inherently unsafe. The only thing we can do is to check a magic number right after.
                let header = unsafe { header_ptr.as_ref::<'a>().unwrap() };
                require!(header.magic_number == MSG_HEADER_MAGIC_NUMBER,
                     "Incorrect header at offset {header_offset} (msg_id == {msg_id})!");
                header
            };
            let author_offset = header_offset + mem::size_of::<MraMessageHeader>();
            let author_slice = &dbs_bytes[author_offset..];
            let author_utf16 = get_null_terminated_utf16le_slice(author_slice)?;
            let author = WStr::from_utf16le(author_utf16)?;

            let text_offset = author_offset + header.nickname_length as usize * 2;
            let text_slice = &dbs_bytes[text_offset..];

            // I don't have SMS messages to check whether this is needed, or is done correctly, leaving this as a
            // leftover.
            //
            // if text_slice[0] == 0 && header.tpe_u32 == MraMessageType::Sms as u32 {
            //     // Original code did: header->count_message = ((*(text + 1)) / sizeof(char16_t)) + 1;
            //     text_slice = &text_slice[3..];
            // }

            let text_utf16 = get_null_terminated_utf16le_slice(text_slice)?;
            let text = WStr::from_utf16le(text_utf16)?;

            let payload_offset = text_offset + 2 * header.text_length as usize;
            let payload = &dbs_bytes[payload_offset..(header_offset + header.size as usize)];

            // WriteFile(hTmpFile, str, (header->count_message - 1) * sizeof(char16_t), &len, NULL); //пишем сообщение
            // text += mes->count_message; // теперь указатель показывает на LSP RTF, но оно нам не надо :)

            // TODO: RTF messages?

            let mra_msg = MraMessage { offset: header_offset, header, text, author, payload };
            msgs.push(mra_msg);

            msg_id_option = u32_ptr_to_option(header.prev_id);
        }
        result.push(MraConversationWithMessages { conv, msgs });
    }

    Ok(result)
}

fn convert<'a>(
    convs_with_msgs: &[MraConversationWithMessages<'a>],
    dbs_bytes: &'a [u8],
    storage_path: &Path,
) -> Result<Vec<DatasetEntry>> {
    lazy_static! {
        // Expected entries are @mail.ru, @bk.ru and @uin.icq.
        // Could also be @chat.agent, which indicates a group chat.
        static ref EMAIL_REGEX: Regex = Regex::new(r"^[a-zA-Z0-9._-]+@([a-z-]+\.)+[a-z]+$").unwrap();
    }

    let mut result = HashMap::<String, DatasetEntry>::new();

    let mut self_name_set = false;
    for conv_w_msgs in convs_with_msgs.iter() {
        if conv_w_msgs.msgs.is_empty() {
            log::debug!("Skipping conversation {} with no messages", conv_w_msgs.conv.name.to_utf8());
            continue;
        }

        let name = conv_w_msgs.conv.name.to_utf8();
        let name_slit = name.splitn(2, '_').map(|s| s.to_owned()).collect_vec();

        let conv_owner_email =
            name_slit.get(0)
                .with_context(|| format!("Couldn't get owner from conversation name {name}"))?
                .clone();

        let entry = result.entry(conv_owner_email.clone()).or_insert_with(|| {
            let ds_uuid = PbUuid::random();
            let myself = User {
                ds_uuid: Some(ds_uuid.clone()),
                id: *MYSELF_ID,
                first_name_option: None,
                last_name_option: None,
                username_option: Some(conv_owner_email.clone()),
                phone_number_option: None,
            };
            let ds_root = storage_path.join(DATASETS_DIR_NAME).join(conv_owner_email.as_str());
            fs::create_dir_all(&ds_root).unwrap();
            DatasetEntry {
                ds: Dataset { uuid: Some(ds_uuid), alias: conv_owner_email.clone() },
                ds_root,
                myself_id: MYSELF_ID,
                users: vec![myself],
                cwms: vec![],
            }
        });

        // FIXME: Use proper persistent ID!
        // FIXME: Sometimes user is already present!
        let mut user = User {
            ds_uuid: entry.ds.uuid.clone(),
            id: conv_w_msgs.conv.offset as i64,
            first_name_option: None,
            last_name_option: None,
            username_option: name_slit.get(1).cloned(),
            phone_number_option: None,
        };


        let mut internal_id = 0;

        let mut msgs = vec![];
        let mut user_nickname_set = user.username_option.is_some();
        let mut user_name_set = user.first_name_option.is_some();
        for mra_msg in conv_w_msgs.msgs.iter() {
            let timestamp = filetime_to_timestamp(mra_msg.header.time);

            let from_id = if mra_msg.header.is_outgoing()? { MYSELF_ID } else { user.id() };

            // Dealing with names
            let author = mra_msg.author.to_utf8();
            if mra_msg.header.is_outgoing()? {
                if !self_name_set && !EMAIL_REGEX.is_match(&author) {
                    entry.users.first_mut().unwrap().first_name_option = Some(author);
                    self_name_set = true;
                }
            } else {
                if !user_name_set && !EMAIL_REGEX.is_match(&author) {
                    user.first_name_option = Some(author);
                    user_name_set = true;
                } else if !user_nickname_set && EMAIL_REGEX.is_match(&author) {
                    user.username_option = Some(author);
                    user_nickname_set = true;
                }
            }

            // For a source message ID, let's use message time as it's precise enough for us to expect it to be unique
            // within a chat.
            let source_id_option = Some((mra_msg.header.time / 2) as i64);

            if user_nickname_set {}

            let tpe = mra_msg.header.get_tpe()?;

            let text = mra_msg.text.to_utf8();
            use crate::protobuf::history::content::SealedValueOptional;
            let (text, typed) = match tpe {
                MraMessageType::AuthorizationRequest |
                MraMessageType::RegularMaybeUnauthorized |
                MraMessageType::Regular |
                MraMessageType::Sms => {
                    (vec![RichText::make_plain(text)],
                     message::Typed::Regular(Default::default()))
                }
                MraMessageType::FileTransfer => {
                    // We can get file names from the outgoing messages.
                    // Mail.Ru allowed to send several files in one message, so we unite them here.
                    let text_parts = text.split('\n').collect_vec();
                    let file_name_option = if text_parts.len() >= 3 {
                        let file_paths: Vec<&str> = text_parts.smart_slice(1..-1).iter().map(|&s|
                            s.trim()
                                .rsplitn(3, ' ')
                                .skip(2)
                                .next()
                                .context("Unexpected file path format!"))
                            .try_collect()?;
                        Some(file_paths.iter().join(", "))
                    } else {
                        None
                    };
                    (vec![RichText::make_plain("<FileTransfer>".to_owned())],
                     message::Typed::Regular(MessageRegular {
                         content_option: Some(Content {
                             sealed_value_optional: Some(SealedValueOptional::File(ContentFile {
                                 path_option: None,
                                 file_name_option,
                                 mime_type_option: None,
                                 thumbnail_path_option: None,
                             }))
                         }),
                         ..Default::default()
                     }))
                }
                MraMessageType::Call |
                MraMessageType::VideoCall => {
                    // FIXME
                    (vec![RichText::make_plain("<Video/Call>".to_owned())],
                     message::Typed::Regular(Default::default()))
                }
                MraMessageType::BirthdayReminder => {
                    // FIXME
                    (vec![RichText::make_plain("<BirthdayReminder>".to_owned())],
                     message::Typed::Regular(Default::default()))
                }
                MraMessageType::Cartoon => {
                    // FIXME
                    (vec![RichText::make_plain("<Cartoon>".to_owned())],
                     message::Typed::Regular(Default::default()))
                }
                MraMessageType::ConferenceUsersChange => {
                    // FIXME
                    (vec![RichText::make_plain("<ConferenceUsersChange>".to_owned())],
                     message::Typed::Regular(Default::default()))
                }
                MraMessageType::MicroblogRecordType1 |
                MraMessageType::MicroblogRecordType2 => {
                    // FIXME
                    (vec![RichText::make_plain("<MicroblogRecord>".to_owned())],
                     message::Typed::Regular(Default::default()))
                }
                MraMessageType::ConferenceMessageType1 |
                MraMessageType::ConferenceMessageType2 => {
                    // FIXME
                    (vec![RichText::make_plain("<ConferenceMessage>".to_owned())],
                     message::Typed::Regular(Default::default()))
                }
                MraMessageType::Unknown1 => {
                    // FIXME
                    (vec![RichText::make_plain("<Unknown1>".to_owned())],
                     message::Typed::Regular(Default::default()))
                }
                MraMessageType::LocationChange => {
                    // FIXME
                    (vec![RichText::make_plain("<LocationChange>".to_owned())],
                     message::Typed::Regular(Default::default()))
                }
            };

            let msg = Message::new(
                internal_id,
                source_id_option,
                timestamp,
                from_id,
                text,
                typed,
            );
            msgs.push(msg);
            internal_id += 1;
        }

        entry.cwms.push(ChatWithMessages {
            chat: Some(Chat {
                ds_uuid: entry.ds.uuid.clone(),
                id: user.id,
                name_option: user.pretty_name_option(),
                source_type: SourceType::Mra as i32,
                tpe: ChatType::Personal as i32, // FIXME
                img_path_option: None,
                member_ids: vec![*MYSELF_ID, user.id],// FIXME
                msg_count: msgs.len() as i32,
                main_chat_id: None,
            }),
            messages: msgs,
        });
        entry.users.push(user);
    }

    Ok(result.into_values().collect_vec())
}

//
// Structs and enums
//

struct MraConversation<'a> {
    /// Offset at which data begins
    offset: usize,
    name: &'a WStr<LE>,
    /// Point to offset table data
    msg_id1: Option<u32>,
    /// Point to offset table data
    msg_id2: Option<u32>,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromPrimitive)]
enum MraMessageType {
    /// Not sure if that's actually unauthorized, some messages seem to have it out of the blue
    RegularMaybeUnauthorized = 0x02,
    AuthorizationRequest = 0x04,
    Regular = 0x07,
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
    MicroblogRecordType1 = 0x23,
    ConferenceMessageType1 = 0x24,
    // No idea what's the difference between them, TODO: Look into it!
    ConferenceMessageType2 = 0x25,
    Unknown1 = 0x27,
    // No idea what's the difference between them, TODO: Look into it!
    MicroblogRecordType2 = 0x29,
    LocationChange = 0x2E,
}

#[repr(C, packed)]
#[derive(Debug)]
struct MraMessageHeader {
    /// Total message size in bytes, including the header itself
    size: u32,
    prev_id: u32,
    next_id: u32,
    _unknown1: u32,
    /// WinApi FILETIME
    time: u64,
    /// Known variants are listed in MraMessageType
    tpe_u32: u32,
    flag_outgoing: u8,
    _unknown2: [u8; 3],
    /// In UTF-16 characters, not bytes, includes terminating zero
    nickname_length: u32,
    /// Matches MSG_HEADER_MAGIC_NUMBER
    magic_number: u32,
    /// In UTF-16 characters, not bytes, includes terminating zero
    text_length: u32,
    _unknown3: u32,
    // Byte
    size_lps_rtf: u32,
    _unknown4: u32,
}

impl MraMessageHeader {
    fn get_tpe(&self) -> Result<MraMessageType> {
        let tpe_u32 = self.tpe_u32;
        FromPrimitive::from_u32(tpe_u32).with_context(|| format!("Unknown message type: {}", tpe_u32))
    }

    fn is_outgoing(&self) -> Result<bool> {
        match self.flag_outgoing {
            0 => Ok(false),
            1 => Ok(true),
            x => err!("Invalid flag_incoming value {x}"),
        }
    }
}

struct MraMessage<'a> {
    /// Offset at which header begins
    offset: usize,
    header: &'a MraMessageHeader,
    text: &'a WStr<LE>,
    author: &'a WStr<LE>,
    /// Exact interpretation depends on the message type
    payload: &'a [u8],
}

impl Debug for MraMessage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let mut formatter = formatter.debug_struct("MraMessage");
        formatter.field("offset", &format!("{:#010x}", self.offset));
        match self.header.get_tpe() {
            Ok(tpe) =>
                formatter.field("type", &tpe),
            Err(_) => {
                let tpe_u32 = self.header.tpe_u32;
                formatter.field("type", &format!("UNKNOWN ({tpe_u32})"))
            }
        };
        formatter.field("author", &self.author.to_utf8());
        formatter.field("text", &self.text.to_utf8());
        formatter.field("payload", &bytes_to_pretty_string(self.payload, usize::MAX));
        formatter.finish()
    }
}

impl MraMessage<'_> {
    pub fn debug_format_bytes(&self, file_bytes: &[u8]) -> String {
        const COLUMNS: usize = 32;
        const ROWS_TO_TAKE: usize = 10;
        let upper_bound = self.offset + ROWS_TO_TAKE * COLUMNS;
        let upper_bound = cmp::min(upper_bound, file_bytes.len());
        bytes_to_pretty_string(&file_bytes[self.offset..upper_bound], COLUMNS)
    }
}

struct MraConversationWithMessages<'a> {
    conv: MraConversation<'a>,
    msgs: Vec<MraMessage<'a>>,
}

//
// Helper functions
//

fn read_hash_table_bytes(bytes: &[u8], shift: usize) -> Vec<u32> {
    let mut res = vec![];
    let mut i = 0;
    loop {
        let v = read_u32_le(bytes, shift + i * 4);
        if v == 4 {
            break res;
        }
        res.push(v);
        i += 1;
    }
}

fn read_chunk_le(bytes: &[u8], shift: usize, count: usize) -> Vec<u32> {
    let mut res = Vec::with_capacity(count);
    for i in 0..count {
        res.push(read_u32_le(bytes, shift + i * 4));
    }
    res
}

fn read_u32_le(bytes: &[u8], shift: usize) -> u32 {
    u32::from_le_bytes(read_4_bytes(bytes, shift))
}

fn read_4_bytes(bytes: &[u8], shift: usize) -> [u8; 4] {
    bytes[shift..(shift + 4)].try_into().unwrap()
}

fn u32_ptr_to_option(int: u32) -> Option<u32> {
    match int {
        0 => None,
        x => Some(x)
    }
}

fn filetime_to_timestamp(ft: u64) -> i64 {
    // FIXME: Timezone seems off?
    // WinApi FILETIME epoch starts 1601-01-01T00:00:00Z, which is 11644473600 seconds before the
    // UNIX/Linux epoch (1970-01-01T00:00:00Z). FILETIME ticks are also in in 100 nanoseconds.
    const TICKS_PER_SECOND: u64 = 10_000_000;
    const SECONSDS_TO_UNIX_EPOCH: i64 = 11_644_473_600;
    let time = ft / TICKS_PER_SECOND;
    let time = time as i64 - SECONSDS_TO_UNIX_EPOCH;
    time
}

/// Efficiently find all indexes of the given sequence occurrence within a longer source sequence.
/// Does not return indexes that overlap matches found earlier.
/// Works in O(n) of the source length, assuming to_find length to be negligible and not accounting for degenerate
/// input cases.
fn find_positions<T: PartialEq>(source: &[T], to_find: &[T]) -> Vec<usize> {
    inner_find_positions_of(source, to_find, false)
}

fn find_first_position<T: PartialEq>(source: &[T], to_find: &[T]) -> Option<usize> {
    inner_find_positions_of(source, to_find, true).first().cloned()
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
                result.push_str(&format!("{b:02x}"));
            }
            result.push(' ');
        }
        result.push('\n');
    }
    result.trim_end().to_owned()
}

fn inner_find_positions_of<T: PartialEq>(source: &[T], to_find: &[T], find_one: bool) -> Vec<usize> {
    if to_find.len() == 0 { panic!("to_find slice was empty!"); }
    let max_i = source.len() as i64 - to_find.len() as i64 + 1;
    if max_i <= 0 { return vec![]; }
    let max_i = max_i as usize;
    let mut res = vec![];
    let mut i = 0_usize;
    'outer: while i < max_i {
        for j in 0..to_find.len() {
            if source[i + j] != to_find[j] {
                i += 1;
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
