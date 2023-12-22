use std::{cmp, fmt, fs, mem, slice};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::PathBuf;

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

        let len = read_u32_le(&dbs_bytes, current_offset) as usize;
        let prev_conv_id = read_u32_le(&dbs_bytes, current_offset + CONVERSATION_IDS_OFFSET);
        let next_conv_id = read_u32_le(&dbs_bytes, current_offset + CONVERSATION_IDS_OFFSET + 4);

        require!(prev_conv_id == last_processed_conv_id,
                 "Conversations linked list is broken!");

        let mrahistory_loc = current_offset + MRAHISTORY_FOOTPRINT_OFFSET;
        if &dbs_bytes[mrahistory_loc..][0..mrahistory_footprint.len()] == mrahistory_footprint {
            // Setting the pointer right after the "mrahistory_"
            let name_slice = &dbs_bytes[(mrahistory_loc + mrahistory_footprint.len())..];
            let separator_pos = {
                // Names are separated by either zero char (0x0000) or an underscore (0x5F00)
                let zero_byte_pos = find_first_position(name_slice, &[0x00, 0x00], 2);
                let underscore_pos = find_first_position(name_slice, &[0x5F, 0x00], 2);
                [zero_byte_pos, underscore_pos].into_iter().filter_map(|v| v).min().unwrap()
            };
            let myself_name_utf16 = &name_slice[..separator_pos];

            // Just zero char this time
            let name_slice = &name_slice[(separator_pos + 2)..];
            let separator_pos = find_first_position(name_slice, &[0x00, 0x00], 2).unwrap();
            let other_name_utf16 = &name_slice[..separator_pos];

            let conv = MraConversation {
                offset: current_offset,
                myself_name: WStr::from_utf16le(myself_name_utf16)?,
                other_name: WStr::from_utf16le(other_name_utf16)?,
                msg_id1: u32_ptr_to_option(read_u32_le(&dbs_bytes, current_offset + MESSAGE_IDS_OFFSET)),
                msg_id2: u32_ptr_to_option(read_u32_le(&dbs_bytes, current_offset + MESSAGE_IDS_OFFSET + 4)),
                raw: &dbs_bytes[current_offset..(current_offset + len)],
            };

            log::debug!("mail_data at offset {:#010x}: Conversation between {} and {}",
                        current_offset, conv.myself_name.to_utf8(), conv.other_name.to_utf8());
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

            let mra_msg = MraMessage { offset: header_offset, header, text, author, payload_offset, payload };
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

    let mut result = HashMap::<String, MraDatasetEntry>::new();

    for conv_w_msgs in convs_with_msgs.iter() {
        let myself_username = conv_w_msgs.conv.myself_name.to_utf8();
        let conv_username = conv_w_msgs.conv.other_name.to_utf8();

        if conv_w_msgs.msgs.is_empty() {
            log::debug!("Skipping conversation between {} and {} with no messages", myself_username, conv_username);
            continue;
        }

        let entry = result.entry(myself_username.clone()).or_insert_with(|| {
            let ds_uuid = PbUuid::random();
            let myself = User {
                ds_uuid: Some(ds_uuid.clone()),
                id: *MYSELF_ID,
                first_name_option: None,
                last_name_option: None,
                username_option: Some(myself_username.clone()),
                phone_number_option: None,
            };
            let ds_root = storage_path.join(DATASETS_DIR_NAME).join(myself_username.as_str());
            fs::create_dir_all(&ds_root).unwrap();
            MraDatasetEntry {
                ds: Dataset { uuid: Some(ds_uuid), alias: myself_username.clone() },
                ds_root,
                users: HashMap::from([(myself_username.clone(), myself)]),
                cwms: HashMap::new(),
            }
        });

        let mut internal_id = 0;

        let mut msgs: Vec<Message> = vec![];
        let mut ongoing_call_msg_id = None;
        let mut interlocutor_ids = HashSet::from([MYSELF_ID]);
        for mra_msg in conv_w_msgs.msgs.iter() {
            let timestamp = filetime_to_timestamp(mra_msg.header.time);

            // For a source message ID, let's use message time as it's precise enough for us to expect it to be unique
            // within a chat.
            let source_id_option = Some((mra_msg.header.time / 2) as i64);

            let tpe = mra_msg.header.get_tpe()?;

            let text = mra_msg.text.to_utf8();
            use crate::protobuf::history::message::Typed;
            use crate::protobuf::history::content::SealedValueOptional as ContentSvo;
            use crate::protobuf::history::message_service::SealedValueOptional as ServiceSvo;
            let (text, typed) = match tpe {
                MraMessageType::AuthorizationRequest |
                MraMessageType::RegularMaybeUnauthorized |
                MraMessageType::Regular |
                MraMessageType::Sms => {
                    (vec![RichText::make_plain(text)],
                     Typed::Regular(Default::default()))
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
                    (vec![],
                     Typed::Regular(MessageRegular {
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
                MraMessageType::Call |
                MraMessageType::VideoCall => {
                    // Payload format: <text_len_u32><text>
                    // It does not carry call information per se.
                    let payload = mra_msg.payload;
                    let payload = validate_skip_bytes(payload, mra_msg.text.as_bytes())?;
                    require!(payload.is_empty(), "Unexpected {:?} message payload format!", tpe);

                    let text = mra_msg.text.to_utf8();

                    const BEGIN_CONNECTING: &str = "Устанавливается соединение...";
                    const BEGIN_I_CALL: &str = "Звонок от вашего собеседника";
                    const BEGIN_O_CALL: &str = "Вы звоните собеседнику. Ожидание ответа...";
                    const BEGIN_STARTED: &str = "Начался разговор";

                    const END_HANG: &str = "Звонок завершен";
                    const END_VHANG: &str = "Видеозвонок завершен";
                    const END_CONN_FAILED: &str = "Не удалось установить соединение. Попробуйте позже.";
                    const END_I_CANCELLED: &str = "Вы отменили звонок";
                    const END_I_VCANCELLED: &str = "Вы отменили видеозвонок";
                    const END_O_CANCELLED: &str = "Собеседник отменил звонок";
                    const END_O_VCANCELLED: &str = "Собеседник отменил видеозвонок";

                    // MRA is not very rigid in propagating all the statuses.
                    match text.as_str() {
                        BEGIN_CONNECTING | BEGIN_I_CALL | BEGIN_O_CALL | BEGIN_STARTED => {
                            if ongoing_call_msg_id.is_some_and(|id| internal_id - id <= 5) {
                                // If call is already (recently) marked, do nothing
                                continue;
                            } else {
                                // Save call ID to later amend with duration and status.
                                ongoing_call_msg_id = Some(internal_id);
                            }
                        }
                        END_HANG | END_VHANG |
                        END_CONN_FAILED |
                        END_I_CANCELLED | END_I_VCANCELLED |
                        END_O_CANCELLED | END_O_VCANCELLED => {
                            if ongoing_call_msg_id.is_some_and(|id| internal_id - id <= 50) {
                                let msg_id = ongoing_call_msg_id.unwrap();
                                let msg = msgs.iter_mut().rfind(|m| m.internal_id == msg_id).unwrap();
                                let start_time = msg.timestamp;
                                let discard_reason_option = match text.as_str() {
                                    END_HANG | END_VHANG => None,
                                    END_CONN_FAILED => Some("Failed to connect"),
                                    END_I_CANCELLED | END_I_VCANCELLED => Some("Declined by you"),
                                    END_O_CANCELLED | END_O_VCANCELLED => Some("Declined by user"),
                                    _ => unreachable!()
                                };
                                match msg.typed_mut() {
                                    Typed::Service(MessageService { sealed_value_optional: Some(ServiceSvo::PhoneCall(call)), .. }) => {
                                        call.duration_sec_option = Some((timestamp - start_time) as i32);
                                        call.discard_reason_option = discard_reason_option.map(|s| s.to_owned());
                                    }
                                    etc => bail!("Unexpected ongoing call type: {etc:?}")
                                };
                                ongoing_call_msg_id = None;
                            }
                            // Either way, this message itself isn't supposed to have a separate entry.
                            continue;
                        }
                        etc => bail!("Unrecognized call message: {etc}"),
                    }

                    (vec![],
                     Typed::Service(MessageService {
                         sealed_value_optional: Some(ServiceSvo::PhoneCall(MessageServicePhoneCall {
                             duration_sec_option: None,
                             discard_reason_option: None,
                         }))
                     }))
                }
                MraMessageType::BirthdayReminder => {
                    // FIXME
                    (vec![RichText::make_plain("<BirthdayReminder>".to_owned())],
                     Typed::Regular(Default::default()))
                }
                MraMessageType::Cartoon => {
                    // FIXME
                    (vec![RichText::make_plain("<Cartoon>".to_owned())],
                     Typed::Regular(Default::default()))
                }
                MraMessageType::ConferenceUsersChange => {
                    // FIXME
                    (vec![RichText::make_plain("<ConferenceUsersChange>".to_owned())],
                     Typed::Regular(Default::default()))
                }
                MraMessageType::MicroblogRecordType1 |
                MraMessageType::MicroblogRecordType2 => {
                    // FIXME
                    (vec![RichText::make_plain("<MicroblogRecord>".to_owned())],
                     Typed::Regular(Default::default()))
                }
                MraMessageType::ConferenceMessageType1 |
                MraMessageType::ConferenceMessageType2 => {
                    // FIXME
                    (vec![RichText::make_plain("<ConferenceMessage>".to_owned())],
                     Typed::Regular(Default::default()))
                }
                MraMessageType::Unknown1 => {
                    // FIXME
                    (vec![RichText::make_plain("<Unknown1>".to_owned())],
                     Typed::Regular(Default::default()))
                }
                MraMessageType::LocationChange => {
                    // Payload format: <name_len_u32><name><lat_len_u32><lat><lon_len_u32><lon><...>
                    let payload = mra_msg.payload;
                    // We observe that location name is exactly the same as the message text
                    let payload = validate_skip_bytes(payload, mra_msg.text.as_bytes())?;
                    // Lattitude
                    let lat_len = read_u32_le(payload, 0) as usize;
                    let payload = &payload[4..];
                    let lat_str = String::from_utf8(payload[..lat_len].to_vec())?;
                    let payload = &payload[lat_len..];
                    // Longitude
                    let lon_len = read_u32_le(payload, 0) as usize;
                    let payload = &payload[4..];
                    let lon_str = String::from_utf8(payload[..lon_len].to_vec())?;

                    let location = ContentLocation {
                        title_option: None,
                        address_option: Some(mra_msg.text.to_utf8()),
                        lat_str: lat_str,
                        lon_str: lon_str,
                        duration_sec_option: None,
                    };
                    (vec![RichText::make_plain("(Location changed)".to_owned())],
                     Typed::Regular(MessageRegular {
                         content_option: Some(Content {
                             sealed_value_optional: Some(ContentSvo::Location(location))
                         }),
                         ..Default::default()
                     }))
                }
            };

            let from_username = if mra_msg.header.is_outgoing()? { &myself_username } else { &conv_username };
            let user = entry.users.entry(from_username.clone()).or_insert_with(|| User {
                ds_uuid: entry.ds.uuid.clone(),
                id: hash_to_id(from_username),
                first_name_option: None,
                last_name_option: None,
                username_option: Some(from_username.clone()),
                phone_number_option: None,
            });

            if user.first_name_option.is_none() {
                let author = mra_msg.author.to_utf8();
                if !EMAIL_REGEX.is_match(&author) {
                    user.first_name_option = Some(author);
                }
            }
            interlocutor_ids.insert(user.id());

            let msg = Message::new(
                internal_id,
                source_id_option,
                timestamp,
                user.id(),
                text,
                typed,
            );
            msgs.push(msg);
            internal_id += 1;
        }

        let member_ids = interlocutor_ids
            .into_iter()
            .map(|id| *id)
            .sorted_by_key(|id| if *id == *MYSELF_ID { i64::MIN } else { *id })
            .collect_vec();

        entry.cwms.insert(conv_username.clone(), ChatWithMessages {
            chat: Some(Chat {
                ds_uuid: entry.ds.uuid.clone(),
                id: hash_to_id(&conv_username),
                name_option: Some(conv_username), // FIXME
                source_type: SourceType::Mra as i32,
                tpe: ChatType::Personal as i32, // FIXME
                img_path_option: None,
                member_ids,
                msg_count: msgs.len() as i32,
                main_chat_id: None,
            }),
            messages: msgs,
        });
    }

    Ok(result.into_values().map(|entry| DatasetEntry {
        ds: entry.ds,
        ds_root: entry.ds_root,
        myself_id: MYSELF_ID,
        users: entry.users.into_values()
            .sorted_by_key(|u| if u.id() == MYSELF_ID { i64::MIN } else { u.id })
            .collect_vec(),
        cwms: entry.cwms.into_values().collect_vec(),
    }).collect_vec())
}

//
// Structs and enums
//

struct MraConversation<'a> {
    /// Offset at which data begins
    offset: usize,
    myself_name: &'a WStr<LE>,
    other_name: &'a WStr<LE>,
    /// Point to offset table data
    msg_id1: Option<u32>,
    /// Point to offset table data
    msg_id2: Option<u32>,
    /// Raw bytes for the conversation record
    raw: &'a [u8],
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
    payload_offset: usize,
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
        formatter.field("payload_offset", &format!("{:#010x}", self.payload_offset));
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

struct MraDatasetEntry {
    ds: Dataset,
    ds_root: PathBuf,
    /// Key is username (in most cases, email)
    users: HashMap<String, User>,
    /// Key is conversation name (in most cases, email or email-like name)
    cwms: HashMap<String, ChatWithMessages>,
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

/// In the next <N_u32><...N bytes...> validate that N bytes correspond to the expected bytes provided
fn validate_skip_bytes<'a>(payload: &'a [u8], expected_bytes: &[u8]) -> Result<&'a [u8]> {
    let len = read_u32_le(payload, 0) as usize;
    require!(len == expected_bytes.len(),
             "Unexpected message payload format!");
    let payload = &payload[4..];
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
fn find_positions<T: PartialEq>(source: &[T], to_find: &[T], step: usize) -> Vec<usize> {
    inner_find_positions_of(source, to_find, step, false)
}

fn find_first_position<T: PartialEq>(source: &[T], to_find: &[T], step: usize) -> Option<usize> {
    inner_find_positions_of(source, to_find, step, true).first().cloned()
}

fn get_null_terminated_utf8_slice(bs: &[u8]) -> Result<&[u8]> {
    static NULL_UTF8: u8 = 0x00;

    let null_term_idx = bs.iter()
        .position(|&bs| bs == NULL_UTF8)
        .context("Null terminator not found!")?;

    Ok(&bs[..null_term_idx])
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

fn inner_find_positions_of<T: PartialEq>(source: &[T], to_find: &[T], step: usize, find_one: bool) -> Vec<usize> {
    assert!(to_find.len() % step == 0, "to_find sequence length is not a multiplier of {step}!");
    if to_find.len() == 0 { panic!("to_find slice was empty!"); }
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
