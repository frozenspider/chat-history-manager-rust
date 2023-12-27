/// For strings, this format uses UTF-8 and UTF-16 LE.

use std::cell::RefCell;
use std::fmt::Formatter;

use super::*;

const MSG_HEADER_MAGIC_NUMBER: u32 = 0x2D;

pub(super) fn do_the_thing(path: &Path) -> EmptyRes {
    for dir_entry in fs::read_dir(path)? {
        let dir_entry = dir_entry?;
        let meta = dir_entry.metadata()?;
        let path = dir_entry.path();
        let name = path_file_name(&path)?;
        if meta.is_dir() {
            process_account(name, &path)?;
        } else {
            log::warn!("{} is not a directory, ignored", name);
        }
    }
    Ok(())
}

fn process_account(my_account_name: &str, path: &Path) -> EmptyRes {
    for db_file in list_all_files(path, false)?
        .into_iter()
        .filter(|p| p.extension().and_then(|s| s.to_str()).is_some_and(|s| s == "db"))
    {
        let user_account_name = path_file_name(&db_file)?.smart_slice(..-3).to_owned();
        // if user_account_name == "unreads" { continue; }
        // if user_account_name.starts_with("unreads") {
        let index_file = db_file.parent().unwrap().join(format!("{user_account_name}.index"));
        require!(index_file.exists(), "Index file for {user_account_name} does not exist!");
        process_conversation(&db_file, &index_file, my_account_name, &user_account_name)?;
        // }
    }
    Ok(())
}

fn process_conversation(
    db_file: &Path,
    index_file: &Path,
    my_account_name: &str,
    user_account_name: &str,
) -> EmptyRes {
    println!("{}", user_account_name);

    // Read whole files into the memory.
    let db_bytes = fs::read(db_file)?;
    let index_bytes = fs::read(index_file)?;

    let mut slice = db_bytes.as_slice();
    let mut offset = 0;
    while !slice.is_empty() {
        let (message_bytes, rest_bytes) = next_sized_chunk(slice)?;
        process_message(message_bytes, offset, my_account_name, user_account_name)?;
        let (message_len_again, rest_bytes) = next_u32_size(rest_bytes);
        require!(message_len_again == message_bytes.len(),
                 "Message was not followed by duplicated length!\nMessage: TODO");
        offset += message_bytes.len() + 8;
        slice = rest_bytes;
    }

    Ok(())
}

fn process_message<'a>(
    bytes: &'a [u8],
    global_offset: usize,
    my_account_name: &str,
    user_account_name: &str,
) -> EmptyRes {
    let bytes = {
        let (wrapped_bytes, remaining_bytes) = next_sized_chunk(bytes)?;
        require!(remaining_bytes.len() == 4);
        require!(read_u32(remaining_bytes, 0) as usize == wrapped_bytes.len());
        wrapped_bytes
    };

    // This is inherently unsafe. The only thing we can do is to check a magic number right after.
    let header = unsafe {
        let header_ptr = bytes.as_ptr() as *const DbMessageHeader;
        header_ptr.as_ref::<'a>().unwrap()
    };
    require!(header.magic_number == MSG_HEADER_MAGIC_NUMBER && header.magic_value_one == 1 && header.padding2 == 0,
             "Incorrect header for message at offset {global_offset:#010x}: {header:?}");

    let bytes = &bytes[mem::size_of::<DbMessageHeader>()..];
    let (payload, bytes) = next_sized_chunk(bytes)?;

    let db_msg = DbMessage { offset: global_offset, header, payload };

    process_message_payload(user_account_name, db_msg)?;

    require!(bytes.is_empty(),
             "Incorrect remainder for message at offset {global_offset:#010x}!");
    Ok(())
}

fn process_message_payload(
    user_account_name: &str,
    db_msg: DbMessage,
) -> EmptyRes {
    macro_rules! require_format {
        ($cond:expr, $msg:expr) => {
            require!($cond, "Unexpected message payload format: {}\nMessage: {db_msg:?}", $msg)
        };
    }
    // println!("{global_offset:#010x}, {header:?}, {payload:02X?}");
    let timestamp = match filetime_to_timestamp(db_msg.header.filetime) {
        0 => db_msg.header.some_timestamp_or_0 as i64,
        v => v
    };
    require_format!(timestamp != 0, "timestamp is not known");

    require_format!(db_msg.payload[0] == 1, "first byte of payload wasn't 0x01");

    let tpe = db_msg.get_tpe()?;

    // Not really sure what is the meaning of this, but empty messages can be identified by this signature.
    // They could have different "types", and this signature doesn't seem obviously meaningful for non-empty messages.
    if &db_msg.header._unknown1[3..=4] == &[0x4A, 0x00] {
        require_format!(db_msg.payload == vec![1, 0, 0, 0, 0], "unexpected empty message payload");
    } else {
        require_format!(db_msg.payload.len() > 13, "payload is too short");
        let (_unknown, mut payload) = next_n_bytes::<5>(db_msg.payload);

        let mut author_name: Option<String> = None;
        let mut plaintext: Option<String> = None;
        let mut my_account: Option<String> = None;
        let mut user_account: Option<String> = None;
        let mut rte: Option<String> = None;

        macro_rules! set_option {
            ($holder:ident, $new_value:expr) => {{
                let new_value = $new_value;
                if !new_value.is_empty() {
                    if let Some(ref old_value) = $holder {
                        require_format!(old_value == &new_value,
                                        format!("unexpected {} value: {old_value} vs {new_value}", stringify!($holder)));
                    } else {
                        $holder = Some(new_value)
                    }
                }
            }};
        }

        while !payload.is_empty() {
            payload = {
                let (section_type, payload) = next_u32(payload);
                let section_type: MessageSectionType = FromPrimitive::from_u32(section_type)
                    .with_context(|| format!("unknown message section: {section_type}"))?;
                // No matter what the section is, it's sized
                let (section, payload) = next_sized_chunk(payload)?;
                match section_type {
                    MessageSectionType::Plaintext => {
                        set_option!(plaintext, String::from_utf8(section.to_vec())?);
                    }
                    MessageSectionType::AuthorName => {
                        set_option!(author_name, String::from_utf8(section.to_vec())?);
                    }
                    MessageSectionType::OtherAccount2 => {
                        assert!(user_account_name == "unreads"); // FIXME
                        set_option!(user_account, String::from_utf8(section.to_vec())?)
                    }
                    MessageSectionType::MyAccount => {
                        set_option!(my_account, String::from_utf8(section.to_vec())?);
                    }
                    MessageSectionType::OtherAccount => {
                        set_option!(user_account, String::from_utf8(section.to_vec())?);
                    }
                    MessageSectionType::Content if tpe == DbMessageType::ConferenceUsersChange => {
                        let (subsection, rest) = next_u32(section);
                        assert!(subsection == 0x05); // FIXME
                        let (text_bytes, rest) = next_sized_chunk(rest)?;
                        let (text_bytes_2, rest) = next_sized_chunk(rest)?;
                        require_format!(rest.is_empty(), "unexpected conference user change content format");
                        require_format!(text_bytes == text_bytes_2, "unexpected conference user change content format");

                        let text_utf16 = WStr::from_utf16le(text_bytes)?;
                        let text_utf8 = text_utf16.to_utf8();
                        set_option!(user_account, text_utf8);
                    }
                    MessageSectionType::Content => {
                        let (text, rest) = {
                            let (text_bytes, rest) = next_sized_chunk(section)?;
                            let text_utf16 = WStr::from_utf16le(text_bytes)?;
                            (text_utf16.to_utf8(), rest)
                        };
                        match tpe {
                            DbMessageType::Plaintext |
                            DbMessageType::File |
                            DbMessageType::Call |
                            DbMessageType::Birthday |
                            DbMessageType::Cartoon |
                            DbMessageType::VCall => {
                                require_format!(rest.is_empty(), "unexpected plaintext text format");
                                // FIXME
                                set_option!(rte, text);
                            }
                            DbMessageType::Rtf => {
                                // Color followed by an optional unknown 4-bytes.
                                let (_color, rest) = next_u32(rest);
                                require_format!(rest.is_empty() || rest.len() == 4,
                                                format!("unexpected follow-up to UTF-16 text section: {rest:02X?}"));
                                set_option!(rte, text);
                            }
                            DbMessageType::MicroblogRecordBroadcast => {
                                // Color followed by an optional unknown 4-bytes.
                                let (_color, rest) = next_u32(rest);
                                require_format!(rest.is_empty() || rest.len() == 4,
                                                format!("unexpected follow-up to UTF-16 text section: {rest:02X?}"));
                                convert_microblog_record(&text, None);
                                set_option!(plaintext, text);
                            }
                            DbMessageType::MicroblogRecordDirected => {
                                let (target_name_bytes, rest) = next_sized_chunk(rest)?;
                                let target_name = WStr::from_utf16le(target_name_bytes)?;
                                let target_name = target_name.to_utf8();
                                require_format!(rest.len() == 8,
                                                format!("unexpected follow-up to directed microblog payload: {rest:02X?}"));
                                convert_microblog_record(&text, Some(&target_name));
                                set_option!(plaintext, text);
                            }
                            DbMessageType::ConferenceMessageRtf => {
                                let (_color, rest) = next_u32(rest);
                                // If no more bytes, author is self
                                if !rest.is_empty() {
                                    let (author_bytes, rest) = next_sized_chunk(rest)?;
                                    let author = String::from_utf8(author_bytes.to_vec())?;
                                    require_format!(rest.is_empty(), "");

                                    set_option!(user_account, author);
                                }
                                set_option!(rte, text);
                            }
                            DbMessageType::ConferenceUsersChange => {
                                println!("CCCC!!!!");
                                //
                            }
                            DbMessageType::AuthRequest => {
                                // Account (email) followed by message, both in UTF-16 LE
                                set_option!(user_account, text);
                                let (text_bytes, rest) = next_sized_chunk(rest)?;
                                set_option!(plaintext, WStr::from_utf16le(text_bytes)?.to_utf8());
                                require_format!(rest.is_empty(), "unexpected auth request text format");
                            }
                            DbMessageType::Location => {
                                // FIXME
                                rte = Some(text);
                            }
                            _ => require_format!(false, "text was not expected for this message type"),
                        }
                    }
                }
                payload
            };
        }

        println!("{my_account:?} {user_account:?} by {author_name:?} - {plaintext:?}, {rte:?}");
    }
    Ok(())
}

//
// Structs and enums
//

struct DbMessage<'a> {
    offset: usize,
    header: &'a DbMessageHeader,
    payload: &'a [u8],
}

impl DbMessage<'_> {
    fn get_tpe(&self) -> Result<DbMessageType> {
        let tpe_u8 = self.header.tpe_u8;
        FromPrimitive::from_u8(tpe_u8)
            .with_context(|| format!("Unknown message type: {:#04x}\nMessage hedaer: {:?}", tpe_u8, self))
    }
}

impl Debug for DbMessage<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let mut formatter = formatter.debug_struct("DbMessage");
        formatter.field("offset", &format!("{:#010x}", self.offset));
        let tpe_u8 = self.header.tpe_u8;
        let tpe_option: Option<DbMessageType> = FromPrimitive::from_u8(tpe_u8);
        match tpe_option {
            Some(tpe) =>
                formatter.field("type", &tpe),
            None => {
                formatter.field("type", &format!("UNKNOWN ({tpe_u8:#04x})"))
            }
        };
        formatter.field("header", &self.header);
        formatter.field("payload", &format!("{:02X?}", self.payload));
        formatter.finish()
    }
}

#[repr(C, packed)]
#[derive(Clone, PartialEq, Eq)]
struct DbMessageHeader {
    /// Matches MSG_HEADER_MAGIC_NUMBER
    magic_number: u32,
    /// == 1
    magic_value_one: u8,
    /// Known variants are listed in DbMessageType
    tpe_u8: u8,
    _unknown1: [u8; 11],
    /// WinApi FILETIME
    filetime: u64,
    _unknown2: [u8; 4],
    /// Might slightly differ from filetime
    some_timestamp_or_0: i32,
    padding2: u128,
}

impl Debug for DbMessageHeader {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let mut formatter = formatter.debug_struct("Header");
        let tpe_u8 = self.tpe_u8;
        formatter.field("type_u8", &tpe_u8);
        let unknown1 = self._unknown1.clone();
        formatter.field("_unknown1", &format!("{unknown1:02X?}"));
        let time = self.filetime;
        formatter.field("filetime", &time);
        let unknown2 = self._unknown2.clone();
        formatter.field("_unknown2", &format!("{unknown2:02X?}"));
        let some_timestamp_or_0 = self.some_timestamp_or_0;
        formatter.field("some_timestamp_or_0", &some_timestamp_or_0);
        formatter.finish()
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromPrimitive)]
enum DbMessageType {
    Empty = 0x00,
    Plaintext = 0x02,
    AuthRequest = 0x04,
    Rtf = 0x07,
    File = 0x0A,
    Call = 0x0C,
    Birthday = 0x0D,
    Cartoon = 0x1A,
    VCall = 0x1E,
    MicroblogRecordBroadcast = 0x23,
    ConferenceMessageRtf = 0x25,
    ConferenceUsersChange = 0x22,
    MicroblogRecordDirected = 0x29,
    Location = 0x2e, // FIXME
}


#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromPrimitive)]
enum MessageSectionType {
    Plaintext = 0x00,
    AuthorName = 0x02,
    OtherAccount2 = 0x03,
    MyAccount = 0x04,
    OtherAccount = 0x05,
    /// Exact content format depends on the message type
    Content = 0x06,
}
