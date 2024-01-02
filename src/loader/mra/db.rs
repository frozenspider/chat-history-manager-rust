/// For strings, this format uses UTF-8 and UTF-16 LE.

use std::cell::RefCell;
use std::fmt::Formatter;

use super::*;

const MSG_HEADER_MAGIC_NUMBER: u32 = 0x2D;

const FLAG_INCOMING: u8 = 0b100;

pub(super) fn do_the_thing(path: &Path, storage_path: &Path) -> EmptyRes {
    let mut dataset_map = HashMap::<String, MraDatasetEntry>::new();
    for dir_entry in fs::read_dir(path)? {
        let dir_entry = dir_entry?;
        let meta = dir_entry.metadata()?;
        let path = dir_entry.path();
        let name = path_file_name(&path)?;
        if meta.is_dir() {
            let ds_uuid = PbUuid::random();
            let users = process_account(name, &path, &ds_uuid)?;
            dataset_map.insert(name.to_owned(), MraDatasetEntry {
                ds: Dataset { uuid: Some(ds_uuid), alias: name.to_owned() },
                ds_root: storage_path.to_path_buf(),
                users,
                cwms: HashMap::new(),
            });
        } else {
            log::warn!("{} is not a directory, ignored", name);
        }
    }
    Ok(())
}

fn process_account(
    my_username: &str,
    path: &Path,
    ds_uuid: &PbUuid,
) -> Result<HashMap<String, User>> {
    let myself = User {
        ds_uuid: Some(ds_uuid.clone()),
        id: *MYSELF_ID,
        first_name_option: None,
        last_name_option: None,
        username_option: Some(my_username.to_owned()),
        phone_number_option: None,
    };

    // Read whole files into the memory
    let mut files_content: Vec<(String, Vec<u8>)> = vec![];
    for db_file in list_all_files(path, false)?
        .into_iter()
        .filter(|p| p.extension().and_then(|s| s.to_str()).is_some_and(|s| s == "db"))
    {
        let conv_name = path_file_name(&db_file)?.smart_slice(..-3).to_owned();

        let index_file = db_file.parent().unwrap().join(format!("{conv_name}.index"));
        require!(index_file.exists(), "Index file for {conv_name} does not exist!");

        let db_bytes = fs::read(db_file)?;
        let index_bytes = fs::read(index_file)?;

        files_content.push((conv_name, db_bytes));
    }
    let files_content = files_content;

    let db_msgs_map: HashMap<String, Vec<DbMessage>> =
        files_content
            .iter()
            .map(|(k, v)| get_conversation_messages(k, v).map(|v| (k.clone(), v)))
            .try_collect()?;

    let mut result = HashMap::from([(my_username.to_owned(), myself)]);
    for (conv_name, db_msgs) in db_msgs_map.iter() {
        collect_users(ds_uuid, my_username, conv_name, db_msgs, &mut result)?;
    }

    for (conv_name, _) in files_content.iter() {
        let db_msgs = &db_msgs_map[conv_name];
        process_conversation(db_msgs, my_username, &conv_name, &result)?;
    }
    Ok(result)
}

fn get_conversation_messages<'a>(conv_name: &str, db_bytes: &'a [u8]) -> Result<Vec<DbMessage<'a>>> {
    let mut result = vec![];
    let mut db_bytes = db_bytes;
    let mut offset = 0;
    while !db_bytes.is_empty() {
        let (message_bytes, rest_bytes) = next_sized_chunk(db_bytes)?;
        let offset_shift = message_bytes.len();
        let (message_len_again, rest_bytes) = next_u32_size(rest_bytes);
        require!(message_len_again == message_bytes.len(),
                 "Message was not followed by duplicated length!\nMessage bytes: {message_bytes:02X?}");

        let message_bytes = {
            let (wrapped_bytes, remaining_bytes) = next_sized_chunk(message_bytes)?;
            require!(remaining_bytes.len() == 4);
            require!(read_u32(remaining_bytes, 0) as usize == wrapped_bytes.len());
            wrapped_bytes
        };

        // This is inherently unsafe. The only thing we can do is to check a magic number right after.
        let header = unsafe {
            let header_ptr = message_bytes.as_ptr() as *const DbMessageHeader;
            header_ptr.as_ref::<'a>().unwrap()
        };
        require!(header.magic_number == MSG_HEADER_MAGIC_NUMBER && header.magic_value_one == 1 && header.padding2 == 0,
                 "Incorrect header for message at offset {offset:#010x}: {header:?}");

        let bytes = &message_bytes[mem::size_of::<DbMessageHeader>()..];
        let (payload, bytes) = next_sized_chunk(bytes)?;

        let mut mra_msg = DbMessage { offset, header, payload, sections: vec![] };
        require_format_clue(bytes.is_empty(), &mra_msg, conv_name, "incorrect remainder")?;

        // Not really sure what is the meaning of this, but empty messages can be identified by this signature.
        // They could have different "types", and this signature doesn't seem obviously meaningful for non-empty messages.
        if &mra_msg.header._unknown1[2..=3] == &[0x4A, 0x00] {
            require_format(mra_msg.payload == vec![1, 0, 0, 0, 0], &mra_msg, conv_name)?;
        } else {
            require_format_clue(mra_msg.payload.len() > 13, &mra_msg, conv_name, "payload is too short")?;
            let (_unknown, mut payload) = next_n_bytes::<5>(mra_msg.payload);

            // Getting sections out of payload
            while !payload.is_empty() {
                payload = {
                    let (section_type, payload) = next_u32(payload);
                    let section_type: MessageSectionType = FromPrimitive::from_u32(section_type)
                        .with_context(|| format!("unknown message section: {section_type}"))?;
                    // No matter what the section is, it's sized
                    let (section_bytes, payload) = next_sized_chunk(payload)?;
                    mra_msg.sections.push((section_type, section_bytes));
                    payload
                }
            }
        }

        result.push(mra_msg);

        offset += offset_shift + 8;
        db_bytes = rest_bytes;
    }
    Ok(result)
}

fn collect_users(
    ds_uuid: &PbUuid,
    my_username: &str,
    conv_name: &str,
    msgs: &[DbMessage],
    users: &mut HashMap<String, User>,
) -> EmptyRes {
    upsert_user(users, ds_uuid, my_username, None);
    upsert_user(users, ds_uuid, my_username, None);

    for mra_msg in msgs {
        if mra_msg.sections.is_empty() { continue; }

        macro_rules! set_option {
            ($holder:ident, $new_value:expr) => {{
                let new_value = $new_value;
                if !new_value.is_empty() {
                    if let Some(ref old_value) = $holder {
                        require_format_with_clue(
                            old_value == &new_value,
                            mra_msg,
                            conv_name,
                            || format!("unexpected {} value: {old_value} vs {new_value}", stringify!($holder)))?;
                    } else {
                        $holder = Some(new_value)
                    }
                }
            }};
        }

        let mut interlocutor_username: Option<String> = None;
        let mut author_name: Option<String> = None;
        let tpe = mra_msg.get_tpe()?;

        for (section_type, section) in mra_msg.sections.iter() {
            match section_type {
                MessageSectionType::AuthorName => {
                    set_option!(author_name, String::from_utf8(section.to_vec())?);
                }
                MessageSectionType::OtherAccount | MessageSectionType::OtherAccount2 => {
                    set_option!(interlocutor_username, String::from_utf8(section.to_vec())?);
                }
                MessageSectionType::Content if tpe == MraMessageType::ConferenceUsersChange =>
                    collect_users_from_conference_user_changed_record(
                        users, ds_uuid, conv_name, mra_msg, section)?,
                MessageSectionType::Content => {
                    let (text, rest) = {
                        let (text_bytes, rest) = next_sized_chunk(section)?;
                        let text_utf16 = WStr::from_utf16le(text_bytes).with_context(|| context(mra_msg, conv_name))?;
                        (text_utf16.to_utf8(), rest)
                    };
                    match tpe {
                        MraMessageType::ConferenceMessagePlaintext => {
                            // If no more bytes, author is self
                            if !rest.is_empty() {
                                let (author_bytes, rest) = next_sized_chunk(rest)?;
                                let author = String::from_utf8(author_bytes.to_vec())?;
                                require_format(rest.is_empty(), mra_msg, conv_name)?;

                                set_option!(interlocutor_username, author);
                            }
                        }
                        MraMessageType::ConferenceMessageRtf => {
                            let (_color, rest) = next_u32(rest);
                            // If no more bytes, author is self
                            if !rest.is_empty() {
                                let (author_bytes, rest) = next_sized_chunk(rest)?;
                                let author = String::from_utf8(author_bytes.to_vec())?;
                                require_format(rest.is_empty(), mra_msg, conv_name)?;

                                set_option!(interlocutor_username, author);
                            }
                        }
                        MraMessageType::AuthorizationRequest => {
                            // Username (email in most cases) followed by message, both in UTF-16 LE
                            set_option!(interlocutor_username, text);
                        }
                        _ => { /* NOOP */ }
                    }
                }
                MessageSectionType::Plaintext | MessageSectionType::MyAccount => { /* NOOP */ }
            }
        }
        if let Some(interlocutor_username) = interlocutor_username {
            let author_username = if mra_msg.is_from_me()? { my_username } else { interlocutor_username.as_str() };
            upsert_user(users, ds_uuid, author_username, author_name);
        }
    }

    Ok(())
}

fn process_conversation(
    db_msgs: &[DbMessage],
    my_username: &str,
    conv_name: &str,
    users: &HashMap<String, User>,
) -> EmptyRes {
    for db_msg in db_msgs {
        if !db_msg.sections.is_empty() {
            process_message(db_msg, conv_name, users)?;
        }
    }
    Ok(())
}

fn process_message(
    mra_msg: &DbMessage,
    conv_name: &str,
    users: &HashMap<String, User>,
) -> EmptyRes {
    let timestamp = match filetime_to_timestamp(mra_msg.header.filetime) {
        0 => mra_msg.header.some_timestamp_or_0 as i64,
        v => v
    };
    require_format_clue(timestamp != 0, mra_msg, conv_name, "timestamp is not known")?;

    require_format_clue(mra_msg.payload[0] == 1, mra_msg, conv_name, "first byte of payload wasn't 0x01")?;

    let tpe = mra_msg.get_tpe()?;

    // This is message author, which could be a system placeholder user
    let mut author_name: Option<String> = None;
    let mut plaintext: Option<String> = None;
    let mut my_username: Option<String> = None;
    let mut interlocutor_username: Option<String> = None;
    let mut rte: Option<String> = None;

    macro_rules! set_option {
        ($holder:ident, $new_value:expr) => {{
            let new_value = $new_value;
            if !new_value.is_empty() {
                if let Some(ref old_value) = $holder {
                    require_format_with_clue(
                        old_value == &new_value,
                        mra_msg,
                        conv_name,
                        || format!("unexpected {} value: {old_value} vs {new_value}", stringify!($holder)))?;
                } else {
                    $holder = Some(new_value)
                }
            }
        }};
    }

    for (section_type, section) in mra_msg.sections.iter() {
        match section_type {
            MessageSectionType::Plaintext => {
                set_option!(plaintext, String::from_utf8(section.to_vec())?);
            }
            MessageSectionType::AuthorName => {
                set_option!(author_name, String::from_utf8(section.to_vec())?);
            }
            MessageSectionType::OtherAccount2 => {
                assert!(interlocutor_username.as_deref() == Some("unreads")); // FIXME
                set_option!(interlocutor_username, String::from_utf8(section.to_vec())?)
            }
            MessageSectionType::MyAccount => {
                set_option!(my_username, String::from_utf8(section.to_vec())?);
            }
            MessageSectionType::OtherAccount => {
                set_option!(interlocutor_username, String::from_utf8(section.to_vec())?);
            }
            MessageSectionType::Content if tpe == MraMessageType::ConferenceUsersChange => {
                convert_conference_user_changed_record(conv_name, mra_msg, section, users)?;
            }
            MessageSectionType::Content => {
                let (text, rest) = {
                    let (text_bytes, rest) = next_sized_chunk(section)?;
                    let text_utf16 = WStr::from_utf16le(text_bytes)?;
                    (text_utf16.to_utf8(), rest)
                };
                match tpe {
                    MraMessageType::RegularPlaintext |
                    MraMessageType::FileTransfer |
                    MraMessageType::Call |
                    MraMessageType::BirthdayReminder |
                    MraMessageType::Sms |
                    MraMessageType::Cartoon |
                    MraMessageType::CartoonType2 |
                    MraMessageType::VideoCall => {
                        require_format(rest.is_empty(), mra_msg, conv_name)?;
                        // FIXME
                        set_option!(rte, text);
                    }
                    MraMessageType::RegularRtf => {
                        // Color followed by an optional unknown 4-bytes.
                        let (_color, rest) = next_u32(rest);
                        require_format_with_clue(rest.is_empty() || rest.len() == 4,
                                                 mra_msg,
                                                 conv_name,
                                                 || format!("follow-up to UTF-16 text section: {rest:02X?}"))?;
                        set_option!(rte, text);
                    }
                    MraMessageType::MicroblogRecordBroadcast => {
                        // Color followed by an optional unknown 4-bytes.
                        let (_color, rest) = next_u32(rest);
                        require_format_with_clue(rest.is_empty() || rest.len() == 4,
                                                 mra_msg,
                                                 conv_name,
                                                 || format!("follow-up to UTF-16 text section: {rest:02X?}"))?;
                        convert_microblog_record(&text, None);
                        set_option!(plaintext, text);
                    }
                    MraMessageType::MicroblogRecordDirected => {
                        let (target_name_bytes, rest) = next_sized_chunk(rest)?;
                        let target_name = WStr::from_utf16le(target_name_bytes)?;
                        let target_name = target_name.to_utf8();
                        require_format_with_clue(rest.len() == 8,
                                                 mra_msg,
                                                 conv_name,
                                                 || format!("follow-up to UTF-16 text section: {rest:02X?}"))?;
                        convert_microblog_record(&text, Some(&target_name));
                        set_option!(plaintext, text);
                    }
                    MraMessageType::ConferenceMessagePlaintext => {
                        // If no more bytes, author is self
                        if !rest.is_empty() {
                            let (author_bytes, rest) = next_sized_chunk(rest)?;
                            let author = String::from_utf8(author_bytes.to_vec())?;
                            require_format(rest.is_empty(), mra_msg, conv_name)?;

                            set_option!(interlocutor_username, author);
                        }
                        set_option!(plaintext, text);
                    }
                    MraMessageType::ConferenceMessageRtf => {
                        let (_color, rest) = next_u32(rest);
                        // If no more bytes, author is self
                        if !rest.is_empty() {
                            let (author_bytes, rest) = next_sized_chunk(rest)?;
                            let author = String::from_utf8(author_bytes.to_vec())?;
                            require_format(rest.is_empty(), mra_msg, conv_name)?;

                            set_option!(interlocutor_username, author);
                        }
                        set_option!(rte, text);
                    }
                    MraMessageType::ConferenceUsersChange => {
                        println!("CCCC!!!!");
                        //
                    }
                    MraMessageType::AuthorizationRequest => {
                        // Username (email in most cases) followed by message, both in UTF-16 LE
                        set_option!(interlocutor_username, text);
                        let (text_bytes, rest) = next_sized_chunk(rest)?;
                        set_option!(plaintext, WStr::from_utf16le(text_bytes)?.to_utf8());
                        require_format(rest.is_empty(), mra_msg, conv_name)?;
                    }
                    MraMessageType::AntispamTriggered => {
                        require_format(rest.len() == 4, mra_msg, conv_name)?;
                        // FIXME: make system message
                        // "Ваш аккаунт был заблокирован системой антиспама. Пожалуйста, смените пароль от вашего почтового ящика, пройдя по ссылке:
                        // http://e.mail.ru/cgi-bin/editpass?fromagent='MRA'"
                        rte = Some(text);
                    }
                    MraMessageType::LocationChange => {
                        // FIXME
                        rte = Some(text);
                    }
                    MraMessageType::Sticker => {
                        require_format(rest.is_empty(), mra_msg, conv_name)?;
                        // Contains SMILE tag like <SMILE>id='ext:MYNUMBER:sticker:MYNUMBER'</SMILE>,
                        // but I don't have a reference to retrieve them.
                        let _id = match SMILE_TAG_REGEX.captures(&text) {
                            Some(captures) if captures.name("alt").is_none() => captures.name("id").unwrap(),
                            _ => {
                                require_format_clue(false, mra_msg, conv_name, "unknown sticker ID format")?;
                                unreachable!()
                            }
                        };
                        rte = Some(text);
                    }
                    MraMessageType::Empty => {
                        require_format_clue(false, mra_msg, conv_name, "text was not expected for this message type")?;
                    }
                }
            }
        }
    }
    println!("{my_username:?} {interlocutor_username:?} by {author_name:?} - {plaintext:?}, {rte:?}");
    Ok(())
}

//
// Structs and enums
//

struct DbMessage<'a> {
    offset: usize,
    header: &'a DbMessageHeader,
    payload: &'a [u8],
    /// Parsed from payload
    sections: Vec<(MessageSectionType, &'a [u8])>,
}

impl MraMessage for DbMessage<'_> {
    fn get_tpe(&self) -> Result<MraMessageType> {
        let tpe_u8 = self.header.tpe_u8;
        FromPrimitive::from_u8(tpe_u8)
            .with_context(|| format!("Unknown message type: {:#04x}\nMessage hedaer: {:?}", tpe_u8, self))
    }

    fn is_from_me(&self) -> Result<bool> {
        Ok(self.header.flags & FLAG_INCOMING == 0)
    }
}

impl Debug for DbMessage<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let mut formatter = formatter.debug_struct("DbMessage");
        formatter.field("offset", &format!("{:#010x}", self.offset));
        let tpe_u8 = self.header.tpe_u8;
        let tpe_option: Option<MraMessageType> = FromPrimitive::from_u8(tpe_u8);
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
    /// Known variants are listed in MraMessageType
    tpe_u8: u8,
    /// Only FLAG_INCOMING is known
    flags: u8,
    _unknown1: [u8; 10],
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
        formatter.field("type_u8", &format!("{tpe_u8:#04X}"));
        let flags = self.flags;
        formatter.field("flags", &format!("{flags:#010b}"));
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
