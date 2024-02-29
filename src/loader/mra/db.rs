/// For strings, this format uses UTF-8 and UTF-16 LE.
/// Note that when both new and old DBs are present, new DB might have some messages missing!

use std::fmt::Formatter;
use std::fs;
use itertools::Itertools;

use super::*;

#[cfg(test)]
#[path = "db_tests.rs"]
mod tests;

const MSG_HEADER_MAGIC_NUMBER: u32 = 0x2D;

const FLAG_INCOMING: u8 = 0b100;

pub(super) type ConversationsMap = HashMap<String, (Vec<Message>, HashSet<UserId>)>;

/// Note that this will NOT add chats/messages to dataset map.
/// Instead, it will return them to be analyzed and added later.
pub(super) fn load_accounts_dir(
    path: &Path,
    storage_path: &Path,
    dataset_map: &mut DatasetMap,
) -> Result<HashMap<String, ConversationsMap>> {
    let mut result: HashMap<_, _> = Default::default();
    for dir_entry in fs::read_dir(path)? {
        let dir_entry = dir_entry?;
        let meta = dir_entry.metadata()?;
        let path = dir_entry.path();
        let name = path_file_name(&path)?;
        if meta.is_dir() {
            let entry = dataset_map.entry(name.to_owned()).or_insert_with(|| MraDatasetEntry {
                ds: Dataset { uuid: Some(PbUuid::random()), alias: name.to_owned() },
                ds_root: storage_path.to_path_buf(),
                users: Default::default(),
                cwms: Default::default(),
            });
            let ds_uuid = entry.ds.uuid();
            let conv_map = load_account(name, ds_uuid, &path, &mut entry.users)?;
            result.insert(name.to_owned(), conv_map);
        } else {
            log::warn!("{} is not a directory, ignored", name);
        }
    }
    Ok(result)
}

fn load_account(
    myself_username: &str,
    ds_uuid: &PbUuid,
    path: &Path,
    users: &mut HashMap<String, User>,
) -> Result<ConversationsMap> {
    let myself = User {
        ds_uuid: Some(ds_uuid.clone()),
        id: *MYSELF_ID,
        first_name_option: None,
        last_name_option: None,
        username_option: Some(myself_username.to_owned()),
        phone_number_option: None,
    };

    // Read whole files into the memory
    let mut db_msgs_map: HashMap<String, Vec<DbMessage>> = Default::default();
    for db_file in list_all_files(path, false)?
        .into_iter()
        .filter(|p| p.extension().and_then(|s| s.to_str()).is_some_and(|s| s == "db"))
    {
        let conv_username = path_file_name(&db_file)?.smart_slice(..-3).to_owned();
        if conv_username == "unreads" { continue; }

        let pretty_conv_name = format!("{myself_username} with {conv_username}");

        let db_bytes = fs::read(db_file)?;

        let msgs = load_conversation_messages(&conv_username, &db_bytes)?;

        let mut msgs = remove_bad_messages(&pretty_conv_name, msgs)?;

        // Messages might be out of order
        if let Err(e) = sort_messages(&mut msgs) {
            // Order of the same element is not well established (but hopefully stable across DB snapshots)
            log::warn!("{pretty_conv_name}: {}", e);
        }

        let msgs = remove_duplicates(&pretty_conv_name, msgs);

        db_msgs_map.insert(conv_username, msgs);
    }

    if users.is_empty() {
        users.insert(myself_username.to_owned(), myself);
    }

    for (conv_username, db_msgs) in db_msgs_map.iter() {
        collect_users(ds_uuid, myself_username, conv_username, db_msgs, users)?;
    }

    let mut result: ConversationsMap = Default::default();
    for (conv_username, db_msgs) in db_msgs_map.into_iter() {
        let (new_msgs, interlocutor_ids) = process_conversation(&db_msgs, myself_username, &conv_username, users)?;
        result.insert(conv_username, (new_msgs, interlocutor_ids));
    }
    Ok(result)
}

/// Loads all messages as-is
fn load_conversation_messages(conv_username: &str, db_bytes: &[u8]) -> Result<Vec<DbMessage>> {
    let mut result = vec![];
    let mut db_bytes = db_bytes;
    let mut offset = 0;
    while !db_bytes.is_empty() {
        let (bytes, rest_bytes) = next_sized_chunk(db_bytes)?;
        let offset_shift = bytes.len();
        let (message_len_again, rest_bytes) = next_u32_size(rest_bytes);
        require!(message_len_again == bytes.len(),
                 "Message was not followed by duplicated length!\nMessage bytes: {}",
                 bytes_to_pretty_string(bytes, usize::MAX));

        let bytes = {
            let (wrapped_bytes, remaining_bytes) = next_sized_chunk(bytes)?;
            require!(remaining_bytes.len() == 4);
            require!(read_u32(remaining_bytes, 0) as usize == wrapped_bytes.len());
            wrapped_bytes
        };

        let (magic_number, bytes) = next_u32(bytes);
        require!(magic_number == MSG_HEADER_MAGIC_NUMBER,
                 "Incorrect magic number for message at offset {offset:#010x}");

        let (header, bytes) = DbMessageHeader::next_header(bytes, offset)?;
        require!(header.full_length as usize == message_len_again + 8,
                 "Incorrect header for message at offset {offset:#010x}: {header:?}");

        let (payload, bytes) = next_sized_chunk(bytes)?;

        let mut mra_msg = DbMessage { offset: offset as u32, header, payload: payload.to_vec(), sections: vec![] };

        require_format_clue(bytes.is_empty(), &mra_msg, conv_username, "incorrect remainder")?;
        require_format_clue(payload[0] == 0x01, &mra_msg, conv_username, "incorrect payload magic")?;

        let (payload_inner_length, payload) = next_u32_size(&payload[1..]);
        require_format_clue(payload_inner_length == payload.len(), &mra_msg, conv_username,
                            "incorrect payload inner length")?;

        // Getting sections out of payload
        let mut payload = payload;
        while !payload.is_empty() {
            payload = {
                let (section_type_u32, payload) = next_u32(payload);
                let section_type: Option<MessageSectionType> = FromPrimitive::from_u32(section_type_u32);
                require_format_with_clue(section_type.is_some(), &mra_msg, conv_username,
                                         || format!("unknown message section: {section_type_u32}"))?;
                // No matter what the section is, it's sized
                let (section_bytes, payload) = next_sized_chunk(payload)?;
                mra_msg.sections.push((section_type.unwrap(), section_bytes.to_vec()));
                payload
            }
        }
        mra_msg.sections.sort_by_key(|pair| pair.0);

        // Placeholder message of type Empty are expected to always have some timestamp set.
        if mra_msg.header.tpe_u8 == MraMessageType::Empty as u8 {
            require_format(mra_msg.header.some_timestamp_or_0 > 0, &mra_msg, conv_username)?;
        } else {
            // Sanity checking filetime, using year 1995 for that
            const MIN_FILETIME: u64 = 124500000000000000;
            require_format_clue(mra_msg.header.filetime > MIN_FILETIME, &mra_msg, conv_username,
                                "filetime is not known")?;
        }
        result.push(mra_msg);

        offset += offset_shift + 8;
        db_bytes = rest_bytes;
    }
    Ok(result)
}

/// Removes empty and "phantom" messages.
/// Phantom messages appear about one hour before real messages. I couldn't determine exact relation between the two,
/// so using best guess to separate and remove them.
/// These messages seem to be incorrectly copied over from mra.dbs, and in DB snapshots taken after 2015
/// they're observed to have proper counterparts.
/// However, in DB between 2014 and 2015 proper messages only exist in mra.dbs, and it's therefore not possible to
/// establish valid timestamps shift for mra.dbs.
///
/// Note that messages here are in the order they were read in.
fn remove_bad_messages(pretty_conv_name: &str, mra_msgs: Vec<DbMessage>) -> Result<Vec<DbMessage>> {
    // Identified the following characteristics of "legacy" phantom messages:
    // * Non-zero _unknown and some_timestamp_or_0 fields.
    // * These values are shared by at least one placeholder record that:
    //   * May or may not be of type Empty.
    //   * May or may not mention an exact address of a phantom message.
    //   * Could come right before the phantom message, or some time after it.
    // * Has a timestamp below a certain value (see below)

    // Established to fall between 1399727019 and 1399740988, picked as a middle point between the two
    const MAX_LEGACY_PHANTOM_TIMESTAMP: i32 = 1399734000;

    let referenced: HashSet<(_, _)> = mra_msgs.iter()
        .filter(|m| m.sections.is_empty() && m.header.some_timestamp_or_0 > 0 && m.header._unknown > 0)
        .map(|m| (m.header.some_timestamp_or_0, m.header._unknown))
        .collect();

    let mut bad_indices = HashSet::new();
    for (idx, mra_msg) in mra_msgs.iter().enumerate() {
        if !mra_msg.sections.is_empty() &&
            mra_msg.header._unknown != 0 &&
            mra_msg.header.some_timestamp_or_0 > 0 &&
            mra_msg.header.some_timestamp_or_0 <= MAX_LEGACY_PHANTOM_TIMESTAMP &&
            referenced.contains(&(mra_msg.header.some_timestamp_or_0, mra_msg.header._unknown))
        {
            bad_indices.insert(idx);
        }
    }

    // In addition to these, messages after the timestamp above might also be phantom.
    // Unfortunately, the only way to know for sure is to check.
    fn get_plaintext(m: &DbMessage) -> String {
        let bs = m.sections.iter().find(|s| s.0 == MessageSectionType::Plaintext).map(|s| &s.1).expect("No plaintext!");
        let s = String::from_utf8_lossy(bs);
        s.replace('\r', "")
    }
    for (idx_prev, mra_msg_prev) in mra_msgs.iter().enumerate().filter(|(_, m)|
        !m.sections.is_empty() && m.header._unknown != 0 && m.header.some_timestamp_or_0 > MAX_LEGACY_PHANTOM_TIMESTAMP)
    {
        const SEC_DIFF: u64 = 3600;
        const SEC_DIFF_DELTA: u64 = 30;
        const MIN_SEC_DIFF: u64 = SEC_DIFF - SEC_DIFF_DELTA;
        const MAX_SEC_DIFF: u64 = SEC_DIFF + SEC_DIFF_DELTA;
        const MIN_FT_DIFF: u64 = MIN_SEC_DIFF * 10_000_000;
        const MAX_FT_DIFF: u64 = MAX_SEC_DIFF * 10_000_000;

        let pt_prev = get_plaintext(mra_msg_prev);

        for mra_msg in mra_msgs[(idx_prev + 1)..].iter().filter(|m|
            !m.sections.is_empty() && m.header.filetime >= mra_msg_prev.header.filetime)
        {
            let ft_diff = mra_msg.header.filetime - mra_msg_prev.header.filetime;
            if ft_diff < MIN_FT_DIFF { continue; }
            if ft_diff > MAX_FT_DIFF { break; }

            if get_plaintext(mra_msg) == pt_prev {
                bad_indices.insert(idx_prev);
                break;
            }
        }
    }

    if !bad_indices.is_empty() {
        log::debug!(r#"{pretty_conv_name}: Found {} "phantom" messages"#, bad_indices.len());
    }

    // Remove empty messages
    bad_indices.extend(mra_msgs.iter().enumerate().filter(|(_, m)| m.sections.is_empty()).map(|(idx, _)| idx));

    Ok(without_indices(mra_msgs, bad_indices))
}

/// Sometimes, message occur several times (with <2 second difference).
/// While we can't always be sure those messages weren't sent manually, we still remove them.
/// This should be called on a sorted vector.
fn remove_duplicates(pretty_conv_name: &str, mra_msgs: Vec<DbMessage>) -> Vec<DbMessage> {
    const MAX_FT_DIFF: u64 = 20_000_000;
    let mut bad_indices = HashSet::new();
    for idx1 in 0..mra_msgs.len() {
        let msg_prev = &mra_msgs[idx1];
        for (idx2, msg) in mra_msgs.iter().enumerate().skip(idx1 + 1) {
            if msg.header.filetime - msg_prev.header.filetime > MAX_FT_DIFF { break; }
            if msg.sections == msg_prev.sections {
                bad_indices.insert(idx2);
            }
        }
    }

    if !bad_indices.is_empty() {
        log::debug!(r#"{pretty_conv_name}: Found {} duplicate messages"#, bad_indices.len());
    }

    without_indices(mra_msgs, bad_indices)
}

fn sort_messages(msgs: &mut Vec<DbMessage>) -> EmptyRes {
    if msgs.is_empty() { return Ok(()); }

    msgs.sort_by_key(|m| m.header.filetime);

    let mut problematic_filetimes = vec![];

    for idx1 in 0..(msgs.len() - 1) {
        let next_seq_idx = {
            let ft_1 = msgs[idx1].header.filetime;
            ((idx1 + 1)..msgs.len())
                .find(|idx| msgs[*idx].header.filetime > ft_1)
                .unwrap_or(msgs.len())
        };

        // Sort a section [idx1; next_seq_idx) starting from the end.
        // If all messages have NULL target address, their order is left unchanged.
        // One corner case we can't distinguish is when next_message_addr is actually 0 and not NULL.
        if msgs[(idx1 + 1)..next_seq_idx].iter().all(|m| m.header.next_message_addr == 0) {
            continue;
        }
        let mut lookup_next_messsage_addr = 0;
        'outer: for target_idx in ((idx1 + 1)..next_seq_idx).rev() {
            for idx2 in (idx1..=target_idx).rev() {
                if msgs[idx2].header.next_message_addr == lookup_next_messsage_addr {
                    lookup_next_messsage_addr = msgs[idx2].offset;
                    msgs.swap(target_idx, idx2);
                    // Last message is in position now
                    continue 'outer;
                }
            }
            problematic_filetimes.push(msgs[idx1].header.filetime);
        }
        if next_seq_idx > idx1 + 1 && msgs[idx1].header.next_message_addr != msgs[idx1 + 1].offset {
            problematic_filetimes.push(msgs[idx1].header.filetime)
        }
    }

    problematic_filetimes.dedup();
    require!(problematic_filetimes.is_empty(),
             "Incorrect same-filetime messages linked list! (FT = {:?})", problematic_filetimes);
    Ok(())
}

fn collect_users(
    ds_uuid: &PbUuid,
    myself_username: &str,
    conv_username: &str,
    msgs: &[DbMessage],
    users: &mut HashMap<String, User>,
) -> EmptyRes {
    upsert_user(users, ds_uuid, myself_username, None);
    upsert_user(users, ds_uuid, conv_username, None);

    for mra_msg in msgs {
        macro_rules! set_option {
            ($holder:ident, $new_value:expr) => {{
                let new_value = $new_value;
                if !new_value.is_empty() {
                    if let Some(ref old_value) = $holder {
                        require_format_with_clue(
                            old_value == &new_value,
                            mra_msg,
                            conv_username,
                            || format!("unexpected {} value: {old_value} vs {new_value}", stringify!($holder)))?;
                    } else {
                        $holder = Some(new_value)
                    }
                }
            }};
        }

        let mut interlocutor_username = None;
        let mut author_name: Option<String> = None;
        let tpe = mra_msg.get_tpe()?;

        for (section_type, section) in mra_msg.sections.iter() {
            match section_type {
                MessageSectionType::AuthorName => {
                    set_option!(author_name, String::from_utf8(section.to_vec())?);
                }
                MessageSectionType::OtherAccount => {
                    set_option!(interlocutor_username, String::from_utf8(section.to_vec())?);
                }
                MessageSectionType::Content if tpe == MraMessageType::ConferenceUsersChange =>
                    collect_users_from_conference_user_changed_record(
                        users, ds_uuid, conv_username, mra_msg, section)?,
                MessageSectionType::Content => {
                    let (text, rest) = {
                        let (text_bytes, rest) = next_sized_chunk(section)?;
                        let text = utf16le_to_string(text_bytes).with_context(|| context(mra_msg, conv_username))?;
                        (text, rest)
                    };
                    match tpe {
                        MraMessageType::ConferenceMessagePlaintext => {
                            // If no more bytes, author is self
                            if !rest.is_empty() {
                                let (author_bytes, rest) = next_sized_chunk(rest)?;
                                let author = String::from_utf8(author_bytes.to_vec())?;
                                require_format(rest.is_empty(), mra_msg, conv_username)?;

                                set_option!(interlocutor_username, author);
                            }
                        }
                        MraMessageType::ConferenceMessageRtf => {
                            let (_color, rest) = next_u32(rest);
                            // If no more bytes, author is self
                            if !rest.is_empty() {
                                let (author_bytes, rest) = next_sized_chunk(rest)?;
                                let author = String::from_utf8(author_bytes.to_vec())?;
                                require_format(rest.is_empty(), mra_msg, conv_username)?;

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
                MessageSectionType::OtherAccountInUnreads => {
                    require_format_clue(false, mra_msg, conv_username,
                                        "unexpected section type OtherAccountInUnreads")?;
                    unreachable!();
                }
                MessageSectionType::Plaintext | MessageSectionType::MyAccount => { /* NOOP */ }
            }
        }
        if interlocutor_username.is_some() || author_name.is_some() {
            let interlocutor_username = interlocutor_username.as_deref().unwrap_or(conv_username);
            let author_username = if mra_msg.is_from_me()? { myself_username } else { interlocutor_username };
            upsert_user(users, ds_uuid, author_username, author_name);
        }
    }

    Ok(())
}

fn process_conversation(
    db_msgs: &[DbMessage],
    myself_username: &str,
    conv_username: &str,
    users: &HashMap<String, User>,
) -> Result<(Vec<Message>, HashSet<UserId>)> {
    let mut msgs: Vec<Message> = vec![];
    let mut ongoing_call_msg_id = None;
    let mut interlocutor_ids = HashSet::from([MYSELF_ID]);
    let mut internal_id = 0;
    for mra_msg in db_msgs {
        if let Some(msg) = convert_message(mra_msg, internal_id, myself_username, conv_username, users,
                                           &mut msgs, &mut ongoing_call_msg_id)? {
            interlocutor_ids.insert(UserId(msg.from_id));
            msgs.push(msg);
            internal_id += 1;
        }
    }

    Ok((msgs, interlocutor_ids))
}

fn convert_message(
    mra_msg: &DbMessage,
    internal_id: i64,
    myself_username: &str,
    conv_username: &str,
    users: &HashMap<String, User>,
    prev_msgs: &mut [Message],
    ongoing_call_msg_id: &mut Option<i64>,
) -> Result<Option<Message>> {
    let timestamp = filetime_to_timestamp(mra_msg.header.filetime);

    let from_me = mra_msg.is_from_me()?;
    let mut from_username = (if from_me { myself_username } else { conv_username }).to_owned();

    // For a source message ID, we're using message time.
    // It's SUPPOSED to be precise enough to be unique within a chat, but in practice it's too rounded.
    // To work around that, we increment source IDs when it's duplicated.
    let source_id_option = {
        let source_id = (mra_msg.header.filetime / 2) as i64;
        Some(if let Some(last_source_id) = prev_msgs.last().and_then(|m| m.source_id_option) {
            if last_source_id >= source_id {
                last_source_id + 1
            } else {
                source_id
            }
        } else {
            source_id
        })
    };

    require_format_clue(mra_msg.payload[0] == 1, mra_msg, conv_username, "first byte of payload wasn't 0x01")?;

    let tpe = mra_msg.get_tpe()?;

    // Going over sections to collect this data, it will be processed later
    let mut plaintext: Option<String> = None;
    let mut rtf: Option<String> = None;
    let mut microblog_record_target_name: Option<String> = None;
    let mut location: Option<ContentLocation> = None;
    let mut conference_user_changed_payload: Option<&[u8]> = None;

    macro_rules! set_option {
        ($holder:ident, $new_value:expr) => {{
            let new_value = $new_value;
            if !new_value.is_empty() {
                if let Some(ref old_value) = $holder {
                    require_format_with_clue(old_value == &new_value, mra_msg, conv_username,
                        || format!("unexpected {} value: {old_value} vs {new_value}", stringify!($holder)))?;
                } else {
                    $holder = Some(new_value)
                }
            }
        }};
    }

    macro_rules! set_unless_empty {
        ($variable:ident, $new_value:expr) => {{
            let new_value = $new_value;
            if !new_value.is_empty() {
                $variable = new_value
            }
        }};
    }

    macro_rules! set_text {
        ($new_text:expr) => {{
            let new_text = $new_text;
            if !new_text.is_empty() {
                if new_text.starts_with(r"{\rtf") {
                    set_option!(rtf, new_text);
                } else {
                    set_option!(plaintext, new_text);
                }
            }
        }};
    }

    for (section_type, section) in mra_msg.sections.iter() {
        match section_type {
            MessageSectionType::Plaintext => {
                set_text!(String::from_utf8(section.to_vec())?);
            }
            MessageSectionType::OtherAccount => {
                if !from_me { set_unless_empty!(from_username, String::from_utf8(section.to_vec())?); }
            }
            MessageSectionType::Content if tpe == MraMessageType::ConferenceUsersChange => {
                conference_user_changed_payload = Some(section);
            }
            MessageSectionType::Content => {
                let (text, rest) = {
                    let (text_bytes, rest) = next_sized_chunk(section)?;
                    let text = utf16le_to_string(text_bytes)?;
                    (text, rest)
                };
                match tpe {
                    MraMessageType::RegularPlaintext |
                    MraMessageType::Call |
                    MraMessageType::BirthdayReminder |
                    MraMessageType::Sms |
                    MraMessageType::Cartoon |
                    MraMessageType::CartoonType2 |
                    MraMessageType::VideoCall => {
                        require_format(rest.is_empty(), mra_msg, conv_username)?;
                        set_text!(text);
                    }
                    MraMessageType::FileTransfer => {
                        // Force-replacing plaintext content
                        plaintext = Some(text);
                    }
                    MraMessageType::RegularRtf => {
                        // Color followed by an optional unknown 4-bytes.
                        let (_color, rest) = next_u32(rest);
                        require_format_with_clue(rest.is_empty() || rest.len() == 4,
                                                 mra_msg,
                                                 conv_username,
                                                 || format!("follow-up to UTF-16 text section: {rest:02X?}"))?;
                        set_text!(text);
                    }
                    MraMessageType::MicroblogRecordBroadcast => {
                        // Color followed by an optional unknown 4-bytes.
                        let (_color, rest) = next_u32(rest);
                        require_format_with_clue(rest.is_empty() || rest.len() == 4,
                                                 mra_msg,
                                                 conv_username,
                                                 || format!("follow-up to UTF-16 text section: {rest:02X?}"))?;
                        set_text!(text);
                    }
                    MraMessageType::MicroblogRecordDirected => {
                        let (target_name_bytes, rest) = next_sized_chunk(rest)?;
                        let target_name = utf16le_to_string(target_name_bytes)?;
                        require_format_with_clue(rest.len() == 8,
                                                 mra_msg,
                                                 conv_username,
                                                 || format!("follow-up to UTF-16 text section: {rest:02X?}"))?;
                        microblog_record_target_name = Some(target_name);
                        set_text!(text);
                    }
                    MraMessageType::ConferenceMessagePlaintext => {
                        // If no more bytes, author is self
                        if !rest.is_empty() {
                            let (author_bytes, rest) = next_sized_chunk(rest)?;
                            let author = String::from_utf8(author_bytes.to_vec())?;
                            require_format(rest.is_empty(), mra_msg, conv_username)?;

                            if !from_me { set_unless_empty!(from_username, author); }
                        }
                        set_text!(text);
                    }
                    MraMessageType::ConferenceMessageRtf => {
                        let (_color, rest) = next_u32(rest);
                        // If no more bytes, author is self
                        if !rest.is_empty() {
                            let (author_bytes, rest) = next_sized_chunk(rest)?;
                            let author = String::from_utf8(author_bytes.to_vec())?;
                            require_format(rest.is_empty(), mra_msg, conv_username)?;

                            if !from_me { set_unless_empty!(from_username, author); }
                        }
                        set_text!(text);
                    }
                    MraMessageType::AuthorizationRequest => {
                        // Username (email in most cases) followed by message, both in UTF-16 LE
                        if !from_me { set_unless_empty!(from_username, text); }
                        let (text_bytes, rest) = next_sized_chunk(rest)?;
                        require_format(rest.is_empty(), mra_msg, conv_username)?;
                        set_text!(utf16le_to_string(text_bytes)?);
                    }
                    MraMessageType::AntispamTriggered => {
                        require_format(rest.len() == 4, mra_msg, conv_username)?;
                        set_text!(text);
                    }
                    MraMessageType::LocationChange => {
                        // Lattitude
                        let (lat_bytes, rest) = next_sized_chunk(rest)?;
                        let lat_str = String::from_utf8(lat_bytes.to_vec())?;
                        // Longitude
                        let (lon_bytes, _rest) = next_sized_chunk(rest)?;
                        let lon_str = String::from_utf8(lon_bytes.to_vec())?;

                        location = Some(ContentLocation {
                            title_option: None,
                            address_option: Some(text),
                            lat_str,
                            lon_str,
                            duration_sec_option: None,
                        });
                    }
                    MraMessageType::Sticker => {
                        require_format(rest.is_empty(), mra_msg, conv_username)?;
                        set_text!(text);
                    }
                    MraMessageType::Empty | MraMessageType::ConferenceUsersChange => {
                        unreachable!()
                    }
                }
            }
            MessageSectionType::AuthorName | MessageSectionType::MyAccount | MessageSectionType::OtherAccountInUnreads => {
                /* Already processed, NOOP */
            }
        }
    }

    // Processing the data collected earlier to make a true message

    // println!("'{from_username}' - {plaintext:?}, {rtf:?}");

    let get_rtes = || ok(match (rtf.as_ref(), plaintext.as_ref()) {
        (Some(rtf), _) => {
            Some(parse_rtf(rtf).with_context(|| context(mra_msg, conv_username))?)
        }
        (_, Some(text)) => {
            let text = normalize_plaintext(text);
            Some(vec![RichText::make_plain(text)])
        }
        _ => {
            None
        }
    });

    use message::Typed;
    let (text, typed) = match tpe {
        MraMessageType::RegularPlaintext |
        MraMessageType::AuthorizationRequest |
        MraMessageType::RegularRtf |
        MraMessageType::Sms |
        MraMessageType::ConferenceMessagePlaintext |
        MraMessageType::ConferenceMessageRtf => {
            let rtes = get_rtes()?;
            require_format_clue(rtes.is_some(), mra_msg, conv_username, "text is not set")?;
            (rtes.unwrap(), Typed::Regular(Default::default()))
        }
        MraMessageType::AntispamTriggered |
        MraMessageType::BirthdayReminder => {
            let rtes = get_rtes()?;
            require_format_clue(rtes.is_some(), mra_msg, conv_username, "text is not set")?;
            (rtes.unwrap(), message_service!(ServiceSvo::Notice(MessageServiceNotice {})))
        }
        MraMessageType::Cartoon |
        MraMessageType::CartoonType2 => {
            require_format_clue(plaintext.is_some(), mra_msg, conv_username, "cartoon source is not set")?;
            let text = plaintext.unwrap();
            convert_cartoon(&text).with_context(|| context(mra_msg, conv_username))?
        }
        MraMessageType::Sticker => {
            require_format_clue(plaintext.is_some(), mra_msg, conv_username, "sticker source is not set")?;
            let text = plaintext.unwrap();
            // Contains SMILE tag like <SMILE>id='ext:MYNUMBER:sticker:MYNUMBER'</SMILE>,
            // but I don't have a reference to retrieve them.
            let _id = match SMILE_TAG_REGEX.captures(&text) {
                Some(captures) if captures.name("alt").is_none() => captures.name("id").unwrap(),
                _ => {
                    require_format_clue(false, mra_msg, conv_username, "unknown sticker ID format")?;
                    unreachable!()
                }
            };
            (vec![], message_regular! {
                content_option: Some(Content {
                    sealed_value_optional: Some(ContentSvo::Sticker(ContentSticker {
                        path_option: None,
                        width: 0,
                        height: 0,
                        thumbnail_path_option: None,
                        emoji_option: None,
                    }))
                }),
                ..Default::default()
            })
        }
        MraMessageType::FileTransfer => {
            require_format_clue(plaintext.is_some(), mra_msg, conv_username, "file transfer text is not set")?;
            convert_file_transfer(&plaintext.unwrap())?
        }
        MraMessageType::Call |
        MraMessageType::VideoCall => {
            require_format_clue(plaintext.is_some(), mra_msg, conv_username, "call text is not set")?;
            let text = plaintext.unwrap();

            match process_call(&text, internal_id, mra_msg, conv_username, timestamp, ongoing_call_msg_id, prev_msgs)? {
                Some(text_and_typed) => text_and_typed,
                None => return Ok(None),
            }
        }
        MraMessageType::ConferenceUsersChange => {
            require_format_clue(conference_user_changed_payload.is_some(), mra_msg, conv_username,
                                "conference user changed payload is not set")?;
            convert_conference_user_changed_record(conv_username, mra_msg, conference_user_changed_payload.unwrap(), users)?
        }
        MraMessageType::MicroblogRecordBroadcast |
        MraMessageType::MicroblogRecordDirected => {
            require_format_clue(plaintext.is_some(), mra_msg, conv_username, "microblog plaintext is not set")?;
            require_format_clue(rtf.is_none(), mra_msg, conv_username, "unexpected microblog RTF")?;
            convert_microblog_record(&plaintext.unwrap(), microblog_record_target_name.as_deref())
        }
        MraMessageType::LocationChange => {
            require_format_clue(location.is_some(), mra_msg, conv_username, "location is not set")?;

            (vec![RichText::make_plain("(Location changed)".to_owned())],
             message_regular! {
                 content_option: Some(Content {
                     sealed_value_optional: Some(ContentSvo::Location(location.unwrap()))
                 }),
                 ..Default::default()
             })
        }
        MraMessageType::Empty => {
            unreachable!()
        }
    };

    let user = users.get(&from_username)
        .with_context(|| format!("no user found with username '{from_username}', looks like a bug!"))?;
    Ok(Some(Message::new(
        internal_id,
        source_id_option,
        timestamp,
        user.id(),
        text,
        typed,
    )))
}

pub(super) fn merge_conversations(
    convs_map: HashMap<String, ConversationsMap>,
    dataset_map: &mut DatasetMap,
) -> EmptyRes {
    for (myself_username, conv_map) in convs_map {
        // Guaranteed to be present
        let entry = dataset_map.get_mut(&myself_username).unwrap();
        let ds_uuid = entry.ds.uuid().clone();

        for (conv_username, (new_msgs, mut interlocutor_ids)) in conv_map {
            let cwm = entry.cwms.entry(conv_username.clone()).or_insert_with(|| {
                ChatWithMessages {
                    chat: Some(Chat {
                        ds_uuid: Some(ds_uuid.clone()),
                        id: loader::hash_to_id(&conv_username),
                        name_option: Some(conv_username.clone()),
                        source_type: SourceType::Mra as i32,
                        tpe: -1, // Will be changed later
                        img_path_option: None,
                        member_ids: vec![], // Will be changed later
                        msg_count: -1, // Will be changed later
                        main_chat_id: None,
                    }),
                    messages: vec![],
                }
            });

            merge_messages(&format!("{myself_username} with {conv_username}"),
                           new_msgs,
                           &mut cwm.messages)?;

            if let Some(chat) = cwm.chat.as_mut() {
                interlocutor_ids.extend(chat.member_ids.iter().map(|id| UserId(*id)));
                chat.member_ids = interlocutor_ids
                    .into_iter()
                    .map(|id| id.0)
                    .sorted_by_key(|&id| if id == *MYSELF_ID { i64::MIN } else { id })
                    .collect_vec();

                chat.tpe = if conv_username.ends_with("@chat.agent") || chat.member_ids.len() > 2 {
                    ChatType::PrivateGroup as i32
                } else {
                    ChatType::Personal as i32
                };
                chat.msg_count = cwm.messages.len() as i32;
            }
        }
    }

    Ok(())
}

/// Merge new messages into old ones, skipping all new messages that are already stored.
/// Note that old and new messages have different source IDs, and might actually have slight time differences.
fn merge_messages(pretty_conv_name: &str, new_msgs: Vec<Message>, msgs: &mut Vec<Message>) -> EmptyRes {
    if msgs.is_empty() {
        // Trivial case
        msgs.extend(new_msgs);
    } else if new_msgs.is_empty() {
        // NOOP
    } else {
        log::debug!("{pretty_conv_name}: Merging conv (old {} <- new {})", msgs.len(), new_msgs.len());
        let old_len = msgs.len();
        let last_internal_id = msgs.last().map(|m| m.internal_id).unwrap_or_default();

        let first_new_idx = first_start_of_new_slice(pretty_conv_name, msgs, &new_msgs);
        msgs.extend(new_msgs.into_iter().skip(first_new_idx));

        for (new_msg, internal_id) in msgs.iter_mut().skip(old_len).zip((last_internal_id + 1)..) {
            new_msg.internal_id = internal_id;
        }
    }

    Ok(())
}

/// Find the first index of new message that isn't contained in old messages.
/// (When both new and old DBs are present, new DB might have some messages missing.)
fn first_start_of_new_slice(pretty_conv_name: &str, old_msgs: &[Message], new_msgs: &[Message]) -> usize {
    const MAX_TIMESTAMP_DIFF: i64 = 10;
    let last_old_msg = old_msgs.last().unwrap(); // At this point, both old and new msgs are not empty
    for (idx, new_msg) in new_msgs.iter().enumerate().rev() {
        if msg_eq(new_msg, last_old_msg) &&
            (new_msg.timestamp - last_old_msg.timestamp).abs() <= MAX_TIMESTAMP_DIFF
        {
            // Next message is truly new
            log::debug!("{pretty_conv_name}: Intersection ends at index {}", idx + 1);
            return idx + 1;
        } else if new_msg.timestamp - last_old_msg.timestamp > MAX_TIMESTAMP_DIFF {
            // No intersections, all messages starting from this one are new
            if idx == 0 {
                log::debug!("{pretty_conv_name}: All new messages are new");
            } else {
                log::debug!("{pretty_conv_name}: New messages start at index {}", idx);
            }
            return idx;
        }
    }
    log::warn!("{pretty_conv_name}: No intersections between old and new DB found for conversation!");
    0
}

//
// Structs and enums
//

/// Made to own its content to simplify moving it around.
#[derive(Clone, PartialEq, Eq, Default)]
struct DbMessage {
    offset: u32,
    header: DbMessageHeader,
    payload: Vec<u8>,
    /// Parsed from payload
    sections: Vec<(MessageSectionType, Vec<u8>)>,
}

impl MraMessage for DbMessage {
    fn get_tpe(&self) -> Result<MraMessageType> {
        let tpe_u8 = self.header.tpe_u8;
        FromPrimitive::from_u8(tpe_u8)
            .with_context(|| format!("Unknown message type: {:#04x}\nMessage hedaer: {:?}", tpe_u8, self))
    }

    fn is_from_me(&self) -> Result<bool> {
        Ok(self.header.flags & FLAG_INCOMING == 0)
    }
}

impl Debug for DbMessage {
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
        formatter.field("payload", &bytes_to_pretty_string(&self.payload, usize::MAX));
        formatter.finish()
    }
}

#[repr(C, packed)]
#[derive(Clone, PartialEq, Eq, Default)]
struct DbMessageHeader {
    /// == 1
    magic_value_one: u8,
    /// Known variants are listed in MraMessageType
    tpe_u8: u8,
    /// Only FLAG_INCOMING is known
    flags: u8,
    padding1: u16,
    /// Length of the entire message including prefix and suffix lengths
    full_length: u32,
    /// Offset of another message this message refers to.
    /// For regular messages this is used to establish order between messages with the same filetime.
    next_message_addr: u32,
    /// WinApi FILETIME
    filetime: u64,
    /// No idea what that is, but it doesn't look like a bit set. It's also not random, is shared between some messages,
    /// and its values seem to fall in certain range.
    _unknown: u32,
    /// Might slightly differ from filetime
    some_timestamp_or_0: i32,
    padding2: u128,
}

impl DbMessageHeader {
    fn next_header(bs: &[u8], offset: usize) -> Result<(DbMessageHeader, &[u8])> {
        const HEADER_SIZE: usize = mem::size_of::<DbMessageHeader>();
        require!(bs.len() >= HEADER_SIZE, "Byte slice at offset {offset:#010x} is too short to fit a header!");

        // This is safe as all header fields can fit any byte sequence - which we ensured is long enough.
        let header = unsafe {
            let header_ptr = bs.as_ptr() as *const DbMessageHeader;
            header_ptr.as_ref().unwrap().clone()
        };
        require!(header.magic_value_one == 1 && header.padding1 == 0 && header.padding2 == 0,
                 "Incorrect header for message at offset {offset:#010x}: {header:?}");

        Ok((header, &bs[HEADER_SIZE..]))
    }
}

impl Debug for DbMessageHeader {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let mut formatter = formatter.debug_struct("Header");
        formatter.field("type_u8", &format!("{:#04x}", clone_packed!(self.tpe_u8)));
        formatter.field("flags", &format!("{:#010b}", clone_packed!(self.flags)));
        let addr_string = if self.next_message_addr == 0 {
            "NULL".to_owned()
        } else {
            format!("{:#010x}", clone_packed!(self.next_message_addr))
        };
        formatter.field("next_message_addr", &format!("{addr_string:<10}"));
        formatter.field("filetime", &clone_packed!(self.filetime));
        formatter.field("_unknown", &format!("{:#010x}", clone_packed!(self._unknown)));
        formatter.field("some_timestamp_or_0", &clone_packed!(self.some_timestamp_or_0));
        formatter.finish()
    }
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, FromPrimitive)]
enum MessageSectionType {
    Plaintext = 0x00,
    AuthorName = 0x02,
    OtherAccountInUnreads = 0x03,
    MyAccount = 0x04,
    OtherAccount = 0x05,
    /// Exact content format depends on the message type
    Content = 0x06,
}
