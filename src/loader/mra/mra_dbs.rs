/// Old versions of Mail.Ru Agent (up to 2014-08-28) stored data in unknown DBMS format storage, with strings formatted
/// as UTF-16 LE. Afterwards, storage moved to a new separate .db files and mra.dbs was left as-is until 2015-03-17
/// when all conversations were deleted from it.
///
/// Known issues:
/// * Some smile types are not converted and left as-is since there's no reference too see how they looked like.
/// * Only a limited RTF support has been added - just bold, italic and underline styles, and only one per substring.
/// * In rare cases, Russian text is double-encoded as cp1251 within UTF-16 LE. Distorted text is passed as-is.
///
/// Following references were helpful in reverse engineering the format (in Russian):
/// * https://xakep.ru/2012/11/30/mailru-agent-hack/
/// * https://c0dedgarik.blogspot.com/2010/08/mradbs.html

use super::*;

const MSG_HEADER_MAGIC_NUMBER: u32 = 0x38;

const CONFERENCE_USER_JOINED: u32 = 0x03;
const CONFERENCE_USER_LEFT: u32 = 0x05;

pub(super) fn load_convs_with_msgs<'a>(dbs_bytes: &'a [u8]) -> Result<Vec<MraLegacyConversationWithMessages<'a>>> {
    let offsets_table: &[u32] = load_offsets_table(&dbs_bytes)?;
    let convs = load_conversations(&dbs_bytes, offsets_table)?;
    load_messages(&dbs_bytes, offsets_table, convs)
}

fn load_offsets_table(dbs_bytes: &[u8]) -> Result<&[u32]> {
    const OFFSETS_TABLE_OFFSET: usize = 0x10;
    const OFFSETS_MAGIC_NUMBER: u32 = 0x04;
    let offsets_table_addr = read_u32(&dbs_bytes, OFFSETS_TABLE_OFFSET) as usize;
    let offsets_table: &[u32] = {
        let u8_slice = &dbs_bytes[offsets_table_addr..];
        require!(offsets_table_addr % 4 == 0 && u8_slice.len() % 4 == 0,
                 "Misaligned offset table at address {:#010x}!", offsets_table_addr);
        // Re-interpreting offsets table as a u32 slice.
        // This is safe because we already checked slice alignment and length.
        unsafe { slice::from_raw_parts(u8_slice.as_ptr() as *const u32, u8_slice.len() / 4) }
    };
    log::debug!("Offsets table is at {:#010x}", offsets_table_addr);
    require!(offsets_table[0] == OFFSETS_MAGIC_NUMBER,
             "Sanity check failed: unexpected offset table magic number");
    Ok(offsets_table)
}

fn load_conversations<'a>(dbs_bytes: &'a [u8], offsets_table: &[u32]) -> Result<Vec<MraLegacyConversation<'a>>> {
    // Offsets are relative to where offset table points at
    const CONVERSATIONS_COUNT_OFFSET: usize = 0x20;
    const LAST_CONVERSATION_OFFSET: usize = 0x2C;

    const CONVERSATION_IDS_OFFSET: usize = 0x04;
    const MESSAGE_IDS_OFFSET: usize = 0x28;
    const MRAHISTORY_FOOTPRINT_OFFSET: usize = 0x194;

    let mrahistory_footprint: &[u8] =
        &"mrahistory_".as_bytes().iter().flat_map(|&b| vec![b, 0x00u8]).collect_vec();

    let expected_convs_count = read_u32(dbs_bytes, offsets_table[1] as usize + CONVERSATIONS_COUNT_OFFSET);
    let mut conv_id = read_u32(dbs_bytes, offsets_table[1] as usize + LAST_CONVERSATION_OFFSET);

    let mut result = vec![];

    let mut last_processed_conv_id = 0;
    let mut actual_convs_count = 0;
    while conv_id != 0 {
        let current_offset = offsets_table[conv_id as usize] as usize;
        assert!(current_offset < dbs_bytes.len());

        let len = read_u32(dbs_bytes, current_offset) as usize;
        let prev_conv_id = read_u32(dbs_bytes, current_offset + CONVERSATION_IDS_OFFSET);
        let next_conv_id = read_u32(dbs_bytes, current_offset + CONVERSATION_IDS_OFFSET + 4);

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
                [zero_byte_pos, underscore_pos].into_iter().flatten().min().unwrap()
            };
            let myself_name_utf16 = &name_slice[..separator_pos];

            // Just zero char this time
            let name_slice = &name_slice[(separator_pos + 2)..];
            let separator_pos = find_first_position(name_slice, &[0x00, 0x00], 2).unwrap();
            let other_name_utf16 = &name_slice[..separator_pos];

            let conv = MraLegacyConversation {
                offset: current_offset,
                myself_name: WStr::from_utf16le(myself_name_utf16)?,
                other_name: WStr::from_utf16le(other_name_utf16)?,
                msg_id1: u32_ptr_to_option(read_u32(dbs_bytes, current_offset + MESSAGE_IDS_OFFSET)),
                msg_id2: u32_ptr_to_option(read_u32(dbs_bytes, current_offset + MESSAGE_IDS_OFFSET + 4)),
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
    convs: Vec<MraLegacyConversation<'a>>,
) -> Result<Vec<MraLegacyConversationWithMessages<'a>>> {
    let mut result = vec![];
    for conv in convs {
        let mut msg_id_option = conv.msg_id1;
        let mut msgs = vec![];
        while let Some(msg_id) = msg_id_option {
            let header_offset = offsets_table[msg_id as usize] as usize;
            let header = {
                let header_slice = &dbs_bytes[header_offset..];
                let header_ptr = header_slice.as_ptr() as *const MraLegacyMessageHeader;
                // This is inherently unsafe. The only thing we can do is to check a magic number right after.
                let header = unsafe { header_ptr.as_ref::<'a>().unwrap() };
                require!(header.magic_number == MSG_HEADER_MAGIC_NUMBER,
                         "Incorrect header at offset {header_offset} (msg_id == {msg_id})!");
                header
            };
            let author_offset = header_offset + mem::size_of::<MraLegacyMessageHeader>();
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

            let mra_msg = MraLegacyMessage { offset: header_offset, header, text, author, payload_offset, payload };
            msgs.push(mra_msg);

            msg_id_option = u32_ptr_to_option(header.prev_id);
        }
        result.push(MraLegacyConversationWithMessages { conv, msgs });
    }

    Ok(result)
}

pub(super) fn collect_datasets(
    convs_with_msgs: &[MraLegacyConversationWithMessages<'_>],
    storage_path: &Path,
) -> Result<HashMap::<String, MraDatasetEntry>> {
    let mut result = HashMap::<String, MraDatasetEntry>::new();

    // Collecting all messages together sorted by timestamp to make sure we only deal with the last possible state
    let mut msgs_with_context = Vec::with_capacity(convs_with_msgs.iter().map(|c| c.msgs.len()).sum());
    for conv_w_msgs in convs_with_msgs.iter() {
        let myself_username = conv_w_msgs.conv.myself_name.to_utf8();
        let conv_username = conv_w_msgs.conv.other_name.to_utf8();

        result.entry(myself_username.clone()).or_insert_with(|| {
            let ds_uuid = PbUuid::random();
            let myself = User {
                ds_uuid: Some(ds_uuid.clone()),
                id: *MYSELF_ID,
                first_name_option: None,
                last_name_option: None,
                username_option: Some(myself_username.clone()),
                phone_number_option: None,
            };
            MraDatasetEntry {
                ds: Dataset { uuid: Some(ds_uuid), alias: myself_username.clone() },
                ds_root: storage_path.to_path_buf(),
                users: HashMap::from([(myself_username.clone(), myself)]),
                cwms: HashMap::new(),
            }
        });

        for mra_msg in conv_w_msgs.msgs.iter().rev() {
            let from_me = mra_msg.is_from_me()?;
            let mut from_username = if from_me { myself_username.clone() } else { conv_username.clone() };

            let tpe = mra_msg.get_tpe()?;
            macro_rules! require_format {
                ($cond:expr) => { require!($cond, "Unexpected {:?} message payload format!", tpe) };
            }
            match tpe {
                MraMessageType::ConferenceMessagePlaintext => {
                    let payload = mra_msg.payload;
                    // Text duplication
                    let payload = validate_skip_chunk(payload, mra_msg.text.as_bytes())?;
                    // Author email
                    let (author_email_bytes, payload) = next_sized_chunk(payload)?;
                    require_format!(payload.is_empty());
                    from_username = String::from_utf8(author_email_bytes.to_vec())?;
                }
                MraMessageType::ConferenceMessageRtf => {
                    let payload = mra_msg.payload;
                    // RTF
                    let (_rtf_bytes, payload) = next_sized_chunk(payload)?;
                    // RGBA bytes
                    let payload = &payload[4..];
                    // Author email (only present for others' messages)
                    require!(payload.is_empty() == mra_msg.is_from_me()?,
                             "Expected message payload to be empty for self messages only!");
                    if !mra_msg.is_from_me()? {
                        let (author_email_bytes, payload) = next_sized_chunk(payload)?;
                        require_format!(payload.is_empty());
                        from_username = String::from_utf8(author_email_bytes.to_vec())?
                    };
                }
                _ => { /* NOOP */ }
            }

            msgs_with_context.push((mra_msg, myself_username.clone(), from_username));
        }
    }
    msgs_with_context.sort_unstable_by_key(|mwc| mwc.0.header.time);

    // Iterating from the end to work on the last state
    for (mra_msg, dataset_key, from_username) in msgs_with_context.into_iter().rev() {
        let entry = result.get_mut(&dataset_key).unwrap();

        /// Create or update user by username, possibly setting a first name if it's not an email too.
        fn upsert_user(entry: &mut MraDatasetEntry,
                       username: String,
                       first_name_or_email: String) {
            let user = entry.users.entry(username.clone()).or_insert_with(|| User {
                ds_uuid: entry.ds.uuid.clone(),
                id: hash_to_id(&username),
                first_name_option: None,
                last_name_option: None,
                username_option: Some(username.clone()),
                phone_number_option: None,
            });

            if user.first_name_option.is_none() && first_name_or_email != username {
                user.first_name_option = Some(first_name_or_email);
            }
        }

        upsert_user(entry, from_username, mra_msg.author.to_utf8());

        let tpe = mra_msg.get_tpe()?;
        macro_rules! require_format {
            ($cond:expr) => { require!($cond, "Unexpected {:?} message payload format!", tpe) };
        }
        match tpe {
            MraMessageType::ConferenceUsersChange => {
                let payload = mra_msg.payload;
                // All payload is a single chunk
                let (change_tpe, payload) = next_u32(payload);

                match change_tpe {
                    CONFERENCE_USER_JOINED => {
                        let (_inviting_user_name_or_email, payload) = next_sized_chunk(payload)?;
                        let (num_invited_user_names, mut payload) = next_u32_size(payload);
                        let mut names = Vec::with_capacity(num_invited_user_names);
                        let mut emails = Vec::with_capacity(num_invited_user_names);

                        for _ in 0..num_invited_user_names {
                            let (name_bytes, payload2) = next_sized_chunk(payload)?;
                            payload = payload2;
                            names.push(WStr::from_utf16le(name_bytes)?.to_utf8());
                        }
                        let (num_invited_user_emails, mut payload) = next_u32_size(payload);
                        require_format!(num_invited_user_names == num_invited_user_emails);

                        for _ in 0..num_invited_user_names {
                            let (email_bytes, payload2) = next_sized_chunk(payload)?;
                            payload = payload2;
                            emails.push(WStr::from_utf16le(email_bytes)?.to_utf8());
                        }
                        require_format!(payload.is_empty());

                        for (email, name_or_email) in emails.into_iter().zip(names.into_iter()) {
                            upsert_user(entry, email, name_or_email);
                        }
                    }
                    CONFERENCE_USER_LEFT => {
                        let (name_bytes, payload) = next_sized_chunk(payload)?;
                        let (email_bytes, payload) = next_sized_chunk(payload)?;
                        require_format!(payload.is_empty());

                        upsert_user(entry,
                                    WStr::from_utf16le(email_bytes)?.to_utf8(),
                                    WStr::from_utf16le(name_bytes)?.to_utf8());
                    }
                    etc => bail!("Unexpected {tpe:?} change type {etc}")
                };
            }
            _ => { /* NOOP */ }
        }
    }
    Ok(result)
}

pub(super) fn convert_messages<'a>(
    convs_with_msgs: &[MraLegacyConversationWithMessages<'a>],
    mut dataset_map: HashMap<String, MraDatasetEntry>,
    _dbs_bytes: &'a [u8],
) -> Result<Vec<DatasetEntry>> {
    for conv_w_msgs in convs_with_msgs.iter() {
        let myself_username = conv_w_msgs.conv.myself_name.to_utf8();
        let conv_username = conv_w_msgs.conv.other_name.to_utf8();

        if conv_w_msgs.msgs.is_empty() {
            log::debug!("Skipping conversation between {} and {} with no messages", myself_username, conv_username);
            continue;
        }

        let entry = dataset_map.get_mut(&myself_username).unwrap();

        let mut internal_id = 0;

        let mut msgs: Vec<Message> = vec![];
        let mut ongoing_call_msg_id = None;
        let mut interlocutor_ids = HashSet::from([*MYSELF_ID]);
        for mra_msg in conv_w_msgs.msgs.iter() {
            if let Some(msg) = convert_message(mra_msg, internal_id, &myself_username, &conv_username, &entry.users,
                                               &mut msgs, &mut ongoing_call_msg_id)? {
                interlocutor_ids.insert(msg.from_id);
                msgs.push(msg);
                internal_id += 1;
            }
        }

        let member_ids = interlocutor_ids
            .into_iter()
            .sorted_by_key(|id| if *id == *MYSELF_ID { i64::MIN } else { *id })
            .collect_vec();

        let chat_type = if conv_username.ends_with("@chat.agent") || member_ids.len() > 2 {
            ChatType::PrivateGroup
        } else {
            ChatType::Personal
        };

        const AVATARS_DIR_NAME: &str = "Avatars";
        const AVATAR_FILE_NAME: &str = "avatar.jpg";
        // ICQ avatar are stored in folders with suffix ###ICQ
        let avatar_dir_name = if conv_username.chars().all(|c| c.is_numeric()) {
            format!("{conv_username}###ICQ")
        } else {
            conv_username.clone()
        };

        entry.cwms.insert(conv_username.clone(), ChatWithMessages {
            chat: Some(Chat {
                ds_uuid: entry.ds.uuid.clone(),
                id: hash_to_id(&conv_username),
                name_option: Some(conv_username), // Will be changed later
                source_type: SourceType::Mra as i32,
                tpe: chat_type as i32,
                img_path_option: Some(format!("{AVATARS_DIR_NAME}/{avatar_dir_name}/{AVATAR_FILE_NAME}")),
                member_ids,
                msg_count: msgs.len() as i32,
                main_chat_id: None,
            }),
            messages: msgs,
        });
    }

    Ok(dataset_map.into_values().sorted_by_key(|e| e.ds.alias.clone()).map(|mut entry| {
        // Now that we know all user names, rename chats accordingly
        for cwm in entry.cwms.values_mut() {
            if let Some(ref mut chat) = cwm.chat {
                let chat_email = chat.name_option.as_ref().unwrap();
                if let Some(pretty_name) = entry.users.get(chat_email).map(|u| u.pretty_name()) {
                    chat.name_option = Some(pretty_name);
                }
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
    }).collect_vec())
}

fn convert_message(
    mra_msg: &MraLegacyMessage<'_>,
    internal_id: i64,
    myself_username: &str,
    conv_username: &str,
    users: &HashMap<String, User>,
    prev_msgs: &mut [Message],
    ongoing_call_msg_id: &mut Option<i64>,
) -> Result<Option<Message>> {
    let timestamp = filetime_to_timestamp(mra_msg.header.time);

    // For a source message ID, we're using message time.
    // It's SUPPOSED to be precise enough to be unique within a chat, but in practice it's too rounded.
    // To work around that, we increment source IDs when it's duplicated.
    let source_id_option = {
        let source_id = (mra_msg.header.time / 2) as i64;
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

    let from_me = mra_msg.is_from_me()?;
    let mut from_username = (if from_me { myself_username } else { conv_username }).to_owned();

    let tpe = mra_msg.get_tpe()?;
    macro_rules! require_format {
        ($cond:expr) => { require!($cond, "Unexpected {tpe:?} message payload format!\nMessage: {mra_msg:?}") };
    }

    // TODO: Sometimes text might come encoded as cp1251 characters (wrapped in normal UTF-16 LE) seemingly at random.
    //       However, so far I observed it only once in a microblog entry, and handling this trivially didn't work.
    let text = mra_msg.text.to_utf8();
    use message::Typed;
    use message_service::SealedValueOptional as ServiceSvo;
    use content::SealedValueOptional as ContentSvo;
    let (text, typed) = match tpe {
        MraMessageType::AuthorizationRequest |
        MraMessageType::RegularPlaintext |
        MraMessageType::RegularRtf |
        MraMessageType::Sms => {
            let rtes = if tpe == MraMessageType::RegularRtf {
                let payload = mra_msg.payload;
                let (rtf_bytes, payload) = next_sized_chunk(payload)?;
                let rtf = WStr::from_utf16le(rtf_bytes)?.to_utf8();
                // RGBA bytes, ignoring
                let payload = &payload[4..];
                // Might be followed by empty bytes
                require_format!(payload.iter().all(|b| *b == 0));

                parse_rtf(&rtf)?
            } else {
                let text = replace_smiles_with_emojis(&text);
                vec![RichText::make_plain(text)]
            };

            (rtes, Typed::Regular(Default::default()))
        }
        MraMessageType::ActionNeedsNewerApp => {
            let payload = mra_msg.payload;
            let (rtf_bytes, payload) = next_sized_chunk(payload)?;
            let rtf = WStr::from_utf16le(rtf_bytes)?.to_utf8();
            // RGBA bytes, ignoring
            let payload = &payload[4..];
            require_format!(payload.is_empty());

            let rtes = parse_rtf(&rtf)?;
            (rtes, Typed::Regular(Default::default()))
        }
        MraMessageType::FileTransfer => {
            // We can get file names from the outgoing messages.
            // Mail.Ru allowed us to send several files in one message, so we unite them here.
            let text_parts = text.split('\n').collect_vec();
            let file_name_option = if text_parts.len() >= 3 {
                let file_paths: Vec<&str> = text_parts.smart_slice(1..-1).iter().map(|&s|
                    s.trim()
                        .rsplitn(3, ' ')
                        .nth(2)
                        .context("Unexpected file path format!"))
                    .try_collect()?;
                Some(file_paths.iter().join(", "))
            } else {
                None
            };
            (vec![], Typed::Regular(MessageRegular {
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
            let payload = validate_skip_chunk(payload, mra_msg.text.as_bytes())?;
            require_format!(payload.is_empty());

            const BEGIN_CONNECTING: &str = "Устанавливается соединение...";
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
            const END_O_VCANCELLED: &str = "Собеседник отменил видеозвонок";

            // MRA is not very rigid in propagating all the statuses.
            match text.as_str() {
                BEGIN_CONNECTING | BEGIN_I_CALL | BEGIN_I_VCALL | BEGIN_O_CALL | BEGIN_STARTED => {
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
                END_O_CANCELLED | END_O_VCANCELLED => {
                    if ongoing_call_msg_id.is_some_and(|id| internal_id - id <= 50) {
                        let msg_id = ongoing_call_msg_id.unwrap();
                        let msg = prev_msgs.iter_mut().rfind(|m| m.internal_id == msg_id).unwrap();
                        let start_time = msg.timestamp;
                        let discard_reason_option = match text.as_str() {
                            END_HANG | END_VHANG => None,
                            END_CONN_FAILED => Some("Failed to connect"),
                            END_I_CANCELLED | END_I_CANCELLED_2 | END_I_VCANCELLED | END_I_VCANCELLED_2 => Some("Declined by you"),
                            END_O_CANCELLED | END_O_VCANCELLED => Some("Declined by user"),
                            _ => unreachable!()
                        };
                        match msg.typed_mut() {
                            Typed::Service(MessageService { sealed_value_optional: Some(ServiceSvo::PhoneCall(call)), .. }) => {
                                call.duration_sec_option = Some((timestamp - start_time) as i32);
                                call.discard_reason_option = discard_reason_option.map(|s| s.to_owned());
                            }
                            etc => bail!("Unexpected ongoing call type: {etc:?}\nMessage: {mra_msg:?}")
                        };
                        *ongoing_call_msg_id = None;
                    }
                    // Either way, this message itself isn't supposed to have a separate entry.
                    return Ok(None);
                }
                etc => bail!("Unrecognized call message: {etc}\nMessage: {mra_msg:?}"),
            }

            (vec![], Typed::Service(MessageService {
                sealed_value_optional: Some(ServiceSvo::PhoneCall(MessageServicePhoneCall {
                    duration_sec_option: None,
                    discard_reason_option: None,
                }))
            }))
        }
        MraMessageType::BirthdayReminder => {
            let payload = mra_msg.payload;
            let payload = validate_skip_chunk(payload, mra_msg.text.as_bytes())?;
            require_format!(payload.is_empty());

            (vec![RichText::make_plain(text)], Typed::Service(MessageService {
                sealed_value_optional: Some(ServiceSvo::Notice(MessageServiceNotice {}))
            }))
        }
        MraMessageType::Cartoon | MraMessageType::CartoonType2 => {
            let payload = mra_msg.payload;
            // Source is a <SMILE> tag
            let (src_bytes, payload) = next_sized_chunk(payload)?;
            require_format!(payload.is_empty());
            let src = WStr::from_utf16le(src_bytes)?.to_utf8();
            let (_id, emoji_option) = match SMILE_TAG_REGEX.captures(&src) {
                Some(captures) => (captures.name("id").unwrap().as_str(),
                                   captures.name("alt").and_then(|smiley| smiley_to_emoji(smiley.as_str()))),
                None => bail!("Unexpected cartoon source: {src}")
            };

            (vec![], Typed::Regular(MessageRegular {
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
        MraMessageType::ConferenceUsersChange => {
            let payload = mra_msg.payload;
            // All payload is a single chunk
            let (change_tpe, payload) = next_u32(payload);

            // We don't care about user names here because they're already set by collect_datasets
            let service: ServiceSvo = match change_tpe {
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
                        emails.push(WStr::from_utf16le(email_bytes)?.to_utf8());
                    }
                    require_format!(payload.is_empty());

                    let members = emails.iter().map(|e| users[e].pretty_name()).collect_vec();
                    ServiceSvo::GroupInviteMembers(MessageServiceGroupInviteMembers { members })
                }
                CONFERENCE_USER_LEFT => {
                    let (_name_bytes, payload) = next_sized_chunk(payload)?;
                    let (email_bytes, payload) = next_sized_chunk(payload)?;
                    require_format!(payload.is_empty());

                    let email = WStr::from_utf16le(email_bytes)?.to_utf8();
                    let members = vec![users[&email].pretty_name()];
                    ServiceSvo::GroupRemoveMembers(MessageServiceGroupRemoveMembers { members })
                }
                etc => bail!("Unexpected {tpe:?} change type {etc}\nMessage: {mra_msg:?}")
            };

            (vec![], Typed::Service(MessageService { sealed_value_optional: Some(service) }))
        }
        MraMessageType::MicroblogRecordBroadcast |
        MraMessageType::MicroblogRecordDirected => {
            let payload = mra_msg.payload;
            // Text duplication
            let mut payload = validate_skip_chunk(payload, mra_msg.text.as_bytes())?;
            let target_name = if tpe == MraMessageType::MicroblogRecordDirected {
                let (target_name_bytes, payload2) = next_sized_chunk(payload)?;
                payload = payload2;
                Some(WStr::from_utf16le(target_name_bytes)?.to_utf8())
            } else { None };
            // Next 8 bytes is some timestamp we don't really care about
            let payload = &payload[8..];
            require_format!(payload.is_empty());
            convert_microblog_record(&text, target_name.as_deref())
        }
        MraMessageType::ConferenceMessagePlaintext => {
            let payload = mra_msg.payload;
            // Text duplication
            let payload = validate_skip_chunk(payload, mra_msg.text.as_bytes())?;
            // Author email
            let (author_email_bytes, payload) = next_sized_chunk(payload)?;
            from_username = String::from_utf8(author_email_bytes.to_vec())?;
            require_format!(payload.is_empty());

            let text = replace_smiles_with_emojis(&text);
            (vec![RichText::make_plain(text)], Typed::Regular(Default::default()))
        }
        MraMessageType::ConferenceMessageRtf => {
            let payload = mra_msg.payload;
            // RTF
            let (rtf_bytes, payload) = next_sized_chunk(payload)?;
            let rtf = WStr::from_utf16le(rtf_bytes)?.to_utf8();
            // RGBA bytes, ignoring
            let payload = &payload[4..];
            // Author email (only present for others' messages)
            require!(payload.is_empty() == mra_msg.is_from_me()?,
                     "Expected message payload to be empty for self messages only!\nMessage: {mra_msg:?}");
            if !mra_msg.is_from_me()? {
                let (author_email_bytes, payload) = next_sized_chunk(payload)?;
                require_format!(payload.is_empty());
                from_username = String::from_utf8(author_email_bytes.to_vec())?
            };

            let rtes = parse_rtf(&rtf)?;
            (rtes, Typed::Regular(Default::default()))
        }
        MraMessageType::LocationChange => {
            // Payload format: <name_len_u32><name><lat_len_u32><lat><lon_len_u32><lon><...>
            let payload = mra_msg.payload;
            // We observe that location name is exactly the same as the message text
            let payload = validate_skip_chunk(payload, mra_msg.text.as_bytes())?;
            // Lattitude
            let (lat_bytes, payload) = next_sized_chunk(payload)?;
            let lat_str = String::from_utf8(lat_bytes.to_vec())?;
            // Longitude
            let (lon_bytes, _payload) = next_sized_chunk(payload)?;
            let lon_str = String::from_utf8(lon_bytes.to_vec())?;

            let location = ContentLocation {
                title_option: None,
                address_option: Some(text),
                lat_str,
                lon_str,
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

    let user = &users[&from_username];
    Ok(Some(Message::new(
        internal_id,
        source_id_option,
        timestamp,
        user.id(),
        text,
        typed,
    )))
}

//
// Structs and enums
//

#[allow(dead_code)]
pub(super) struct MraLegacyConversation<'a> {
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

#[repr(C, packed)]
#[derive(Debug)]
struct MraLegacyMessageHeader {
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

#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromPrimitive)]
enum MraMessageType {
    RegularPlaintext = 0x02,
    AuthorizationRequest = 0x04,
    ActionNeedsNewerApp = 0x06,
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
}

struct MraLegacyMessage<'a> {
    /// Offset at which header begins
    offset: usize,
    header: &'a MraLegacyMessageHeader,
    text: &'a WStr<LE>,
    author: &'a WStr<LE>,
    payload_offset: usize,
    /// Exact interpretation depends on the message type
    payload: &'a [u8],
}

impl Debug for MraLegacyMessage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let mut formatter = formatter.debug_struct("MraMessage");
        formatter.field("offset", &format!("{:#010x}", self.offset));
        let tpe_u32 = self.header.tpe_u32;
        let tpe_option: Option<MraMessageType> = FromPrimitive::from_u32(tpe_u32);
        match tpe_option {
            Some(tpe) =>
                formatter.field("type", &tpe),
            None => {
                formatter.field("type", &format!("UNKNOWN ({tpe_u32:#04x})"))
            }
        };
        formatter.field("author", &self.author.to_utf8());
        formatter.field("text", &self.text.to_utf8());
        formatter.field("payload_offset", &format!("{:#010x}", self.payload_offset));
        formatter.field("payload", &bytes_to_pretty_string(self.payload, usize::MAX));
        formatter.finish()
    }
}

impl MraLegacyMessage<'_> {
    fn get_tpe(&self) -> Result<MraMessageType> {
        let tpe_u32 = self.header.tpe_u32;
        FromPrimitive::from_u32(tpe_u32)
            .with_context(|| format!("Unknown message type: {}\nMessage: {:?}", tpe_u32, self))
    }

    fn is_from_me(&self) -> Result<bool> {
        match self.header.flag_outgoing {
            0 => Ok(false),
            1 => Ok(true),
            x => err!("Invalid flag_incoming value {x}\nMessage: {:?}", self),
        }
    }

    #[allow(dead_code)]
    fn debug_format_bytes(&self, file_bytes: &[u8]) -> String {
        const COLUMNS: usize = 32;
        const ROWS_TO_TAKE: usize = 10;
        let upper_bound = self.offset + ROWS_TO_TAKE * COLUMNS;
        let upper_bound = cmp::min(upper_bound, file_bytes.len());
        bytes_to_pretty_string(&file_bytes[self.offset..upper_bound], COLUMNS)
    }
}

pub(super) struct MraLegacyConversationWithMessages<'a> {
    conv: MraLegacyConversation<'a>,
    msgs: Vec<MraLegacyMessage<'a>>,
}
