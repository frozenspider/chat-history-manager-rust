#![allow(dead_code)]

use const_format::concatcp;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::*;
use uuid::Uuid;

use crate::protobuf::history::message::Typed;

use super::*;

pub fn serialize_arr(v: &[String]) -> Option<String> {
    if v.is_empty() { None } else { Some(v.iter().join(";;;")) }
}

pub fn deserialize_arr(v: String) -> Vec<String> {
    if v.is_empty() { vec![] } else { v.split(";;;").map(|v| v.to_owned()).collect() }
}

fn serialize_bool(b: bool) -> i32 {
    if b { 1 } else { 0 }
}

fn deserialize_bool(bi: i32) -> bool {
    match bi {
        0 => false,
        1 => true,
        _ => panic!("Unexpected boolean integer {bi}!") // In THAT case it probably makes sense to panic
    }
}

pub trait EnumSerialization: Sized {
    fn serialize(v: i32) -> Result<String>;

    fn deserialize(v: &str) -> Result<i32>;
}

macro_rules! impl_enum_serialization {
    ($self:ident, {$($key:ident=>$value:literal),+}) =>
    {
        impl EnumSerialization for $self {
            fn serialize(v: i32) -> Result<String> {
                Ok(match $self::resolve(v)? {
                    $($self::$key => $value),+
                }.to_owned())
            }

            fn deserialize(v: &str) -> Result<i32> {
                Ok(match v {
                    $($value => $self::$key),+,
                    x => bail!("Unrecognized {} {x}", stringify!($self)),
                } as i32)
            }
        }
    };
}

impl_enum_serialization!(SourceType, {
    TextImport  => "text_import",
    Telegram    => "telegram",
    WhatsappDb  => "whatsapp",
    TinderDb    => "tinder",
    BadooDb     => "badoo"
});

impl_enum_serialization!(ChatType, {
    Personal     => "personal",
    PrivateGroup => "private_group"
});

//
// Per-entity serialization
//

pub mod dataset {
    use super::*;

    pub fn deserialize(raw: RawDataset) -> Result<Dataset> {
        Ok(Dataset {
            uuid: Some(PbUuid { value: Uuid::from_slice(&raw.uuid)?.to_string() }),
            alias: raw.alias,
        })
    }

    pub fn serialize(ds: &Dataset) -> RawDataset {
        let uuid = Uuid::parse_str(&ds.uuid.as_ref().unwrap().value).expect("Invalid UUID!");
        RawDataset {
            uuid: Vec::from(uuid.as_ref()),
            alias: ds.alias.clone(),
        }
    }
}

pub mod user {
    use super::*;

    pub fn deserialize(raw: RawUser) -> Result<(User, bool)> {
        Ok((User {
            ds_uuid: Some(PbUuid { value: Uuid::from_slice(&raw.ds_uuid)?.to_string() }),
            id: raw.id,
            first_name_option: raw.first_name,
            last_name_option: raw.last_name,
            username_option: raw.username,
            phone_number_option: raw.phone_numbers,
        }, deserialize_bool(raw.is_myself)))
    }

    pub fn serialize(user: &User, is_myself: bool, raw_uuid: &[u8]) -> RawUser {
        RawUser {
            ds_uuid: raw_uuid.to_vec(),
            id: user.id,
            first_name: user.first_name_option.clone(),
            last_name: user.last_name_option.clone(),
            username: user.username_option.clone(),
            phone_numbers: user.phone_number_option.clone(),
            is_myself: serialize_bool(is_myself),
        }
    }
}

pub mod chat {
    use super::*;

    const SELECT: &str =
        r#"SELECT
                c.*,
                c.type as tpe,
                (
                  SELECT GROUP_CONCAT(user_id) FROM (
                    SELECT cm.user_id FROM chat_member cm
                    WHERE cm.ds_uuid = c.ds_uuid AND cm.chat_id = c.id
                    ORDER BY cm."order"
                  )
                ) AS member_ids,
                (
                  SELECT MAX(internal_id) FROM message m
                  WHERE m.ds_uuid = c.ds_uuid AND m.chat_id = c.id
                ) AS last_message_internal_id
            FROM chat c"#;
    const DS_IS: &str = "c.ds_uuid = ?";
    const ID_IS: &str = "c.id = ?";
    const ORDER: &str = "ORDER BY c.id ASC";

    const SELECT_BY_DS_SQL: &str = concatcp!(SELECT, " WHERE ", DS_IS, " ", ORDER);
    const SELECT_BY_DS_AND_ID_SQL: &str = concatcp!(SELECT, " WHERE ", DS_IS, " AND ", ID_IS, " ", ORDER);

    pub fn select_by_ds(ds_uuid: &Uuid,
                        conn: &mut SqliteConnection) -> Result<Vec<RawChatQ>> {
        Ok(sql_query(SELECT_BY_DS_SQL)
            .bind::<Binary, _>(ds_uuid.as_ref())
            .load::<RawChatQ>(conn)?)
    }

    pub fn select_by_ds_and_id<'a>(ds_uuid: &'a Uuid,
                                   id: i64,
                                   conn: &'a mut SqliteConnection) -> Result<Vec<RawChatQ>> {
        Ok(sql_query(SELECT_BY_DS_AND_ID_SQL)
            .bind::<Binary, _>(ds_uuid.as_ref())
            .bind::<BigInt, _>(id)
            .load::<RawChatQ>(conn)?)
    }

    pub fn serialize(chat: &Chat, raw_uuid: &[u8]) -> Result<RawChat> {
        Ok(RawChat {
            ds_uuid: raw_uuid.to_vec(),
            id: chat.id,
            name: chat.name_option.clone(),
            source_type: SourceType::serialize(chat.source_type)?,
            tpe: ChatType::serialize(chat.tpe)?,
            img_path: chat.img_path_option.clone(),
            msg_count: chat.msg_count,
            main_chat_id: None,
        })
    }

    pub fn deserialize(raw: RawChatQ,
                       conn: &mut SqliteConnection,
                       ds_uuid: &PbUuid,
                       cache: &DaoCacheInner) -> Result<ChatWithDetails> {
        let last_msg_option =
            transpose_option_result(raw.last_message_internal_id.map(|last_message_internal_id| {
                Ok(message::fetch(conn, |conn| {
                    Ok(schema::message::table
                        .filter(schema::message::columns::internal_id.eq(last_message_internal_id))
                        .left_join(schema::message_content::table)
                        .select((RawMessage::as_select(), Option::<RawMessageContent>::as_select()))
                        .load(conn)?)
                })?.remove(0))
            }))?;
        let mut cwd = ChatWithDetails {
            chat: Chat {
                ds_uuid: Some(ds_uuid.clone()),
                id: raw.chat.id,
                name_option: raw.chat.name,
                source_type: SourceType::deserialize(raw.chat.source_type.as_str())?,
                tpe: ChatType::deserialize(raw.chat.tpe.as_str())?,
                img_path_option: raw.chat.img_path,
                member_ids: raw.member_ids
                    .map(|s| s.split(',').map(|s| s.parse::<i64>()).try_collect())
                    .unwrap_or(Ok(vec![]))?,
                msg_count: raw.chat.msg_count,
                main_chat_id: raw.chat.main_chat_id,
            },
            last_msg_option,
            members: vec![] /* Will be set right next */,
        };
        cwd.members = resolve_users(&cache.users[ds_uuid], cwd.chat.member_ids())?;
        Ok(cwd)
    }

    fn resolve_users(cache: &UserCacheForDataset, user_ids: impl Iterator<Item=UserId>) -> Result<Vec<User>> {
        Ok(user_ids
            .map(|id|
                cache.user_by_id.get(&id)
                    .cloned()
                    .with_context(|| format!("Cannot find user with ID {}", *id))
            )
            .try_collect::<_, Vec<_>, _>()?
            .into_iter()
            .sorted_by_key(|u| if u.id == cache.myself.id { i64::MIN } else { u.id })
            .collect_vec())
    }
}

pub mod message {
    use super::*;

    // Sadly, I couldn't figure out how to type only the .filter(...).offset(...).limit(...) part to make it into a
    // clousre, typechecker went into infinite recursion.
    // As such, more boilerplate is needed now.
    pub fn fetch<F>(conn: &mut SqliteConnection,
                    get_raw_messages_with_content: F) -> Result<Vec<Message>>
        where F: Fn(&mut SqliteConnection) -> Result<Vec<(RawMessage, Option<RawMessageContent>)>>
    {
        let raw_messages_with_content: Vec<(RawMessage, Option<RawMessageContent>)> =
            get_raw_messages_with_content(conn)?;

        let raw_messages: Vec<&RawMessage> =
            raw_messages_with_content.iter().map(|pair| &pair.0).collect();

        let raw_message_rtes: Vec<RawRichTextElement> =
            RawRichTextElement::belonging_to(&raw_messages)
                .select(RawRichTextElement::as_select())
                .load(conn)?;

        let mut grouped = raw_message_rtes.grouped_by(&raw_messages);
        for group in grouped.iter_mut() {
            // TODO: This may be redundant
            group.sort_by_key(|rte| rte.id)
        }

        let messages: Vec<Message> = grouped.into_iter()
            .zip(raw_messages_with_content)
            .map(|(rtes, (m, mc))| FullRawMessage { m, mc, rtes })
            .map(deserialize)
            .try_collect()?;

        Ok(messages)
    }

    /// Discards message internal ID.
    pub fn serialize_and_copy_files(m: &Message,
                                    chat_id: i64,
                                    raw_uuid: &[u8],
                                    src_ds_root: &DatasetRoot,
                                    dst_ds_root: &DatasetRoot) -> Result<FullRawMessage> {
        let (tpe, subtype, mc, time_edited, is_deleted, forward_from_name, reply_to_message_id) =
            match m.typed.as_ref().unwrap() {
                Typed::Regular(mr) => {
                    let content = mr.content_option.as_ref()
                        .map(|mc| serialize_content_and_copy_files(mc.sealed_value_optional.as_ref().unwrap(),
                                                                   chat_id, src_ds_root, dst_ds_root)).transpose()?;
                    ("regular",
                     None,
                     content,
                     mr.edit_timestamp_option,
                     serialize_bool(mr.is_deleted),
                     mr.forward_from_name_option.clone(),
                     mr.reply_to_message_id_option)
                }
                Typed::Service(MessageService { sealed_value_optional: ms }) => {
                    let (subtype, mc) = serialize_service_and_copy_files(ms.as_ref().unwrap(),
                                                                         chat_id, src_ds_root, dst_ds_root)?;
                    ("service", Some(subtype), mc, None, serialize_bool(false), None, None)
                }
            };
        Ok(FullRawMessage {
            m: RawMessage {
                internal_id: None, // Discarded
                ds_uuid: Vec::from(raw_uuid),
                chat_id,
                source_id: m.source_id_option,
                tpe: tpe.to_owned(),
                subtype: subtype.map(|s| s.to_owned()),
                time_sent: m.timestamp,
                time_edited,
                is_deleted,
                from_id: m.from_id,
                forward_from_name,
                reply_to_message_id,
                searchable_string: m.searchable_string.clone(),
            },
            mc,
            rtes: m.text.iter().map(serialize_rte).try_collect()?,
        })
    }

    fn serialize_content_and_copy_files(mc: &content::SealedValueOptional,
                                        chat_id: i64,
                                        src_ds_root: &DatasetRoot,
                                        dst_ds_root: &DatasetRoot) -> Result<RawMessageContent> {
        use content::SealedValueOptional::*;
        macro_rules! copy_path {
            ($obj:ident.$field:ident, $thumb:expr, $subpath:expr) => {
                $obj.$field.as_ref().map(|v|
                    copy_file(&v, $thumb, $subpath,chat_id, src_ds_root, dst_ds_root)
                ).transpose()?.flatten()
            };
        }
        Ok(match mc {
            Sticker(v) => {
                let path = copy_path!(v.path_option, &None, &subpaths::STICKERS);
                let thumbnail_path = copy_path!(v.thumbnail_path_option, &path, &subpaths::STICKERS);
                RawMessageContent {
                    element_type: "sticker".to_owned(),
                    path,
                    width: Some(v.width),
                    height: Some(v.height),
                    thumbnail_path,
                    emoji: v.emoji_option.clone(),
                    ..Default::default()
                }
            }
            Photo(v) => serialize_photo_and_copy_files(v, chat_id, src_ds_root, dst_ds_root)?,
            VoiceMsg(v) => {
                let path = copy_path!(v.path_option, &None, &subpaths::VOICE_MESSAGES);
                RawMessageContent {
                    element_type: "voice_message".to_owned(),
                    path,
                    mime_type: Some(v.mime_type.clone()),
                    duration_sec: v.duration_sec_option,
                    ..Default::default()
                }
            }
            Audio(v) => {
                let path = copy_path!(v.path_option, &None, &subpaths::AUDIOS);
                let thumbnail_path = copy_path!(v.thumbnail_path_option, &path, &subpaths::AUDIOS);
                RawMessageContent {
                    element_type: "audio".to_owned(),
                    path,
                    title: v.title_option.clone(),
                    performer: v.performer_option.clone(),
                    mime_type: Some(v.mime_type.clone()),
                    duration_sec: v.duration_sec_option,
                    thumbnail_path,
                    ..Default::default()
                }
            }
            VideoMsg(v) => {
                let path = copy_path!(v.path_option, &None, &subpaths::VIDEO_MESSAGES);
                let thumbnail_path = copy_path!(v.thumbnail_path_option, &path, &subpaths::VIDEO_MESSAGES);
                RawMessageContent {
                    element_type: "video_message".to_owned(),
                    path,
                    width: Some(v.width),
                    height: Some(v.height),
                    mime_type: Some(v.mime_type.clone()),
                    duration_sec: v.duration_sec_option,
                    thumbnail_path,
                    is_one_time: Some(serialize_bool(v.is_one_time)),
                    ..Default::default()
                }
            }
            Video(v) => {
                let path = copy_path!(v.path_option, &None, &subpaths::VIDEOS);
                let thumbnail_path = copy_path!(v.thumbnail_path_option, &path, &subpaths::VIDEOS);
                RawMessageContent {
                    element_type: "video".to_owned(),
                    path,
                    title: v.title_option.clone(),
                    performer: v.performer_option.clone(),
                    width: Some(v.width),
                    height: Some(v.height),
                    mime_type: Some(v.mime_type.clone()),
                    duration_sec: v.duration_sec_option,
                    thumbnail_path,
                    is_one_time: Some(serialize_bool(v.is_one_time)),
                    ..Default::default()
                }
            }
            File(v) => {
                let path = copy_path!(v.path_option, &None, &subpaths::FILES);
                let thumbnail_path = copy_path!(v.thumbnail_path_option, &path, &subpaths::FILES);
                RawMessageContent {
                    element_type: "file".to_owned(),
                    path,
                    title: v.file_name_option.clone(),
                    mime_type: v.mime_type_option.clone(),
                    thumbnail_path,
                    ..Default::default()
                }
            }
            Location(v) => RawMessageContent {
                element_type: "location".to_owned(),
                title: v.title_option.clone(),
                address: v.address_option.clone(),
                lat: Some(v.lat_str.clone()),
                lon: Some(v.lon_str.clone()),
                duration_sec: v.duration_sec_option,
                ..Default::default()
            },
            Poll(v) => RawMessageContent {
                element_type: "poll".to_owned(),
                poll_question: Some(v.question.clone()),
                ..Default::default()
            },
            SharedContact(v) => {
                let path = copy_path!(v.vcard_path_option, &None, &subpaths::FILES);
                RawMessageContent {
                    element_type: "shared_contact".to_owned(),
                    path,
                    first_name: v.first_name_option.clone(),
                    last_name: v.last_name_option.clone(),
                    phone_number: v.phone_number_option.clone(),
                    ..Default::default()
                }
            }
        })
    }

    fn serialize_photo_and_copy_files(photo: &ContentPhoto,
                                      chat_id: i64,
                                      src_ds_root: &DatasetRoot,
                                      dst_ds_root: &DatasetRoot) -> Result<RawMessageContent> {
        let path = photo.path_option.as_ref().map(|path|
            copy_file(path, &None, &subpaths::PHOTOS,
                      chat_id, src_ds_root, dst_ds_root)
        ).transpose()?.flatten();
        Ok(RawMessageContent {
            element_type: "photo".to_owned(),
            path,
            width: Some(photo.width),
            height: Some(photo.height),
            is_one_time: Some(serialize_bool(photo.is_one_time)),
            ..Default::default()
        })
    }

    fn serialize_service_and_copy_files(ms: &message_service::SealedValueOptional,
                                        chat_id: i64,
                                        src_ds_root: &DatasetRoot,
                                        dst_ds_root: &DatasetRoot) -> Result<(&'static str, Option<RawMessageContent>)> {
        use message_service::SealedValueOptional::*;
        let (subtype, mut mc) = match ms {
            PhoneCall(v) =>
                ("phone_call", Some(RawMessageContent {
                    duration_sec: v.duration_sec_option,
                    discard_reason: v.discard_reason_option.clone(),
                    ..Default::default()
                })),
            SuggestProfilePhoto(v) =>
                ("suggest_profile_photo",
                 v.photo.as_ref()
                     .map(|p| serialize_photo_and_copy_files(p, chat_id, src_ds_root, dst_ds_root))
                     .transpose()?),
            PinMessage(v) =>
                ("pin_message", Some(RawMessageContent {
                    pinned_message_id: Some(v.message_id),
                    ..Default::default()
                })),
            ClearHistory(_) =>
                ("clear_history", None),
            BlockUser(v) =>
                ("block_user", Some(RawMessageContent {
                    is_blocked: Some(serialize_bool(v.is_blocked)),
                    ..Default::default()
                })),
            GroupCreate(v) =>
                ("group_create", Some(RawMessageContent {
                    title: Some(v.title.clone()),
                    members: serialize_arr(&v.members),
                    ..Default::default()
                })),
            GroupEditTitle(v) =>
                ("group_edit_title", Some(RawMessageContent {
                    title: Some(v.title.clone()),
                    ..Default::default()
                })),
            GroupEditPhoto(v) =>
                ("group_edit_photo",
                 v.photo.as_ref().map(|p| serialize_photo_and_copy_files(p, chat_id, src_ds_root, dst_ds_root))
                     .transpose()?),
            GroupDeletePhoto(_) =>
                ("group_delete_photo", None),
            GroupInviteMembers(v) =>
                ("group_invite_members", Some(RawMessageContent {
                    members: serialize_arr(&v.members),
                    ..Default::default()
                })),
            GroupRemoveMembers(v) =>
                ("group_remove_members", Some(RawMessageContent {
                    members: serialize_arr(&v.members),
                    ..Default::default()
                })),
            GroupMigrateFrom(v) =>
                ("group_migrate_from", Some(RawMessageContent {
                    title: Some(v.title.clone()),
                    ..Default::default()
                })),
            GroupMigrateTo(_) =>
                ("group_migrate_to", None),
            GroupCall(v) =>
                ("group_call", Some(RawMessageContent {
                    members: serialize_arr(&v.members),
                    ..Default::default()
                })),
        };

        if let Some(ref mut mc) = mc {
            mc.element_type = subtype.to_owned()
        }

        Ok((subtype, mc))
    }


    /// Ignores message internal ID.
    fn serialize_rte(rte: &RichTextElement) -> Result<RawRichTextElement> {
        use rich_text_element::Val::*;
        let (mut language, mut hidden, mut href) = (None, None, None);
        let (text, tpe): (Option<String>, &str) = match rte.val.as_ref().unwrap() {
            Plain(v) =>
                (Some(v.text.clone()), "plain"),
            Bold(v) =>
                (Some(v.text.clone()), "bold"),
            Italic(v) =>
                (Some(v.text.clone()), "italic"),
            Underline(v) =>
                (Some(v.text.clone()), "underline"),
            Strikethrough(v) =>
                (Some(v.text.clone()), "strikethrough"),
            Link(v) => {
                hidden = Some(serialize_bool(v.hidden));
                href = Some(v.href.clone());
                (v.text_option.clone(), "link")
            }
            PrefmtInline(v) =>
                (Some(v.text.clone()), "prefmt_inline"),
            PrefmtBlock(v) => {
                language = v.language_option.clone();
                (Some(v.text.clone()), "prefmt_block")
            }
            Blockquote(v) =>
                (Some(v.text.clone()), "blockquote"),
            Spoiler(v) =>
                (Some(v.text.clone()), "spoiler"),
        };
        Ok(RawRichTextElement {
            id: None,
            message_internal_id: None, // Discarded
            element_type: tpe.to_owned(),
            text,
            href,
            hidden,
            language,
        })
    }

    pub fn deserialize(raw: FullRawMessage) -> Result<Message> {
        let text = raw.rtes.into_iter().map(deserialize_rte).try_collect()?;
        let typed = match raw.m.tpe.as_str() {
            "regular" => Typed::Regular(MessageRegular {
                edit_timestamp_option: raw.m.time_edited,
                is_deleted: deserialize_bool(raw.m.is_deleted),
                forward_from_name_option: raw.m.forward_from_name,
                reply_to_message_id_option: raw.m.reply_to_message_id,
                content_option: raw.mc.map(|mc| Ok::<_, anyhow::Error>(Content {
                    sealed_value_optional: Some(deserialize_content(mc)?)
                })).transpose()?,
            }),
            "service" => Typed::Service(MessageService {
                sealed_value_optional: Some(deserialize_service(
                    raw.m.subtype.as_deref().expect("Service message subtype is empty!"),
                    raw.mc)?)
            }),
            tpe => bail!("Unknown message type {}!", tpe)
        };
        Ok(Message::new(
            raw.m.internal_id.expect("Message has no internal ID!"),
            raw.m.source_id,
            raw.m.time_sent,
            raw.m.from_id,
            text,
            typed,
        ))
    }

    fn deserialize_content(raw: RawMessageContent) -> Result<content::SealedValueOptional> {
        use content::SealedValueOptional::*;
        macro_rules! get_or_bail {
                ($obj:ident.$field:ident) => {
                    $obj.$field.with_context(|| format!("{} field was missing for a {} content!",
                                                        stringify!($field), raw.element_type))? };
            }
        Ok(match raw.element_type.as_str() {
            "sticker" => Sticker(ContentSticker {
                path_option: raw.path,
                width: get_or_bail!(raw.width),
                height: get_or_bail!(raw.height),
                thumbnail_path_option: raw.thumbnail_path,
                emoji_option: raw.emoji,
            }),
            "photo" => Photo(deserialize_photo(raw)?),
            "voice_message" => VoiceMsg(ContentVoiceMsg {
                path_option: raw.path,
                mime_type: get_or_bail!(raw.mime_type),
                duration_sec_option: raw.duration_sec,
            }),
            "audio" => Audio(ContentAudio {
                path_option: raw.path,
                title_option: raw.title,
                performer_option: raw.performer,
                mime_type: get_or_bail!(raw.mime_type),
                duration_sec_option: raw.duration_sec,
                thumbnail_path_option: raw.thumbnail_path,
            }),
            "video_message" => VideoMsg(ContentVideoMsg {
                path_option: raw.path,
                width: get_or_bail!(raw.width),
                height: get_or_bail!(raw.height),
                mime_type: get_or_bail!(raw.mime_type),
                duration_sec_option: raw.duration_sec,
                thumbnail_path_option: raw.thumbnail_path,
                is_one_time: deserialize_bool(get_or_bail!(raw.is_one_time)),
            }),
            "video" => Video(ContentVideo {
                path_option: raw.path,
                title_option: raw.title,
                performer_option: raw.performer,
                width: get_or_bail!(raw.width),
                height: get_or_bail!(raw.height),
                mime_type: get_or_bail!(raw.mime_type),
                duration_sec_option: raw.duration_sec,
                thumbnail_path_option: raw.thumbnail_path,
                is_one_time: deserialize_bool(get_or_bail!(raw.is_one_time)),
            }),
            "file" => File(ContentFile {
                path_option: raw.path,
                file_name_option: raw.title,
                mime_type_option: raw.mime_type,
                thumbnail_path_option: raw.thumbnail_path,
            }),
            "location" => Location(ContentLocation {
                title_option: raw.title,
                address_option: raw.address,
                lat_str: get_or_bail!(raw.lat),
                lon_str: get_or_bail!(raw.lon),
                duration_sec_option: raw.duration_sec,
            }),
            "poll" => Poll(ContentPoll {
                question: get_or_bail!(raw.poll_question),
            }),
            "shared_contact" => SharedContact(ContentSharedContact {
                first_name_option: raw.first_name,
                last_name_option: raw.last_name,
                phone_number_option: raw.phone_number,
                vcard_path_option: raw.path,
            }),
            tpe => bail!("Unknown content type {}!", tpe)
        })
    }

    fn deserialize_photo(raw: RawMessageContent) -> Result<ContentPhoto> {
        macro_rules! get_or_bail {
                ($obj:ident.$field:ident) => {
                    $obj.$field.with_context(|| format!("{} field was missing for a photo!", stringify!($field)))? };
            }
        Ok(ContentPhoto {
            path_option: raw.path,
            width: get_or_bail!(raw.width),
            height: get_or_bail!(raw.height),
            is_one_time: deserialize_bool(get_or_bail!(raw.is_one_time)),
        })
    }

    fn deserialize_service(subtype: &str, raw: Option<RawMessageContent>)
                           -> Result<message_service::SealedValueOptional> {
        use message_service::SealedValueOptional::*;
        macro_rules! raw_or_bail {
                () => { raw.with_context(|| format!("Message content was not present for a {} service message!",
                                                    subtype))? };
            }
        macro_rules! get_or_bail {
                ($obj:ident.$field:ident) => {
                    $obj.$field.with_context(|| format!("{} field was missing for a {} service message!",
                                                        stringify!($field), subtype))? };
            }
        Ok(match subtype {
            "phone_call" => {
                let raw = raw_or_bail!();
                PhoneCall(MessageServicePhoneCall {
                    duration_sec_option: raw.duration_sec,
                    discard_reason_option: raw.discard_reason,
                })
            }
            "suggest_profile_photo" =>
                SuggestProfilePhoto(MessageServiceSuggestProfilePhoto {
                    photo: raw.map(deserialize_photo).transpose()?,
                }),
            "pin_message" => {
                let raw = raw_or_bail!();
                PinMessage(MessageServicePinMessage {
                    message_id: get_or_bail!(raw.pinned_message_id),
                })
            }
            "clear_history" =>
                ClearHistory(MessageServiceClearHistory {}),
            "block_user" => {
                let raw = raw_or_bail!();
                BlockUser(MessageServiceBlockUser {
                    is_blocked: deserialize_bool(get_or_bail!(raw.is_blocked)),
                })
            }
            "group_create" => {
                let raw = raw_or_bail!();
                GroupCreate(MessageServiceGroupCreate {
                    title: get_or_bail!(raw.title),
                    members: raw.members.map(deserialize_arr).unwrap_or_default(),
                })
            }
            "group_edit_title" => {
                let raw = raw_or_bail!();
                GroupEditTitle(MessageServiceGroupEditTitle {
                    title: get_or_bail!(raw.title),
                })
            }
            "group_edit_photo" =>
                GroupEditPhoto(MessageServiceGroupEditPhoto {
                    photo: raw.map(deserialize_photo).transpose()?,
                }),
            "group_delete_photo" =>
                GroupDeletePhoto(MessageServiceGroupDeletePhoto {}),
            "group_invite_members" => {
                let raw = raw_or_bail!();
                GroupInviteMembers(MessageServiceGroupInviteMembers {
                    members: deserialize_arr(get_or_bail!(raw.members)),
                })
            }
            "group_remove_members" => {
                let raw = raw_or_bail!();
                GroupRemoveMembers(MessageServiceGroupRemoveMembers {
                    members: deserialize_arr(get_or_bail!(raw.members)),
                })
            }
            "group_migrate_from" => {
                let raw = raw_or_bail!();
                GroupMigrateFrom(MessageServiceGroupMigrateFrom {
                    title: get_or_bail!(raw.title),
                })
            }
            "group_migrate_to" =>
                GroupMigrateTo(MessageServiceGroupMigrateTo {}),
            "group_call" => {
                let raw = raw_or_bail!();
                GroupCall(MessageServiceGroupCall {
                    members: deserialize_arr(get_or_bail!(raw.members)),
                })
            }
            subtype => bail!("Unknown service message subtype {}!", subtype)
        })
    }

    fn deserialize_rte(raw: RawRichTextElement) -> Result<RichTextElement> {
        macro_rules! text_or_bail {
                () => { raw.text.with_context(|| format!("Text not found for a rich text element #{} ({})!",
                                                         raw.id.unwrap(), raw.element_type))? };
            }
        Ok(match raw.element_type.as_str() {
            "plain" => RichText::make_plain(text_or_bail!()),
            "bold" => RichText::make_bold(text_or_bail!()),
            "italic" => RichText::make_italic(text_or_bail!()),
            "underline" => RichText::make_underline(text_or_bail!()),
            "strikethrough" => RichText::make_strikethrough(text_or_bail!()),
            "link" => RichText::make_link(raw.text,
                                          raw.href.with_context(|| format!("Link has no href!"))?,
                                          raw.hidden.map(deserialize_bool).unwrap_or_default()),
            "prefmt_inline" => RichText::make_prefmt_inline(text_or_bail!()),
            "prefmt_block" => RichText::make_prefmt_block(text_or_bail!(), raw.language),
            "blockquote" => RichText::make_blockquote(text_or_bail!()),
            "spoiler" => RichText::make_spoiler(text_or_bail!()),
            x => bail!("Unknown rich text element {x}!")
        })
    }
}
