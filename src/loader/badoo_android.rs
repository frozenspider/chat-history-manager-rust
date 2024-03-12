use std::fs;

use rusqlite::Connection;
use simd_json::prelude::*;

use crate::loader::DataLoader;
use crate::prelude::*;

use super::*;

#[cfg(test)]
#[path = "badoo_android_tests.rs"]
mod tests;

pub struct BadooAndroidDataLoader;

android_sqlite_loader!(BadooAndroidDataLoader, BadooDb, "Badoo", "ChatComDatabase");

/// Using a first legal ID (i.e. "1") for myself
const MYSELF_ID: UserId = UserId(UserId::INVALID.0 + 1);

type EncUserId = String;

#[derive(Default)]
struct Users {
    user_id_to_encrypted: HashMap<UserId, EncUserId>,
    user_id_to_user: HashMap<UserId, User>,
}

impl Users {
    fn resolve_encrypted(&self, user_id: UserId) -> Result<&EncUserId> {
        self.user_id_to_encrypted.get(&user_id)
            .with_context(|| format!("Couldn't resolve encrypted user ID for user ID {}", *user_id))
    }
}

impl BadooAndroidDataLoader {
    fn tweak_conn(&self, path: &Path, conn: &Connection) -> EmptyRes {
        conn.execute(r#"ATTACH DATABASE ?1 AS conn_db"#, [path_to_str(&path.join("CombinedConnectionsDatabase"))?])?;
        Ok(())
    }

    fn parse_users(&self, conn: &Connection, ds_uuid: &PbUuid) -> Result<Users> {
        let mut users: Users = Default::default();

        // We can get own encrypted ID from messages table where is_incoming = 0, but no reason to do so.
        // Also, not sure how to decrypt it.
        users.user_id_to_user.insert(MYSELF_ID, User {
            ds_uuid: ds_uuid.clone(),
            id: *MYSELF_ID,
            first_name_option: Some("Me".to_owned()), // No way to know your own name, sadly
            last_name_option: None,
            username_option: None,
            phone_number_option: None,
        });

        let mut stmt = conn.prepare(r"SELECT * FROM conversation_info WHERE conversation_type = 'User'")?;
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let id = row.get::<_, String>("user_id")?.parse::<i64>()?;
            let id = UserId(id);

            let enc_id = row.get::<_, String>("encrypted_user_id")?;
            ensure!(users.user_id_to_encrypted.insert(id, enc_id).is_none(),
                    "Duplicate encrypted user ID for user {}!", *id);

            let name = row.get::<_, String>("user_name")?;

            users.user_id_to_user.insert(id, User {
                ds_uuid: ds_uuid.clone(),
                id: *id,
                first_name_option: Some(name),
                last_name_option: None,
                username_option: None,
                phone_number_option: None,
            });
        }

        Ok(users)
    }

    fn normalize_users(&self, users: Users, _cwms: &[ChatWithMessages]) -> Result<Vec<User>> {
        let mut users = users.user_id_to_user.into_values().collect_vec();
        // Set myself to be a first member.
        users.sort_by_key(|u| if u.id == *MYSELF_ID { *UserId::MIN } else { u.id });
        Ok(users)
    }

    fn parse_chats(&self, conn: &Connection, ds_uuid: &PbUuid, users: &Users, path: &Path) -> Result<Vec<ChatWithMessages>> {
        let mut cwms = vec![];

        let downloaded_media_path = path.join(RELATIVE_MEDIA_DIR);
        fs::create_dir_all(downloaded_media_path)?;

        let mut stmt = conn.prepare(r"
            SELECT *
            FROM message
            WHERE sender_id = ?
            OR recipient_id = ?
            ORDER BY created_timestamp ASC
        ")?;

        for (user_id, user) in users.user_id_to_user.iter() {
            if *user_id == MYSELF_ID { continue; }

            let enc_user_id = users.resolve_encrypted(*user_id)?;
            let mut rows = stmt.query([enc_user_id, enc_user_id])?;

            let mut messages = vec![];
            while let Some(row) = rows.next()? {
                let from_id = if row.get::<_, i8>("is_incoming")? == 1 { *user_id } else { MYSELF_ID };

                let source_id: i64 = row.get::<_, String>("id")?.parse()?;
                let reply_to_message_id_option = row.get::<_, Option<String>>("reply_to_id")?;
                let reply_to_message_id_option: Option<i64> =
                    transpose_option_std_result(reply_to_message_id_option.map(|s| s.parse()))?;

                let timestamp = row.get::<_, i64>("created_timestamp")? / 1000;
                // TODO: if created_timestamp <> modified_timestamp, does it really mean message was edited?

                // While URLs are known, following them without setting headers results in 403.
                let (text, content_option) = {
                    let payload_json = row.get::<_, String>("payload")?;
                    let mut payload_bytes_vec = payload_json.as_bytes().to_vec();
                    let parsed = simd_json::to_borrowed_value(&mut payload_bytes_vec)
                        .with_context(|| payload_json.clone())?;
                    let root_obj = as_object!(parsed, "root");
                    let keys: HashSet<&str> = root_obj.keys().map(|s| s.as_ref()).collect();
                    match row.get::<_, String>("payload_type")?.as_str() {
                        "REACTION" => {
                            ensure!(keys == HashSet::from(["photo_id", "photo_url", "photo_width", "photo_height",
                                                            "photo_expiration_timestamp", "emoji_reaction", "message"]),
                                    "Unexpected payload format for reaction to photo: {}", payload_json);
                            let message = get_field_str!(root_obj, "message", "message");
                            let emoji = get_field_str!(root_obj, "emoji_reaction", "emoji_reaction");
                            (vec![RichText::make_plain(format!("{message}: {emoji}"))], None)
                        }
                        "AUDIO" => {
                            ensure!(keys == HashSet::from(["id", "waveform", "url", "duration", "expiration_timestamp"]),
                                    "Unexpected payload format for audio message: {}", payload_json);
                            let duration_ms = get_field!(root_obj, "duration", "duration")?;
                            let duration_sec_option = Some(duration_ms.try_as_i32()? / 1000);
                            (vec![], Some(content::SealedValueOptional::VoiceMsg(ContentVoiceMsg {
                                path_option: None,
                                mime_type: "".to_string(),
                                duration_sec_option,
                            })))
                        }
                        "TEXT" => {
                            ensure!(keys == HashSet::from(["text", "type", "substitute_id"]),
                                    "Unexpected payload format: {}", payload_json);
                            match get_field_str!(root_obj, "type", "type") {
                                "TEXT" => {
                                    let text = get_field_string!(root_obj, "text", "text");
                                    (vec![RichText::make_plain(text)], None)
                                }
                                "SMILE" => {
                                    // This is an auto-generated message, let's mark it as such
                                    let text = get_field_string!(root_obj, "text", "text");
                                    (vec![RichText::make_italic("(Auto-generated message)\n".to_owned()),
                                          RichText::make_plain(text)], None)
                                }
                                etc => bail!("Unexpected message type {etc}!")
                            }
                        }
                        etc => bail!("Unexpected payload type {etc}!")
                    }
                };
                let content_option = content_option.map(|c| Content { sealed_value_optional: Some(c) });

                messages.push(Message::new(
                    *NO_INTERNAL_ID,
                    Some(source_id),
                    timestamp,
                    from_id,
                    text,
                    message_regular! {
                        edit_timestamp_option: None,
                        is_deleted: false,
                        forward_from_name_option: None,
                        reply_to_message_id_option,
                        content_option,
                    },
                ));
            }
            messages.iter_mut().enumerate().for_each(|(i, m)| m.internal_id = i as i64);

            if !messages.is_empty() {
                cwms.push(ChatWithMessages {
                    chat: Chat {
                        ds_uuid: ds_uuid.clone(),
                        id: user.id,
                        name_option: user.first_name_option.clone(),
                        source_type: SourceType::BadooDb as i32,
                        tpe: ChatType::Personal as i32,
                        img_path_option: None,
                        member_ids: vec![*MYSELF_ID, user.id],
                        msg_count: messages.len() as i32,
                        main_chat_id: None,
                    },
                    messages,
                });
            }
        }

        Ok(cwms)
    }
}
