use std::collections::HashMap;
use std::fs;

use rusqlite::Connection;

use crate::*;
use crate::loader::DataLoader;
use crate::protobuf::history::*;

use super::*;

#[cfg(test)]
#[path = "tinder_android_tests.rs"]
mod tests;

pub struct TinderAndroidDataLoader<H: HttpClient + 'static> {
    pub http_client: &'static H,
}

android_sqlite_loader!(TinderAndroidDataLoader<H: HttpClient>, TinderDb, "Tinder", "tinder-3.db");

const MEDIA_DIR: &str = "Media";
const MEDIA_DOWNLOADED_SUBDIR: &str = "_downloaded";
const RELATIVE_MEDIA_DIR: &str = concatcp!(MEDIA_DIR, "/", MEDIA_DOWNLOADED_SUBDIR);

/// Using a first legal ID (i.e. "1") for myself
const MYSELF_ID: UserId = UserId(UserId::INVALID.0 + 1);

/// Technically, self does have a proper key, but knowing it doesn't help us.
const MYSELF_KEY: &str = "myself";

type UserKey = String;
type Users = HashMap<UserKey, User>;

impl<H: HttpClient + 'static> TinderAndroidDataLoader<H> {
    fn tweak_conn(&self, _path: &Path, _conn: &Connection) -> EmptyRes { Ok(()) }

    fn normalize_users(&self, users: Users, _cwms: &[ChatWithMessages]) -> Result<Vec<User>> {
        let mut users = users.into_values().collect_vec();
        // Set myself to be a first member.
        users.sort_by_key(|u| if u.id == *MYSELF_ID { *UserId::MIN } else { u.id });
        Ok(users)
    }

    fn parse_users(&self, conn: &Connection, ds_uuid: &PbUuid) -> Result<Users> {
        let mut users: Users = Default::default();

        users.insert(MYSELF_KEY.to_owned(), User {
            ds_uuid: Some(ds_uuid.clone()),
            id: *MYSELF_ID,
            first_name_option: None, // No way to know your own name, sadly
            last_name_option: None,
            username_option: None,
            phone_number_option: None,
        });

        let mut stmt = conn.prepare(r"SELECT * FROM match_person")?;
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let key = row.get::<_, String>("id")?;
            let id = UserId(hash_to_id(&key));

            let name_option = row.get::<_, Option<String>>("name")?;

            users.insert(key, User {
                ds_uuid: Some(ds_uuid.clone()),
                id: *id,
                first_name_option: name_option,
                last_name_option: None,
                username_option: None,
                phone_number_option: None,
            });
        }

        Ok(users)
    }

    fn parse_chats(&self, conn: &Connection, ds_uuid: &PbUuid, users: &Users, path: &Path) -> Result<Vec<ChatWithMessages>> {
        let mut cwms = vec![];

        let downloaded_media_path = path.join(RELATIVE_MEDIA_DIR);
        fs::create_dir_all(&downloaded_media_path)?;

        let mut stmt = conn.prepare(r"
            SELECT *
            FROM message
            WHERE match_id LIKE '%' || ? || '%'
            ORDER BY sent_date ASC
        ")?;

        for (key, user) in users {
            if key == MYSELF_KEY { continue; }

            let mut rows = stmt.query([key])?;

            let mut messages = vec![];
            while let Some(row) = rows.next()? {
                // Source ID is way too large to fit into i64, so we use hash instead.
                let source_id = row.get::<_, String>("id")?;
                let source_id = hash_to_id(&source_id);

                let timestamp = row.get::<_, i64>("sent_date")? / 1000;

                let from_id = if &row.get::<_, String>("from_id")? == key { user.id } else { *MYSELF_ID };

                let text = row.get::<_, String>("text")?;
                let (text, content_option) = if text.starts_with("https://media.tenor.com/") {
                    // This is a GIF, let's download it and include it as a sticker.
                    // Example: https://media.tenor.com/mYFQztB4EHoAAAAM/house-hugh-laurie.gif?width=220&height=226
                    let hash = hash_to_id(&text);
                    let filename = format!("{}.gif", hash);
                    let gif_path = downloaded_media_path.join(&filename);
                    if !gif_path.exists() {
                        log::info!("Downloading {}", text);
                        let bytes = self.http_client.get_bytes(&text)?;
                        fs::write(&gif_path, bytes)?;
                    }
                    let (width, height) = {
                        let split = text.split(['?', '&']).skip(1).collect_vec();
                        (split.iter().find(|s| s.starts_with("width=")).map(|s| s[6..].parse()).unwrap_or(Ok(0))?,
                         split.iter().find(|s| s.starts_with("height=")).map(|s| s[7..].parse()).unwrap_or(Ok(0))?)
                    };
                    (vec![], Some(Content {
                        sealed_value_optional: Some(content::SealedValueOptional::Sticker(ContentSticker {
                            path_option: Some(format!("{RELATIVE_MEDIA_DIR}/{filename}")),
                            width: width * 2,
                            height: height * 2,
                            thumbnail_path_option: None,
                            emoji_option: None,
                        }))
                    }))
                } else {
                    (vec![RichText::make_plain(text)], None)
                };

                messages.push(Message::new(
                    *NO_INTERNAL_ID,
                    Some(source_id),
                    timestamp,
                    from_id,
                    text,
                    message::Typed::Regular(MessageRegular {
                        edit_timestamp_option: None,
                        is_deleted: false,
                        forward_from_name_option: None,
                        reply_to_message_id_option: None,
                        content_option,
                    }), ));
            }
            messages.iter_mut().enumerate().for_each(|(i, m)| m.internal_id = i as i64);

            cwms.push(ChatWithMessages {
                chat: Some(Chat {
                    ds_uuid: Some(ds_uuid.clone()),
                    id: user.id,
                    name_option: user.first_name_option.clone(),
                    source_type: SourceType::TinderDb as i32,
                    tpe: ChatType::Personal as i32,
                    img_path_option: None,
                    member_ids: vec![*MYSELF_ID, user.id],
                    msg_count: messages.len() as i32,
                    main_chat_id: None,
                }),
                messages,
            });
        }

        Ok(cwms)
    }
}
