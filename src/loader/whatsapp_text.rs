use std::fs;
use chrono::{NaiveDateTime, TimeZone};

use lazy_static::lazy_static;
use regex::Regex;

use crate::*;
use crate::dao::in_memory_dao::InMemoryDao;
use crate::loader::DataLoader;
use crate::protobuf::history::*;

use super::*;

#[cfg(test)]
#[path = "whatsapp_text_tests.rs"]
mod tests;

const TIMESTAMP_REGEX_STR: &str = r"^(\d{1,2}/\d{1,2}/\d{1,4}, \d{2}:\d{2})";

lazy_static! {
    static ref FILENAME_REGEX: Regex = Regex::new(r"^WhatsApp Chat with ([^.]+)\.txt$").unwrap();
    static ref TIMESTAMP_REGEX: Regex = Regex::new(TIMESTAMP_REGEX_STR).unwrap();
    static ref MESSAGE_PREFIX_REGEX: Regex = Regex::new(&format!("{}{}", TIMESTAMP_REGEX_STR, " - ([^:]+): (.+)$")).unwrap();
    static ref ATTACHED_FILE_REGEX: Regex = Regex::new(r"^(([^-]+)-[^ ]+) \(file attached\)$").unwrap();
}

pub struct WhatsAppTextDataLoader;

impl DataLoader for WhatsAppTextDataLoader {
    fn name(&self) -> &'static str { "WhatsApp (text)" }

    fn src_type(&self) -> &'static str { "whatsapp-text" }

    fn looks_about_right_inner(&self, path: &Path) -> EmptyRes {
        let filename = path_file_name(path)?;
        if !FILENAME_REGEX.is_match(filename) {
            bail!("File {} is not named as expected", filename);
        }
        if !TIMESTAMP_REGEX.is_match(first_line(&path)?.as_str()) {
            bail!("File {} does not start with a timestamp as expected", path_to_str(&path)?);
        }
        Ok(())
    }

    fn load_inner(&self, path: &Path, ds: Dataset, _myself_chooser: &dyn MyselfChooser) -> Result<Box<InMemoryDao>> {
        parse_whatsapp_text_file(path, ds)
    }
}

fn parse_whatsapp_text_file(path: &Path, ds: Dataset) -> Result<Box<InMemoryDao>> {
    let ds_uuid = ds.uuid.as_ref().unwrap();

    let file_content = fs::read_to_string(&path)?;
    let (myself, other) = parse_users(ds_uuid, path_file_name(path)?, &file_content)?;

    let messages = parse_messages(&file_content, &myself, &other)?;

    let cwms = vec![ChatWithMessages {
        chat: Some(Chat {
            ds_uuid: Some(ds_uuid.clone()),
            id: other.id, // Using user ID as a chat ID
            name_option: Some(other.pretty_name()),
            tpe: ChatType::Personal as i32,
            img_path_option: None,
            member_ids: vec![myself.id, other.id],
            msg_count: messages.len() as i32,
        }),
        messages: messages,
    }];

    let parent_name = path_file_name(path.parent().unwrap())?;
    Ok(Box::new(InMemoryDao::new(
        format!("WhatsApp ({})", parent_name),
        ds,
        path.parent().unwrap().to_path_buf(),
        myself.clone(),
        vec![myself, other],
        cwms,
    )))
}

fn parse_users(ds_uuid: &PbUuid, filename: &str, content: &str) -> Result<(User, User)> {
    let other_name = FILENAME_REGEX.captures(filename).unwrap().get(1).unwrap().as_str();

    let mut user_names = content.lines()
        .filter_map(|line| MESSAGE_PREFIX_REGEX.captures(line).map(|capt| capt.get(2).unwrap().as_str()))
        .unique()
        .collect_vec();

    if user_names.len() != 2 {
        bail!("Expected just two users, found {:?}", user_names);
    }

    let self_name = if let Some(other_id) = user_names.iter().position(|name| *name == other_name) {
        user_names.remove(other_id);
        user_names[0]
    } else {
        bail!("Expected other user to be named '{}', but users were {:?}", other_name, user_names)
    };

    // Self ID is set to minimum valid one.
    Ok((User {
        ds_uuid: Some(ds_uuid.clone()),
        id: UserId::INVALID.0 + 1,
        first_name_option: Some(self_name.to_owned()),
        last_name_option: None,
        username_option: None,
        phone_number_option: None,
    }, User {
        ds_uuid: Some(ds_uuid.clone()),
        id: hash_to_id(other_name),
        first_name_option: if other_name.starts_with("+") { None } else { Some(other_name.to_owned()) },
        last_name_option: None,
        username_option: None,
        phone_number_option: if other_name.starts_with("+") { Some(other_name.to_owned()) } else { None },
    }))
}

fn parse_messages(content: &String, myself: &User, other: &User) -> Result<Vec<Message>> {
    const NOTICE_LINE: &str = "Messages and calls are end-to-end encrypted.";
    const TIMER_LINE: &str = "updated the message timer. New messages will disappear from this chat";

    let mut result = vec![];

    let mut user_id: Option<UserId> = None;
    let mut timestamp: Timestamp = Timestamp::MIN;
    let mut lines: Vec<&str> = Vec::with_capacity(10);
    let mut last_internal_id = NO_INTERNAL_ID;

    let mut iter = content.lines().peekable();
    while let Some(line) = iter.next() {
        if line.contains(NOTICE_LINE) || line.contains(TIMER_LINE) {
            continue;
        }
        match MESSAGE_PREFIX_REGEX.captures(line) {
            Some(capture) => {
                // First message line
                let timestamp2: Timestamp = parse_datetime(capture.get(1).unwrap().as_str())?;
                if *timestamp2 > *timestamp {
                    timestamp = timestamp2;
                } else {
                    // Multiple messages have the same timestamp - treat them as 1 second apart
                    timestamp = Timestamp(*timestamp + 1);
                }

                let username_str = capture.get(2).unwrap().as_str();
                user_id = Some(if username_str == &myself.pretty_name() {
                    myself.id()
                } else {
                    other.id()
                });

                lines.push(capture.get(3).unwrap().as_str());
            }
            None => {
                // Not the first message line, just text
                lines.push(line);
            }
        }
        match iter.peek() {
            Some(l) if !MESSAGE_PREFIX_REGEX.is_match(l) => {
                // Multiline message continues, NOOP
            }
            _ => {
                // Time to process collected info
                let timestamp = if timestamp != Timestamp::MIN { *timestamp } else { bail!("Message timestamp unknown!") };
                let from_id = *user_id.ok_or_else(|| anyhow!("Message author unknown!"))?;

                last_internal_id = MessageInternalId(*last_internal_id + 1);

                let (text, content_option) = parse_message_text(&lines)?;
                result.push(Message::new(
                    *last_internal_id,
                    None /* source_id_option */,
                    timestamp,
                    from_id,
                    text,
                    message::Typed::Regular(MessageRegular {
                        edit_timestamp_option: None,
                        is_deleted: false,
                        forward_from_name_option: None,
                        reply_to_message_id_option: None,
                        content_option,
                    }),
                ));
                user_id = None;
                lines.clear();
            }
        }
    }

    Ok(result)
}

fn parse_message_text(lines: &[&str]) -> Result<(Vec<RichTextElement>, Option<Content>)> {
    use content::SealedValueOptional::*;

    let (lines, content) = if let Some(attachment_captures) = ATTACHED_FILE_REGEX.captures(lines[0]) {
        // First line describes attached file, determine the type
        let tpe = attachment_captures.get(2).unwrap().as_str();
        let filename = attachment_captures.get(1).unwrap().as_str();


        let content_value = match tpe {
            "IMG" => Photo(ContentPhoto {
                path_option: Some(filename.to_owned()),
                width: 0,
                height: 0,
                is_one_time: false,
            }),
            "STK" => Sticker(ContentSticker {
                path_option: Some(filename.to_owned()),
                width: 0,
                height: 0,
                thumbnail_path_option: None,
                emoji_option: None,
            }),
            "VID" => {
                require!(filename.ends_with(".mp4"), "Unexpected video file extension: {}", filename);
                Video(ContentVideo {
                    path_option: Some(filename.to_owned()),
                    title_option: None,
                    performer_option: None,
                    width: 0,
                    height: 0,
                    mime_type: "video/mp4".to_owned(),
                    duration_sec_option: None,
                    thumbnail_path_option: None,
                    is_one_time: false,
                })
            }
            "AUD" => {
                require!(filename.ends_with(".opus"), "Unexpected audio file extension: {}", filename);
                VoiceMsg(ContentVoiceMsg {
                    path_option: Some(filename.to_owned()),
                    mime_type: "audio/ogg".to_owned(),
                    duration_sec_option: None,
                })
            }
            _ => bail!("Unknown file type: {}", filename)
        };

        (&lines[1..], Some(Content { sealed_value_optional: Some(content_value) }))
    } else if lines[0] == "null" || lines[0] == "<Media omitted>" {
        // File wasn't present - e.g. one-time photo/video.
        // Since we don't know the type, represent it as a missing file.
        let content_value = File(ContentFile {
            path_option: None,
            file_name_option: None,
            mime_type_option: None,
            thumbnail_path_option: None,
        });
        (&lines[1..], Some(Content { sealed_value_optional: Some(content_value) }))
    } else {
        (lines, None)
    };

    let text = lines.iter().join("\n").trim().to_owned();
    let rtes = if text.is_empty() {
        vec![]
    } else {
        vec![RichText::make_plain(text)]
    };

    Ok((rtes, content))
}

/// Datetime formats used by WhatsApp:
/// ```text
/// 6/30/20, 16:14
/// 30/6/2020, 16:14
/// ```
fn parse_datetime(s: &str) -> Result<Timestamp> {
    // NaiveDateTime::parse_from_str is slow, but we don't usually have a lot of mesages in this format,
    // so we're fine with it.
    const DATE_TIME_SHORT_FMT: &str = "%m/%d/%y, %H:%M"; // 2-digit year
    const DATE_TIME_LONG_FMT: &str = "%d/%m/%Y, %H:%M";
    let parse = NaiveDateTime::parse_from_str;
    let naive_dt = parse(s, DATE_TIME_SHORT_FMT).or(parse(s, DATE_TIME_LONG_FMT))?;
    let local_dt = LOCAL_TZ.from_local_datetime(&naive_dt).unwrap();
    Ok(Timestamp(local_dt.timestamp()))
}
