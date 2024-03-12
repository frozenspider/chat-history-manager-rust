#![allow(unused_imports)]

use std::fmt::format;
use std::fs;
use std::path::PathBuf;
use chrono::prelude::*;
use lazy_static::lazy_static;
use pretty_assertions::{assert_eq, assert_ne};

use crate::{NoChooser, User};
use crate::dao::ChatHistoryDao;
use crate::entity_utils::*;
use crate::protobuf::history::content::SealedValueOptional::*;
use crate::protobuf::history::message::*;
use crate::protobuf::history::message_service::SealedValueOptional::*;

use super::*;

const RESOURCE_DIR: &str = "badoo-android";
const LOADER: BadooAndroidDataLoader = BadooAndroidDataLoader;

//
// Tests
//

#[test]
fn loading_2023_12() -> EmptyRes {
    let (res, _db_dir) = test_android::create_databases(RESOURCE_DIR, "2023-12", "", DB_FILENAME);

    LOADER.looks_about_right(&res)?;
    let dao = LOADER.load(&res, &NoChooser)?;

    let ds_uuid = &dao.ds_uuid();
    let myself = dao.myself_single_ds();
    assert_eq!(myself, expected_myself(ds_uuid));

    let member = User {
        ds_uuid: ds_uuid.clone(),
        id: 1234567890_i64,
        first_name_option: Some("Abcde".to_owned()),
        last_name_option: None,
        username_option: None,
        phone_number_option: None,
    };

    assert_eq!(dao.users_single_ds(), vec![myself.clone(), member.clone()]);

    assert_eq!(dao.cwms_single_ds().len(), 1);

    {
        let cwm = dao.cwms_single_ds().remove(0);
        let chat = cwm.chat;
        assert_eq!(chat, Chat {
            ds_uuid: ds_uuid.clone(),
            id: member.id,
            name_option: Some("Abcde".to_owned()),
            source_type: SourceType::BadooDb as i32,
            tpe: ChatType::Personal as i32,
            img_path_option: None,
            member_ids: vec![myself.id, member.id],
            msg_count: 4,
            main_chat_id: None,
        });

        let msgs = dao.first_messages(&chat, 99999)?;
        assert_eq!(msgs.len() as i32, chat.msg_count);

        assert_eq!(msgs[0], Message {
            internal_id: 0,
            source_id_option: Some(4313483375),
            timestamp: 1687425601,
            from_id: member.id,
            text: vec![RichText::make_plain("Hello there!".to_owned())],
            searchable_string: "Hello there!".to_owned(),
            typed: Some(MESSAGE_REGULAR_NO_CONTENT.clone()),
        });
        assert_eq!(msgs[1], Message {
            internal_id: 1,
            source_id_option: Some(4313483378),
            timestamp: 1687425658,
            from_id: myself.id,
            text: vec![RichText::make_plain("Reply there!".to_owned())],
            searchable_string: "Reply there!".to_owned(),
            typed: Some(message_regular! {
                edit_timestamp_option: None,
                is_deleted: false,
                forward_from_name_option: None,
                reply_to_message_id_option: Some(4313483375),
                content_option: None,
            }),
        });
        assert_eq!(msgs[2], Message {
            internal_id: 2,
            source_id_option: Some(4313658961),
            timestamp: 1690856116,
            from_id: member.id,
            text: vec![],
            searchable_string: "".to_owned(),
            typed: Some(message_regular! {
                edit_timestamp_option: None,
                is_deleted: false,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: Some(Content {
                    sealed_value_optional: Some(VoiceMsg(ContentVoiceMsg {
                        path_option: None,
                        mime_type: "".to_owned(),
                        duration_sec_option: Some(23),
                    }))
                }),
            }),
        });
        assert_eq!(msgs[3], Message {
            internal_id: 3,
            source_id_option: Some(4313616080),
            timestamp: 1692781351,
            from_id: member.id,
            text: vec![RichText::make_plain("Abcde reacted to your profile: 🤔".to_owned())],
            searchable_string: "Abcde reacted to your profile: 🤔".to_owned(),
            typed: Some(MESSAGE_REGULAR_NO_CONTENT.clone()),
        });
    }

    Ok(())
}

//
// Helpers
//

fn expected_myself(ds_uuid: &PbUuid) -> User {
    User {
        ds_uuid: ds_uuid.clone(),
        id: 1_i64,
        first_name_option: Some("Me".to_owned()),
        last_name_option: None,
        username_option: None,
        phone_number_option: None,
    }
}
