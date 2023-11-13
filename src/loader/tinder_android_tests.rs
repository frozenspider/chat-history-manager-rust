#![allow(unused_imports)]

use std::fs;
use std::path::PathBuf;
use chrono::prelude::*;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::info;
use pretty_assertions::{assert_eq, assert_ne};

use crate::{NoChooser, User};
use crate::dao::ChatHistoryDao;
use crate::entity_utils::*;
use crate::protobuf::history::*;
use crate::protobuf::history::content::SealedValueOptional::*;
use crate::protobuf::history::message::*;
use crate::protobuf::history::message_service::SealedValueOptional::*;

use super::*;

const RESOURCE_DIR: &str = "tinder-android";
const LOADER: TinderAndroidDataLoader = TinderAndroidDataLoader;

//
// Tests
//

#[test]
fn loading_2023_11() -> EmptyRes {
    let (res, _db_dir) = test_android::create_databases(RESOURCE_DIR, "2023-11", DB_FILENAME)?;
    LOADER.looks_about_right(&res)?;

    let dao = LOADER.load(&res, &NoChooser)?;

    let ds_uuid = dao.dataset.uuid.unwrap_ref();
    let myself = &dao.myself;
    assert_eq!(myself, &expected_myself(ds_uuid));

    let member = User {
        ds_uuid: Some(ds_uuid.clone()),
        id: 780327027359649707_i64,
        first_name_option: Some("Abcde".to_owned()),
        last_name_option: None,
        username_option: None,
        phone_number_option: None,
    };

    assert_eq!(dao.users, vec![myself.clone(), member.clone()]);

    assert_eq!(dao.cwms.len(), 1);

    {
        let cwm = &dao.cwms[0];
        let chat = cwm.chat.unwrap_ref();

        assert_eq!(chat.member_ids.len(), 2);
        assert!(chat.member_ids.contains(&myself.id));
        assert!(chat.member_ids.contains(&member.id));

        let msgs = dao.first_messages(&chat, 99999)?;
        assert_eq!(msgs.len(), 2);
        assert_eq!(chat.msg_count, 2);

        assert_eq!(msgs[0], Message {
            internal_id: 0,
            source_id_option: Some(869569426176655274),
            timestamp: 1699812983,
            from_id: myself.id,
            text: vec![RichText::make_plain("Sending you a text!".to_owned())],
            searchable_string: "Sending you a text!".to_owned(),
            typed: Some(MESSAGE_REGULAR_NO_CONTENT.clone()),
        });
        assert_eq!(msgs[1], Message { // TODO: Tenor links should be parsed as GIFs
            internal_id: 1,
            source_id_option: Some(5405907581016140653),
            timestamp: 1699813000,
            from_id: member.id,
            text: vec![RichText::make_plain("https://media.tenor.com/mYFQztB4EHoAAAAC/house-hugh-laurie.gif?width=271&height=279".to_owned())],
            searchable_string: "https://media.tenor.com/mYFQztB4EHoAAAAC/house-hugh-laurie.gif?width=271&height=279".to_owned(),
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
        ds_uuid: Some(ds_uuid.clone()),
        id: 1_i64,
        first_name_option: None,
        last_name_option: None,
        username_option: None,
        phone_number_option: None,
    }
}
