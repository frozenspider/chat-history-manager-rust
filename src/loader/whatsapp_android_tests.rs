#![allow(unused_imports)]

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

const RESOURCE_DIR: &str = "whatsapp-android";
const LOADER: WhatsAppAndroidDataLoader = WhatsAppAndroidDataLoader;

//
// Tests
//

#[test]
fn vcards() -> EmptyRes {
    fn parse(vcard_string: &str) -> Result<ContentSharedContact> {
        parse_vcard(&trim_vcard_string(vcard_string))
    }
    fn vc(first_name: &str, phone: &str) -> ContentSharedContact {
        ContentSharedContact {
            first_name_option: Some(first_name.to_owned()),
            last_name_option: None,
            phone_number_option: Some(phone.to_owned()),
            vcard_path_option: None,
        }
    }

    assert_eq!(parse(r"
        BEGIN:VCARD
        VERSION:3.0
        N:;Name (comment);;;
        FN:Name (comment)
        TEL;type=Mobile;waid=112223456543:+11 222-3456-543
        END:VCARD
    ")?, vc("Name (comment)", "+11 222-3456-543"));

    assert_eq!(parse(r"
        BEGIN:VCARD
        VERSION:3.0
        N:Name3;Name1;Name2;;
        FN:Name1 Name2 Name3
        TEL;type=Home:+12 345-6789-8765
        TEL;type=Mobile;waid=9876543212345:+98 765-4321-2345
        END:VCARD
    ")?, vc("Name1 Name2 Name3", "+98 765-4321-2345"));

    assert_eq!(parse(r"
        BEGIN:VCARD
        VERSION:3.0
        N:;+11 222-3333-4444;;;
        FN:+11 222-3333-4444
        TEL;type=CELL;waid=1122233334444:+11 222-3333-4444
        X-WA-BIZ-NAME:+11 222-3333-4444
        X-WA-BIZ-DESCRIPTION:My Fancy Description!
        END:VCARD
    ")?, vc("+11 222-3333-4444", "+11 222-3333-4444"));

    assert_eq!(parse(r"
        BEGIN:VCARD
        VERSION:3.0
        N:Name;Full;;;
        FN:Full Name
        item1.TEL;waid=1122233334444:+11 222-3333-4444
        item1.X-ABLabel:Ponsel
        X-WA-BIZ-DESCRIPTION:My Fancy Description!
        X-WA-BIZ-NAME:Full Name
        END:VCARD
    ")?, vc("Full Name", "+11 222-3333-4444"));

    Ok(())
}

#[test]
fn loading_2023_10() -> EmptyRes {
    let (res, _db_dir) = test_android::create_databases(RESOURCE_DIR, "2023-10", ".db", DB_FILENAME);
    LOADER.looks_about_right(&res)?;

    let dao = LOADER.load(&res, &NoChooser)?;

    let ds_uuid = &dao.ds_uuid();
    let myself = dao.myself_single_ds();
    assert_eq!(myself, expected_myself(ds_uuid));

    let member = User {
        ds_uuid: Some(ds_uuid.clone()),
        id: 9017079856339592512_i64,
        first_name_option: None,
        last_name_option: None,
        username_option: None,
        phone_number_option: Some("+11111".to_owned()),
    };

    assert_eq!(dao.users_single_ds(), vec![myself.clone(), member.clone()]);

    assert_eq!(dao.cwms_single_ds().len(), 2);

    {
        let cwm = dao.cwms_single_ds().into_iter().find(|cwm| cwm.chat.unwrap_ref().tpe == ChatType::PrivateGroup as i32).unwrap();
        let chat = cwm.chat.unwrap_ref();
        assert_eq!(*chat, Chat {
            ds_uuid: Some(ds_uuid.clone()),
            id: 15668065017168951_i64,
            name_option: Some("My Group".to_owned()),
            source_type: SourceType::WhatsappDb as i32,
            tpe: ChatType::PrivateGroup as i32,
            img_path_option: Some("files/Avatars/100000000000000001@g.us.j".to_owned()),
            member_ids: vec![myself.id, member.id],
            msg_count: 2,
            main_chat_id: None,
        });

        let msgs = dao.first_messages(&chat, 99999)?;
        assert_eq!(msgs.len() as i32, chat.msg_count);

        assert_eq!(msgs[0], Message {
            internal_id: 0,
            source_id_option: Some(8082739393298423973),
            timestamp: 1643607839,
            from_id: member.id,
            text: vec![],
            searchable_string: myself.pretty_name(),
            typed: Some(message_service!(GroupInviteMembers(MessageServiceGroupInviteMembers {
                members: vec![myself.pretty_name()],
            }))),
        });
        assert_eq!(msgs[1], Message {
            internal_id: 1,
            source_id_option: Some(4824408779253713719),
            timestamp: 1661417508,
            from_id: myself.id,
            text: vec![
                RichTextElement {
                    searchable_string: "Last group message".to_owned(),
                    val: Some(rich_text_element::Val::Plain(RtePlain {
                        text: "Last group message".to_owned()
                    })),
                },
            ],
            searchable_string: "Last group message".to_owned(),
            typed: Some(message_regular! {
                edit_timestamp_option: Some(1661417955),
                is_deleted: false,
                forward_from_name_option: Some(SOMEONE.to_owned()),
                reply_to_message_id_option: msgs[0].source_id_option,
                content_option: None,
            }),
        });
    }

    {
        let cwm = dao.cwms_single_ds().into_iter().find(|cwm| cwm.chat.unwrap_ref().tpe == ChatType::Personal as i32).unwrap();
        let chat = cwm.chat.unwrap_ref();
        assert_eq!(*chat, Chat {
            ds_uuid: Some(ds_uuid.clone()),
            id: member.id,
            name_option: Some("+11111".to_owned()),
            source_type: SourceType::WhatsappDb as i32,
            tpe: ChatType::Personal as i32,
            img_path_option: Some("files/Avatars/11111@s.whatsapp.net.j".to_owned()),
            member_ids: vec![myself.id, member.id],
            msg_count: 2,
            main_chat_id: None,
        });

        let msgs = dao.first_messages(&chat, 99999)?;
        assert_eq!(msgs.len() as i32, chat.msg_count);

        assert_eq!(msgs[0], Message {
            internal_id: 0,
            source_id_option: Some(3891646720130869054),
            timestamp: 1687757170,
            from_id: member.id,
            text: vec![],
            searchable_string: "Jl. Gurita No.21x, Denpasar, Bali New Bahari -8.70385650 115.21673666".to_owned(),
            typed: Some(message_regular! {
                edit_timestamp_option: None,
                is_deleted: false,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: Some(Content {
                    sealed_value_optional: Some(Location(ContentLocation {
                        title_option: Some("New Bahari".to_owned()),
                        address_option: Some("Jl. Gurita No.21x, Denpasar, Bali".to_owned()),
                        lat_str: "-8.70385650".to_string(),
                        lon_str: "115.21673666".to_string(),
                        duration_sec_option: Some(123),
                    }))
                }),
            }),
        });

        assert_eq!(msgs[1], Message {
            internal_id: 1,
            source_id_option: Some(8221205389172673925),
            timestamp: 1693993938,
            from_id: myself.id,
            text: vec![],
            searchable_string: "".to_owned(),
            typed: Some(message_regular! {
                edit_timestamp_option: Some(1693993963),
                is_deleted: true,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: None,
            }),
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
        first_name_option: Some("Aaaaa Aaaaaaaaaaa".to_owned()),
        last_name_option: None,
        username_option: None,
        phone_number_option: Some("+00000".to_owned()),
    }
}

fn trim_vcard_string(s: &str) -> String {
    s.trim().lines().map(|s| s.trim()).join("\n")
}
