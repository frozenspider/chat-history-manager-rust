#![allow(unused_imports)]

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

static LOADER: TelegramDataLoader = TelegramDataLoader;

//
// Tests
//

#[test]
fn loading_2020_01() -> EmptyRes {
    let res = resource("telegram_2020-01");
    LOADER.looks_about_right(&res)?;

    let dao =
        LOADER.load(&res, &NoChooser)?;

    let ds_uuid = &dao.ds_uuid();
    let myself = dao.myself_single_ds();
    assert_eq!(myself, expected_myself(ds_uuid));

    let member = ShortUser::new(UserId(32507588), None);
    let expected_users = vec![
        myself.clone(),
        User {
            ds_uuid: Some(ds_uuid.clone()),
            id: 22222222,
            first_name_option: Some("Wwwwww".to_owned()),
            last_name_option: Some("Www".to_owned()),
            username_option: None,
            phone_number_option: Some("+998 90 9998877".to_owned()),
        },
        member.to_user(ds_uuid),
        User {
            ds_uuid: Some(ds_uuid.clone()),
            id: 44444444,
            first_name_option: Some("Eeeee".to_owned()),
            last_name_option: Some("Eeeeeeeeee".to_owned()),
            username_option: None,
            phone_number_option: Some("+7 999 333 44 55".to_owned()),
        },
        ShortUser::new_name_str(UserId(310242343), "Vlllllll").to_user(ds_uuid),
        ShortUser::new_name_str(UserId(333333333), "Ddddddd Uuuuuuuu").to_user(ds_uuid),
        User {
            ds_uuid: Some(ds_uuid.clone()),
            id: 555555555,
            first_name_option: Some("Nnnnnnn".to_owned()),
            last_name_option: None,
            username_option: None,
            phone_number_option: Some("+998 90 1112233".to_owned()),
        },
        ShortUser::new_name_str(UserId(666666666), "Iiiii Kkkkkkkkkk").to_user(ds_uuid),
        User {
            ds_uuid: Some(ds_uuid.clone()),
            id: 777777777,
            first_name_option: Some("Vvvvv".to_owned()),
            last_name_option: Some("Vvvvvvvvv".to_owned()),
            username_option: None,
            phone_number_option: Some("+7 951 123 45 67".to_owned()),
        },
    ];

    assert_eq!(dao.users_single_ds().len(), 9);
    assert_eq!(dao.users_single_ds(), expected_users);

    assert_eq!(dao.cwms_single_ds().len(), 4);

    // "Ordered" chat
    {
        let cwm = dao.cwms_single_ds().into_iter()
            .find(|c| c.chat.unwrap_ref().id == 4321012345)
            .unwrap();
        let chat = cwm.chat.unwrap_ref();
        assert_eq!(*chat, Chat {
            ds_uuid: Some(ds_uuid.clone()),
            id: 4321012345,
            name_option: None,
            source_type: SourceType::Telegram as i32,
            tpe: ChatType::Personal as i32,
            img_path_option: None,
            member_ids: vec![myself.id, *member.id],
            msg_count: 5,
            main_chat_id: None,
        });

        let msgs = dao.first_messages(&chat, 99999)?;
        assert_eq!(msgs.len() as i32, chat.msg_count);

        assert_eq!(chat.msg_count, 5);
        msgs.iter().for_each(|m| {
            assert_matches!(m.typed(), Typed::Regular(_));
            assert_eq!(m.from_id, *member.id);
        });
        assert_eq!(
            msgs.iter().map(|m| RichText::unwrap_copy(&m.text).clone()).collect_vec(),
            vec![
                "Message from null-names contact",
                "These messages...",
                "...have the same timestamp...",
                "...but different IDs...",
                "...and we expect order to be preserved.",
            ].into_iter()
                .map(|s|
                    vec![rich_text_element::Val::Plain(RtePlain { text: s.to_owned() })]
                )
                .collect_vec()
        );
        Ok(())
    }
}

#[test]
fn loading_2021_05() -> EmptyRes {
    let res = resource("telegram_2021-05");
    LOADER.looks_about_right(&res)?;

    let dao =
        LOADER.load(&res, &NoChooser)?;

    let ds_uuid = &dao.ds_uuid();
    let myself = dao.myself_single_ds();
    assert_eq!(myself, expected_myself(ds_uuid));

    // We only know of myself + two users (other's IDs aren't known), as well as service "member".
    let service_member =
        ShortUser::new_name_str(UserId(8112233), "My Old Group").to_user(ds_uuid);
    let member1 = User {
        ds_uuid: Some(ds_uuid.clone()),
        id: 22222222,
        first_name_option: Some("Wwwwww".to_owned()),
        last_name_option: Some("Www".to_owned()),
        username_option: None,
        phone_number_option: Some("+998 90 9998877".to_owned()), // Taken from contacts list
    };
    let member2 = User {
        ds_uuid: Some(ds_uuid.clone()),
        id: 44444444,
        first_name_option: Some("Eeeee".to_owned()),
        last_name_option: Some("Eeeeeeeeee".to_owned()),
        username_option: None,
        phone_number_option: Some("+7 999 333 44 55".to_owned()), // Taken from contacts list
    };
    assert_eq!(dao.users_single_ds().len(), 4);
    assert_eq!(dao.users_single_ds().iter().collect_vec(), vec![&myself, &service_member, &member1, &member2]);

    assert_eq!(dao.cwms_single_ds().len(), 1);

    // Group chat
    {
        // Chat ID is shifted by 2^33
        let cwm = dao.cwms_single_ds().into_iter()
            .find(|c| c.chat.unwrap_ref().id == 123123123 + GROUP_CHAT_ID_SHIFT)
            .unwrap();
        let chat = cwm.chat.unwrap_ref();
        assert_eq!(*chat, Chat {
            ds_uuid: Some(ds_uuid.clone()),
            id: 123123123 + GROUP_CHAT_ID_SHIFT,
            name_option: Some("My Group".to_owned()),
            source_type: SourceType::Telegram as i32,
            tpe: ChatType::PrivateGroup as i32,
            img_path_option: None,
            member_ids: vec![myself.id, service_member.id, member1.id, member2.id],
            msg_count: 3,
            main_chat_id: None,
        });

        let msgs = dao.first_messages(&chat, 99999)?;
        assert_eq!(msgs.len() as i32, chat.msg_count);

        let typed = msgs.iter().map(|m| m.typed()).collect_vec();

        // I wish we could use assert_matches!() already...
        assert_matches!(typed[0], message_service_pat!(GroupCreate(_)));
        assert_matches!(typed[1], message_service_pat!(GroupMigrateFrom(_)));
        assert_matches!(typed[2], Typed::Regular(_));
    };
    Ok(())
}


#[test]
fn loading_2021_06_supergroup() -> EmptyRes {
    let res = resource("telegram_2021-06_supergroup");
    LOADER.looks_about_right(&res)?;

    let dao =
        LOADER.load(&res, &NoChooser)?;

    let ds_uuid = &dao.ds_uuid();
    let myself = dao.myself_single_ds();
    assert_eq!(myself, expected_myself(ds_uuid));

    // We only know of myself + two users (other's IDs aren't known), as well as service "member".
    let u222222222 =
        ShortUser::new_name_str(UserId(222222222), "Sssss Pppppp").to_user(ds_uuid);
    let u333333333 =
        ShortUser::new_name_str(UserId(333333333), "Tttttt Yyyyyyy").to_user(ds_uuid);
    let u444444444 =
        ShortUser::new_name_str(UserId(444444444), "Vvvvvvvv Bbbbbbb").to_user(ds_uuid);

    {
        let sorted_users = dao.users_single_ds().into_iter().sorted_by_key(|u| u.id).collect_vec();
        assert_eq!(sorted_users.len(), 4);
        assert_eq!(sorted_users.iter().collect_vec(), vec![&myself, &u222222222, &u333333333, &u444444444]);
    }

    assert_eq!(dao.cwms_single_ds().len(), 1);

    // Group chat
    {
        // Chat ID is shifted by 2^33
        let cwm = dao.cwms_single_ds().into_iter()
            .find(|c| c.chat.unwrap_ref().id == 1234567890 + GROUP_CHAT_ID_SHIFT)
            .unwrap();
        let chat = cwm.chat.unwrap_ref();
        // All users are taken from chat itself
        assert_eq!(*chat, Chat {
            ds_uuid: Some(ds_uuid.clone()),
            id: 1234567890 + GROUP_CHAT_ID_SHIFT,
            name_option: Some("My Supergroup".to_owned()),
            source_type: SourceType::Telegram as i32,
            tpe: ChatType::PrivateGroup as i32,
            img_path_option: None,
            member_ids: vec![myself.id, u222222222.id, u333333333.id, u444444444.id],
            msg_count: 4,
            main_chat_id: None,
        });

        let msgs = dao.first_messages(&chat, 99999)?;
        assert_eq!(msgs.len() as i32, chat.msg_count);

        assert_eq!(msgs[0], Message {
            internal_id: 0,
            source_id_option: Some(-999681092),
            timestamp: dt("2020-12-22 23:11:21", None).timestamp(),
            from_id: u222222222.id,
            text: vec![],
            searchable_string: "Vvvvvvvv Bbbbbbb".to_owned(),
            typed: Some(message_service!(GroupInviteMembers(MessageServiceGroupInviteMembers {
                members: vec![u444444444.first_name_option.unwrap()]
            }))),
        });

        assert_eq!(msgs[1], Message {
            internal_id: 1,
            source_id_option: Some(-999681090),
            timestamp: dt("2020-12-22 23:12:09", None).timestamp(),
            from_id: u333333333.id,
            text: vec![RichTextElement {
                searchable_string: "Message text with emoji 🙂".to_owned(),
                val: Some(rich_text_element::Val::Plain(RtePlain {
                    text: "Message text with emoji 🙂".to_owned(),
                })),
            }],
            searchable_string: "Message text with emoji 🙂".to_owned(),
            typed: Some(message_regular! {
                edit_timestamp_option: None,
                is_deleted: false,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: None,
            }),
        });

        assert_eq!(msgs[2], Message {
            internal_id: 2,
            source_id_option: Some(-999681087),
            timestamp: dt("2020-12-22 23:12:51", None).timestamp(),
            from_id: u444444444.id,
            text: vec![RichTextElement {
                searchable_string: "Message from an added user".to_owned(),
                val: Some(rich_text_element::Val::Plain(RtePlain {
                    text: "Message from an added user".to_owned(),
                })),
            }],
            searchable_string: "Message from an added user".to_owned(),
            typed: Some(message_regular! {
                edit_timestamp_option: None,
                is_deleted: false,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: None,
            }),
        });
        assert_eq!(msgs[3], Message {
            internal_id: 3,
            source_id_option: Some(358000),
            timestamp: dt("2021-03-18 17:50:23", None).timestamp(),
            from_id: myself.id,
            text: vec![],
            searchable_string: format!("{} {}", myself.first_name_option.unwrap_ref(), &myself.phone_number_option.as_ref().unwrap()),
            typed: Some(message_regular! {
                edit_timestamp_option: None,
                is_deleted: false,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: Some(Content {
                    sealed_value_optional: Some(SharedContact(ContentSharedContact {
                        first_name_option: myself.first_name_option.to_owned(),
                        last_name_option: None,
                        phone_number_option: Some(myself.phone_number_option.to_owned().unwrap()),
                        vcard_path_option: None,
                    }))
                }),
            }),
        });
    };
    Ok(())
}

#[test]
fn loading_2021_07() -> EmptyRes {
    let res = resource("telegram_2021-07");
    LOADER.looks_about_right(&res)?;

    let dao =
        LOADER.load(&res, &NoChooser)?;

    let ds_uuid = &dao.ds_uuid();
    let myself = dao.myself_single_ds();
    assert_eq!(myself, expected_myself(ds_uuid));

    let member = User {
        ds_uuid: Some(ds_uuid.clone()),
        id: 44444444,
        first_name_option: Some("Eeeee".to_owned()),
        last_name_option: Some("Eeeeeeeeee".to_owned()),
        username_option: None,
        phone_number_option: Some("+7 999 333 44 55".to_owned()), // Taken from contacts list
    };
    assert_eq!(dao.users_single_ds().len(), 2);
    assert_eq!(dao.users_single_ds().iter().collect_vec(), vec![&myself, &member]);

    assert_eq!(dao.cwms_single_ds().len(), 1);

    // Group chat
    {
        // Chat ID is shifted by 2^33
        let cwm = dao.cwms_single_ds().into_iter()
            .find(|c| c.chat.unwrap_ref().id == 123123123 + GROUP_CHAT_ID_SHIFT)
            .unwrap();
        let chat = cwm.chat.unwrap_ref();
        assert_eq!(*chat, Chat {
            ds_uuid: Some(ds_uuid.clone()),
            id: 123123123 + GROUP_CHAT_ID_SHIFT,
            name_option: Some("My Group".to_owned()),
            source_type: SourceType::Telegram as i32,
            tpe: ChatType::PrivateGroup as i32,
            img_path_option: None,
            member_ids: vec![myself.id, member.id],
            msg_count: 2,
            main_chat_id: None,
        });

        let msgs = dao.first_messages(&chat, 99999)?;
        assert_eq!(msgs.len() as i32, chat.msg_count);

        assert_eq!(msgs[0], Message {
            internal_id: 0,
            source_id_option: Some(111111),
            timestamp: dt("2021-07-03 22:38:58", None).timestamp(),
            from_id: member.id,
            text: vec![],
            searchable_string: "Www Wwwwww".to_owned(),
            typed: Some(message_service!(GroupCall(MessageServiceGroupCall {
                members: vec!["Www Wwwwww".to_owned()]
            }))),
        });
        assert_eq!(msgs[1], Message {
            internal_id: 1,
            source_id_option: Some(111112),
            timestamp: dt("2021-07-03 22:39:01", None).timestamp(),
            from_id: member.id,
            text: vec![],
            searchable_string: "Myself".to_owned(),
            typed: Some(message_service!(GroupCall(MessageServiceGroupCall {
                members: vec!["Myself".to_owned()]
            }))),
        });
    };
    Ok(())
}

#[test]
fn loading_2023_01() -> EmptyRes {
    let res = resource("telegram_2023-01");
    LOADER.looks_about_right(&res)?;

    let dao =
        LOADER.load(&res, &NoChooser)?;

    // Parsing as UTC+5.
    let offset = FixedOffset::east_opt(5 * 3600).unwrap();

    let ds_uuid = &dao.ds_uuid();
    let myself = dao.myself_single_ds();
    assert_eq!(myself, expected_myself(ds_uuid));

    let member = User {
        ds_uuid: Some(ds_uuid.clone()),
        id: 44444444,
        first_name_option: Some("Eeeee".to_owned()),
        last_name_option: Some("Eeeeeeeeee".to_owned()),
        username_option: None,
        phone_number_option: Some("+7 999 333 44 55".to_owned()), // Taken from contacts list
    };
    let channel_user = User {
        ds_uuid: Some(ds_uuid.clone()),
        id: 123123123,
        first_name_option: Some("My Group".to_owned()),
        last_name_option: None,
        username_option: None,
        phone_number_option: None,
    };
    assert_eq!(dao.users_single_ds().len(), 3);
    assert_eq!(dao.users_single_ds().iter().collect_vec(), vec![&myself, &member, &channel_user]);

    assert_eq!(dao.cwms_single_ds().len(), 1);

    // Group chat
    {
        // Chat ID is shifted by 2^33
        let cwm = dao.cwms_single_ds().into_iter()
            .find(|c| c.chat.unwrap_ref().id == 123123123 + GROUP_CHAT_ID_SHIFT)
            .unwrap();
        let chat = cwm.chat.unwrap_ref();
        assert_eq!(*chat, Chat {
            ds_uuid: Some(ds_uuid.clone()),
            id: 123123123 + GROUP_CHAT_ID_SHIFT,
            name_option: Some("My Group".to_owned()),
            source_type: SourceType::Telegram as i32,
            tpe: ChatType::PrivateGroup as i32,
            img_path_option: None,
            member_ids: vec![myself.id, member.id, channel_user.id],
            msg_count: 6,
            main_chat_id: None,
        });

        let msgs = dao.first_messages(&chat, 99999)?;
        assert_eq!(msgs.len() as i32, chat.msg_count);

        // Order of these two is swapped by Telegram
        assert_eq!(msgs[0], Message {
            internal_id: 0,
            source_id_option: Some(1),
            timestamp: dt("2016-02-10 21:55:02", Some(&offset)).timestamp(),
            from_id: channel_user.id,
            text: vec![],
            searchable_string: "My Group".to_owned(),
            typed: Some(message_service!(GroupMigrateFrom(MessageServiceGroupMigrateFrom {
                title: "My Group".to_owned()
            }))),
        });
        assert_eq!(msgs[1], Message {
            internal_id: 1,
            source_id_option: Some(-999999999),
            timestamp: dt("2016-02-10 21:55:03", Some(&offset)).timestamp(),
            from_id: member.id,
            text: vec![],
            searchable_string: "".to_owned(),
            typed: Some(message_service!(GroupMigrateTo(MessageServiceGroupMigrateTo {}))),
        });
        assert_eq!(msgs[2], Message {
            internal_id: 2,
            source_id_option: Some(111111),
            timestamp: dt("2016-11-17 17:57:40", Some(&offset)).timestamp(),
            from_id: member.id,
            text: vec![
                // Two plaintext elements are concatenated
                RichTextElement {
                    searchable_string: "this contains a lot of stuff: 😁".to_owned(),
                    val: Some(rich_text_element::Val::Plain(RtePlain {
                        text: "this contains a lot of stuff: 😁".to_owned(),
                    })),
                },
                RichTextElement {
                    searchable_string: "http://mylink.org/".to_owned(),
                    val: Some(rich_text_element::Val::Link(RteLink {
                        text_option: Some("http://mylink.org/".to_owned()),
                        href: "http://mylink.org/".to_owned(),
                        hidden: false,
                    })),
                },
                RichTextElement {
                    searchable_string: "HIDE ME".to_owned(),
                    val: Some(rich_text_element::Val::Spoiler(RteSpoiler {
                        text: "HIDE ME".to_owned(),
                    })),
                },
            ],
            searchable_string: "this contains a lot of stuff: 😁 http://mylink.org/ HIDE ME".to_owned(),
            typed: Some(message_regular! {
                edit_timestamp_option: None,
                is_deleted: false,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: None,
            }),
        });
        assert_eq!(msgs[3], Message {
            internal_id: 3,
            source_id_option: Some(111112),
            timestamp: dt("2022-10-17 16:40:09", Some(&offset)).timestamp(),
            from_id: myself.id,
            text: vec![],
            searchable_string: UNKNOWN.to_owned(),
            typed: Some(message_service!(GroupInviteMembers(MessageServiceGroupInviteMembers {
                members: vec![UNKNOWN.to_owned()]
            }))),
        });
        assert_eq!(msgs[4], Message {
            internal_id: 4,
            source_id_option: Some(111113),
            timestamp: 1666993143, // Here we put an explicit timestamp, just for fun
            from_id: myself.id,
            text: vec![],
            searchable_string: "".to_owned(),
            typed: Some(message_service!(GroupDeletePhoto(MessageServiceGroupDeletePhoto {}))),
        });
        assert_eq!(msgs[5], Message {
            internal_id: 5,
            source_id_option: Some(111114),
            timestamp: 1676732102, // Here we put an explicit timestamp, just for fun
            from_id: myself.id,
            text: vec![],
            searchable_string: "".to_owned(),
            typed: Some(message_service!(SuggestProfilePhoto(MessageServiceSuggestProfilePhoto {
                photo: Some(ContentPhoto {
                    path_option: None,
                    width: 640,
                    height: 640,
                    is_one_time: false,
                })
            }))),
        });
    };
    Ok(())
}

#[test]
fn loading_2023_08() -> EmptyRes {
    let res = resource("telegram_2023-08");
    LOADER.looks_about_right(&res)?;

    let dao =
        LOADER.load(&res, &NoChooser)?;

    let ds_uuid = &dao.ds_uuid();
    let myself = dao.myself_single_ds();
    assert_eq!(myself, expected_myself(ds_uuid));

    let unnamed_user = User {
        ds_uuid: Some(ds_uuid.clone()),
        id: 5555555555 - USER_ID_SHIFT,
        first_name_option: None,
        last_name_option: None,
        username_option: None,
        phone_number_option: None,
    };
    assert_eq!(dao.users_single_ds().len(), 2);
    assert_eq!(dao.users_single_ds().iter().collect_vec(), vec![&myself, &unnamed_user]);

    assert_eq!(dao.cwms_single_ds().len(), 1);

    // Group chat
    {
        // Chat ID is shifted by 2^33
        let cwm = dao.cwms_single_ds().into_iter()
            .find(|c| c.chat.unwrap_ref().id == 123123123 + GROUP_CHAT_ID_SHIFT)
            .unwrap();
        let chat = cwm.chat.unwrap_ref();
        assert_eq!(*chat, Chat {
            ds_uuid: Some(ds_uuid.clone()),
            id: 123123123 + GROUP_CHAT_ID_SHIFT,
            name_option: Some("My Group".to_owned()),
            source_type: SourceType::Telegram as i32,
            tpe: ChatType::PrivateGroup as i32,
            img_path_option: None,
            member_ids: vec![myself.id, unnamed_user.id],
            msg_count: 2,
            main_chat_id: None,
        });

        let msgs: &Vec<Message> = &cwm.messages;
        assert_eq!(msgs.len() as i32, chat.msg_count);

        // Order of these two is swapped by Telegram
        assert_eq!(msgs[0], Message {
            internal_id: 0,
            source_id_option: Some(11111),
            timestamp: 1664352868,
            from_id: unnamed_user.id,
            text: vec![],
            searchable_string: UNNAMED.to_owned(),
            typed: Some(message_service!(GroupInviteMembers(MessageServiceGroupInviteMembers {
                members: vec![UNNAMED.to_owned()]
            }))),
        });
        assert_eq!(msgs[1], Message {
            internal_id: 1,
            source_id_option: Some(11112),
            timestamp: 1665499755,
            from_id: unnamed_user.id,
            text: vec![
                RichTextElement {
                    searchable_string: "My message!".to_owned(),
                    val: Some(rich_text_element::Val::Plain(RtePlain {
                        text: "My message!".to_owned(),
                    })),
                },
            ],
            searchable_string: "My message!".to_owned(),
            typed: Some(message_regular! {
                edit_timestamp_option: None,
                is_deleted: false,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: None,
            }),
        });
    };
    Ok(())
}

#[test]
fn loading_2023_10_audio_video() -> EmptyRes {
    let res = resource("telegram_2023-10_audio-video");
    LOADER.looks_about_right(&res)?;

    let dao =
        LOADER.load(&res, &NoChooser)?;

    let ds_uuid = &dao.ds_uuid();
    let myself = dao.myself_single_ds();
    assert_eq!(myself, expected_myself(ds_uuid));

    let unnamed_user = User {
        ds_uuid: Some(ds_uuid.clone()),
        id: 5555555555 - USER_ID_SHIFT,
        first_name_option: None,
        last_name_option: None,
        username_option: None,
        phone_number_option: None,
    };
    assert_eq!(dao.users_single_ds().len(), 2);
    assert_eq!(dao.users_single_ds().iter().collect_vec(), vec![&myself, &unnamed_user]);

    assert_eq!(dao.cwms_single_ds().len(), 1);

    // Group chat
    {
        // Chat ID is shifted by 2^33
        let cwm = dao.cwms_single_ds().into_iter()
            .find(|c| c.chat.unwrap_ref().id == 123123123 + GROUP_CHAT_ID_SHIFT)
            .unwrap();
        let chat = cwm.chat.unwrap_ref();
        assert_eq!(*chat, Chat {
            ds_uuid: Some(ds_uuid.clone()),
            id: 123123123 + GROUP_CHAT_ID_SHIFT,
            name_option: Some("My Group".to_owned()),
            source_type: SourceType::Telegram as i32,
            tpe: ChatType::PrivateGroup as i32,
            img_path_option: None,
            member_ids: vec![myself.id, unnamed_user.id],
            msg_count: 4,
            main_chat_id: None,
        });

        let msgs: &Vec<Message> = &cwm.messages;
        assert_eq!(msgs.len() as i32, chat.msg_count);

        assert_eq!(msgs[0], Message {
            internal_id: 0,
            source_id_option: Some(11111),
            timestamp: 1532249471,
            from_id: unnamed_user.id,
            text: vec![RichText::make_plain("Audio file (incomplete) message".to_owned())],
            searchable_string: "Audio file (incomplete) message".to_owned(),
            typed: Some(message_regular! {
                edit_timestamp_option: None,
                is_deleted: false,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: Some(Content {
                    sealed_value_optional: Some(Audio(ContentAudio {
                        path_option: Some("audio_file.mp3".to_owned()),
                        title_option: None,
                        performer_option: None,
                        mime_type: "audio/mpeg".to_owned(),
                        duration_sec_option: None,
                        thumbnail_path_option: None,
                    }))
                }),
            }),
        });
        assert_eq!(msgs[1], Message {
            internal_id: 1,
            source_id_option: Some(11112),
            timestamp: 1532249472,
            from_id: unnamed_user.id,
            text: vec![RichText::make_plain("Audio file (full) message".to_owned())],
            searchable_string: "Audio file (full) message Song Name Audio Performer".to_owned(),
            typed: Some(message_regular! {
                edit_timestamp_option: None,
                is_deleted: false,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: Some(Content {
                    sealed_value_optional: Some(Audio(ContentAudio {
                        path_option: Some("audio_file.mp3".to_owned()),
                        title_option: Some("Song Name".to_string()),
                        performer_option: Some("Audio Performer".to_owned()),
                        mime_type: "audio/mpeg".to_owned(),
                        duration_sec_option: Some(123),
                        thumbnail_path_option: Some("audio_file.mp3_thumb.jpg".to_owned()),
                    }))
                }),
            }),
        });
        assert_eq!(msgs[2], Message {
            internal_id: 2,
            source_id_option: Some(21111),
            timestamp: 1665499755,
            from_id: unnamed_user.id,
            text: vec![RichText::make_plain("Video file (incomplete) message".to_owned())],
            searchable_string: "Video file (incomplete) message".to_owned(),
            typed: Some(message_regular! {
                edit_timestamp_option: None,
                is_deleted: false,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: Some(Content {
                    sealed_value_optional: Some(Video(ContentVideo {
                        path_option: Some("video_file.mp4".to_owned()),
                        title_option: None,
                        performer_option: None,
                        width: 222,
                        height: 333,
                        mime_type: "video/mp4".to_owned(),
                        duration_sec_option: Some(111),
                        thumbnail_path_option: Some("video_file.mp4_thumb.jpg".to_owned()),
                        is_one_time: false,
                    }))
                }),
            }),
        });
        assert_eq!(msgs[3], Message {
            internal_id: 3,
            source_id_option: Some(21112),
            timestamp: 1665499756,
            from_id: unnamed_user.id,
            text: vec![RichText::make_plain("Video file (full) message".to_owned())],
            searchable_string: "Video file (full) message Clip Name Video Performer".to_owned(),
            typed: Some(message_regular! {
                edit_timestamp_option: None,
                is_deleted: false,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: Some(Content {
                    sealed_value_optional: Some(Video(ContentVideo {
                        path_option: Some("video_file.mp4".to_owned()),
                        title_option: Some("Clip Name".to_string()),
                        performer_option: Some("Video Performer".to_owned()),
                        width: 222,
                        height: 333,
                        mime_type: "video/mp4".to_owned(),
                        duration_sec_option: Some(111),
                        thumbnail_path_option: Some("video_file.mp4_thumb.jpg".to_owned()),
                        is_one_time: false,
                    }))
                }),
            }),
        });
    };
    Ok(())
}

#[test]
fn loading_2023_11_diff() -> EmptyRes {
    let res = resource("telegram_2023-11");
    LOADER.looks_about_right(&res)?;

    let dao =
        LOADER.load(&res, &NoChooser)?;

    let cwm = &dao.cwms_single_ds()[0];
    let msgs = &cwm.messages;
    assert_eq!(msgs.len() as i32, 1);

    assert_eq!(msgs[0].text, vec![
        RichText::make_plain("This has blockquote!".to_owned()),
        RichText::make_blockquote("My blockquote text".to_owned()),
    ]);

    Ok(())
}

#[test]
fn loading_2024_01() -> EmptyRes {
    let res = resource("telegram_2024-01");
    LOADER.looks_about_right(&res)?;

    let dao =
        LOADER.load(&res, &NoChooser)?;

    let cwm = &dao.cwms_single_ds()[0];
    let msgs = &cwm.messages;
    assert_eq!(msgs.len() as i32, 2);

    assert_eq!(msgs[0].text, vec![
        RichText::make_italic("(Replying to a channel post)\n".to_owned()),
        RichText::make_plain("My reply to a channel post!".to_owned()),
    ]);

    assert_eq!(msgs[1].text, vec![
        RichText::make_plain("Messages will be auto-deleted in 31 day(s)".to_owned()),
    ]);
    assert_matches!(&msgs[1].typed, Some(message_service_pat!(Notice(_))));

    Ok(())
}

#[test]
fn loading_2024_02() -> EmptyRes {
    // Verify a parsed message with create_channel and saved_from to be treated correctly
    let res = resource("telegram_2024-02_create-channel_saved-from");
    LOADER.looks_about_right(&res)?;

    let dao =
        LOADER.load(&res, &NoChooser)?;

    let cwm = &dao.cwms_single_ds()[0];
    let msgs = &cwm.messages;
    assert_eq!(msgs.len() as i32, 2);

    if let Some(message_service_pat!(GroupCreate(MessageServiceGroupCreate { title, members }))) = &msgs[0].typed {
        assert_eq!(title.as_str(), "Dummy Supergroup");
        assert_eq!(members, &Vec::<String>::new());
    } else { bail!("Message is not GroupCreate: {:?}", msgs[0].typed) }

    assert_eq!(msgs[1], Message {
        internal_id: 1,
        source_id_option: Some(11112),
        timestamp: 1665499756,
        from_id: 11111111,
        text: vec![RichText::make_plain("Forward of a forward of a message".to_owned())],
        searchable_string: "Forward of a forward of a message".to_owned(),
        typed: Some(message_regular! {
            edit_timestamp_option: None,
            is_deleted: false,
            forward_from_name_option: Some("Forwarded From Name".to_owned()),
            reply_to_message_id_option: None,
            content_option: None,
        }),
    });

    Ok(())
}

#[test]
fn inline_bot_buttons() -> EmptyRes {
    let res = resource("telegram_2024-01_inline-bot-buttons");
    LOADER.looks_about_right(&res)?;

    let dao =
        LOADER.load(&res, &NoChooser)?;

    let cwm = &dao.cwms_single_ds()[0];
    let msgs = &cwm.messages;
    assert_eq!(msgs.len() as i32, 5);

    let button_link_1 =
        RichText::make_link(Some("Button link 1".to_owned()), "https://www.example.com/".to_owned(), false);

    assert_eq!(msgs[0].text, vec![
        RichText::make_bold("Some initial bold text".to_owned()),
        RichText::make_plain("\n".to_owned()),
        button_link_1.clone(),
        RichText::make_plain("\n".to_owned()),
        RichText::make_link(Some("Button link 2".to_owned()), "https://t.me/".to_owned(), false),
        RichText::make_plain("\n".to_owned()),
        RichText::make_link(Some("Button Link 3 at second row".to_owned()), "http://localhost:80/".to_owned(), false),
    ]);

    assert_eq!(msgs[1].text, vec![
        RichText::make_plain("Initial plain text should have a line break added to it\n".to_owned()),
        button_link_1.clone(),
    ]);

    assert_eq!(msgs[2].text, vec![
        button_link_1.clone(),
    ]);

    assert_eq!(msgs[3].text, vec![
        RichText::make_plain("Only auth link".to_owned()),
    ]);

    assert_eq!(msgs[4].text, vec![]);

    Ok(())
}

//
// Helpers
//

fn expected_myself(ds_uuid: &PbUuid) -> User {
    User {
        ds_uuid: Some(ds_uuid.clone()),
        id: 11111111,
        first_name_option: Some("Aaaaa".to_owned()),
        last_name_option: Some("Aaaaaaaaaaa".to_owned()),
        username_option: Some("@frozenspider".to_owned()),
        phone_number_option: Some("+998 91 1234567".to_owned()),
    }
}
