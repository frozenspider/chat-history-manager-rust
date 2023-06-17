use chrono::prelude::*;
use itertools::Itertools;
use lazy_static::lazy_static;

use crate::json::parse_file;
use crate::json::telegram::*;
use crate::protobuf::history::*;
use crate::protobuf::history::message::*;
use crate::{NO_CHOOSER, User};

lazy_static! {
    static ref RESOURCES_DIR: String =
        concat!(env!("CARGO_MANIFEST_DIR"), "/resources/test").replace("//", "/");
}

fn resource(relative_path: &str) -> String {
    [RESOURCES_DIR.as_str(), relative_path].join("/")
}

fn verify_result<T, E: std::fmt::Display>(r: Result<T, E>) -> T {
    match r {
        Ok(res) => res,
        Err(e) => {
            panic!(r#"Result has an error:
{}"#, e)
        }
    }
}

fn dt(s: &str) -> DateTime<Local> {
    let local_tz = Local::now().timezone();
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap().and_local_timezone(local_tz).unwrap()
}

trait ExtOption<T> {
    fn unwrap_ref(&self) -> &T;
}

impl<T> ExtOption<T> for Option<T> {
    fn unwrap_ref(&self) -> &T { self.as_ref().unwrap() }
}

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

//
// Tests
//

#[test]
fn loading_2020_01() {
    let dao = verify_result(parse_file(resource("telegram_2020-01").as_str(), NO_CHOOSER));

    let ds_uuid = dao.dataset.uuid.unwrap_ref();
    let myself = &dao.myself;
    assert_eq!(myself, &expected_myself(ds_uuid));

    let member = ShortUser::new(32507588, None);
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
            phone_number_option: Some("+7 916 337 53 10".to_owned()),
        },
        ShortUser::new_name_str(310242343, "Vlllllll").to_user(ds_uuid),
        ShortUser::new_name_str(333333333, "Ddddddd Uuuuuuuu").to_user(ds_uuid),
        User {
            ds_uuid: Some(ds_uuid.clone()),
            id: 555555555,
            first_name_option: Some("Nnnnnnn".to_owned()),
            last_name_option: None,
            username_option: None,
            phone_number_option: Some("+998 90 1112233".to_owned()),
        },
        ShortUser::new_name_str(666666666, "Iiiii Kkkkkkkkkk").to_user(ds_uuid),
        User {
            ds_uuid: Some(ds_uuid.clone()),
            id: 777777777,
            first_name_option: Some("Vvvvv".to_owned()),
            last_name_option: Some("Vvvvvvvvv".to_owned()),
            username_option: None,
            phone_number_option: Some("+7 951 123 45 67".to_owned()),
        },
    ];

    assert_eq!(dao.users.len(), 9);
    assert_eq!(dao.users, expected_users);

    assert_eq!(dao.cwm.len(), 4);

    // "Ordered" chat
    {
        let cwm = dao.cwm.iter()
            .find(|&c| c.chat.unwrap_ref().id == 4321012345)
            .unwrap();
        let chat = cwm.chat.unwrap_ref();
        assert_eq!(chat.tpe, ChatType::Personal as i32);

        assert_eq!(chat.member_ids.len(), 2);
        assert!(chat.member_ids.contains(&myself.id));
        assert!(chat.member_ids.contains(&member.id));

        let msgs: &Vec<Message> = &cwm.messages; // TODO: Ask DAO instead?
        assert_eq!(msgs.len(), 5);
        assert_eq!(chat.msg_count, 5);
        msgs.iter().for_each(|m| {
            assert!(matches!(m.typed.unwrap_ref(), Typed::Regular(_)));
            assert_eq!(m.from_id, member.id);
        });
        assert_eq!(
            msgs.iter().map(|m| unwrap_rich_text_copy(&m.text).clone()).collect_vec(),
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
        )
    }
}

#[test]
fn loading_2021_05() {
    let dao = verify_result(parse_file(resource("telegram_2021-05").as_str(), NO_CHOOSER));

    let ds_uuid = dao.dataset.uuid.unwrap_ref();
    let myself = &dao.myself;
    assert_eq!(myself, &expected_myself(ds_uuid));

    // We only know of myself + two users (other's IDs aren't known), as well as service "member".
    let service_member =
        ShortUser::new_name_str(8112233, "My Old Group").to_user(ds_uuid);
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
        phone_number_option: Some("+7 916 337 53 10".to_owned()), // Taken from contacts list
    };
    assert_eq!(dao.users.len(), 4);
    assert_eq!(dao.users.iter().collect_vec(), vec![myself, &service_member, &member1, &member2]);

    assert_eq!(dao.cwm.len(), 1);

    // Group chat
    {
        // Chat ID is shifted by 2^33
        let cwm = dao.cwm.iter()
            .find(|&c| c.chat.unwrap_ref().id == 123123123 + GROUP_CHAT_ID_SHIFT)
            .unwrap();
        let chat = cwm.chat.unwrap_ref();
        assert_eq!(chat.name_option, Some("My Group".to_owned()));
        assert_eq!(chat.tpe, ChatType::PrivateGroup as i32);

        assert_eq!(chat.member_ids.len(), 4);
        assert_eq!(chat.member_ids[0], myself.id);
        assert_eq!(chat.member_ids[1], service_member.id);
        assert_eq!(chat.member_ids[2], member1.id);
        assert_eq!(chat.member_ids[3], member2.id);

        let msgs: &Vec<Message> = &cwm.messages; // TODO: Ask DAO instead?
        assert_eq!(msgs.len(), 3);
        assert_eq!(chat.msg_count, 3);
        let typed = msgs.iter().map(|m| m.typed.unwrap_ref()).collect_vec();

        use crate::protobuf::history::message_service::Val;
        // I wish we could use assert_matches!() already...
        assert!(matches!(typed[0], Typed::Service(MessageService { val: Some(Val::GroupCreate(_)) })));
        assert!(matches!(typed[1], Typed::Service(MessageService { val: Some(Val::GroupMigrateFrom(_)) })));
        assert!(matches!(typed[2], Typed::Regular(_)));
    }
}


#[test]
fn loading_2021_06_supergroup() {
    let dao = verify_result(parse_file(resource("telegram_2021-06_supergroup").as_str(), NO_CHOOSER));

    let ds_uuid = dao.dataset.uuid.unwrap_ref();
    let myself = &dao.myself;
    assert_eq!(myself, &expected_myself(ds_uuid));

    // We only know of myself + two users (other's IDs aren't known), as well as service "member".
    let u222222222 =
        ShortUser::new_name_str(222222222, "Sssss Pppppp").to_user(ds_uuid);
    let u333333333 =
        ShortUser::new_name_str(333333333, "Tttttt Yyyyyyy").to_user(ds_uuid);
    let u444444444 =
        ShortUser::new_name_str(444444444, "Vvvvvvvv Bbbbbbb").to_user(ds_uuid);

    {
        let mut sorted_users = dao.users.iter().collect_vec();
        sorted_users.sort_by_key(|&u| u.id);
        assert_eq!(sorted_users.len(), 4);
        assert_eq!(sorted_users, vec![myself, &u222222222, &u333333333, &u444444444]);
    }

    assert_eq!(dao.cwm.len(), 1);

    // Group chat
    {
        // Chat ID is shifted by 2^33
        let cwm = dao.cwm.iter()
            .find(|&c| c.chat.unwrap_ref().id == 1234567890 + GROUP_CHAT_ID_SHIFT)
            .unwrap();
        let chat = cwm.chat.unwrap_ref();
        assert_eq!(chat.name_option, Some("My Supergroup".to_owned()));
        assert_eq!(chat.tpe, ChatType::PrivateGroup as i32);

        // All users are taken from chat itself
        assert_eq!(chat.member_ids.len(), 4);
        assert_eq!(chat.member_ids[0], myself.id);
        assert_eq!(chat.member_ids[1], u222222222.id);
        assert_eq!(chat.member_ids[2], u333333333.id);
        assert_eq!(chat.member_ids[3], u444444444.id);


        let msgs: &Vec<Message> = &cwm.messages; // TODO: Ask DAO instead?
        assert_eq!(msgs.len(), 4);
        assert_eq!(chat.msg_count, 4);

        assert_eq!(msgs[0], Message {
            internal_id: -1,
            source_id_option: Some(-999681092),
            timestamp: dt("2020-12-22 23:11:21").timestamp(),
            from_id: u222222222.id,
            text: vec![],
            searchable_string: None,
            typed: Some(Typed::Service(MessageService {
                val: Some(message_service::Val::GroupInviteMembers(MessageServiceGroupInviteMembers {
                    members: vec![u444444444.first_name_option.unwrap()]
                }))
            })),
        });

        assert_eq!(msgs[1], Message {
            internal_id: -1,
            source_id_option: Some(-999681090),
            timestamp: dt("2020-12-22 23:12:09").timestamp(),
            from_id: u333333333.id,
            text: vec![RichTextElement {
                searchable_string: None,
                val: Some(rich_text_element::Val::Plain(RtePlain {
                    text: "Message text with emoji 🙂".to_owned(),
                })),
            }],
            searchable_string: None,
            typed: Some(Typed::Regular(MessageRegular {
                edit_timestamp_option: None,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: None,
            })),
        });

        assert_eq!(msgs[2], Message {
            internal_id: -1,
            source_id_option: Some(-999681087),
            timestamp: dt("2020-12-22 23:12:51").timestamp(),
            from_id: u444444444.id,
            text: vec![RichTextElement {
                searchable_string: None,
                val: Some(rich_text_element::Val::Plain(RtePlain {
                    text: "Message from an added user".to_owned(),
                })),
            }],
            searchable_string: None,
            typed: Some(Typed::Regular(MessageRegular {
                edit_timestamp_option: None,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: None,
            })),
        });

        assert_eq!(msgs[3], Message {
            internal_id: -1,
            source_id_option: Some(358000),
            timestamp: dt("2021-03-18 17:50:23").timestamp(),
            from_id: myself.id,
            text: vec![],
            searchable_string: None,
            typed: Some(Typed::Regular(MessageRegular {
                edit_timestamp_option: None,
                forward_from_name_option: None,
                reply_to_message_id_option: None,
                content_option: Some(Content {
                    val: Some(content::Val::SharedContact(ContentSharedContact {
                        first_name: myself.first_name_option.to_owned().unwrap(),
                        last_name_option: None,
                        phone_number_option: Some(myself.phone_number_option.to_owned().unwrap()),
                        vcard_path_option: None,
                    }))
                }),
            })),
        });
    }
}

#[test]
fn loading_2021_07() {
    let dao = verify_result(parse_file(resource("telegram_2021-07").as_str(), NO_CHOOSER));

    let ds_uuid = dao.dataset.uuid.unwrap_ref();
    let myself = &dao.myself;
    assert_eq!(myself, &expected_myself(ds_uuid));

    let member = User {
        ds_uuid: Some(ds_uuid.clone()),
        id: 44444444,
        first_name_option: Some("Eeeee".to_owned()),
        last_name_option: Some("Eeeeeeeeee".to_owned()),
        username_option: None,
        phone_number_option: Some("+7 916 337 53 10".to_owned()), // Taken from contacts list
    };
    assert_eq!(dao.users.len(), 2);
    assert_eq!(dao.users.iter().collect_vec(), vec![myself, &member]);

    assert_eq!(dao.cwm.len(), 1);

    // Group chat
    {
        // Chat ID is shifted by 2^33
        let cwm = dao.cwm.iter()
            .find(|&c| c.chat.unwrap_ref().id == 123123123 + GROUP_CHAT_ID_SHIFT)
            .unwrap();
        let chat = cwm.chat.unwrap_ref();
        assert_eq!(chat.name_option, Some("My Group".to_owned()));
        assert_eq!(chat.tpe, ChatType::PrivateGroup as i32);

        assert_eq!(chat.member_ids.len(), 2);
        assert_eq!(chat.member_ids[0], myself.id);
        assert_eq!(chat.member_ids[1], member.id);

        let msgs: &Vec<Message> = &cwm.messages; // TODO: Ask DAO instead?
        assert_eq!(msgs.len(), 2);
        assert_eq!(chat.msg_count, 2);
        // let typed = msgs.iter().map(|m| m.typed.unwrap_ref()).collect_vec();

        use crate::protobuf::history::message_service::Val;
        assert_eq!(msgs[0], Message {
            internal_id: -1,
            source_id_option: Some(111111),
            timestamp: dt("2021-07-03 22:38:58").timestamp(),
            from_id: member.id,
            text: vec![],
            searchable_string: None,
            typed: Some(Typed::Service(MessageService {
                val: Some(Val::GroupCall(MessageServiceGroupCall {
                    members: vec!["Www Wwwwww".to_owned()]
                }))
            })),
        });
        assert_eq!(msgs[1], Message {
            internal_id: -1,
            source_id_option: Some(111112),
            timestamp: dt("2021-07-03 22:39:01").timestamp(),
            from_id: member.id,
            text: vec![],
            searchable_string: None,
            typed: Some(Typed::Service(MessageService {
                val: Some(Val::GroupCall(MessageServiceGroupCall {
                    members: vec!["Myself".to_owned()]
                }))
            })),
        });
    }
}
